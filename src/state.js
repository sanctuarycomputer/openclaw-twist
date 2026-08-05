// Cursor persistence for the Twist channel. The cursor (highest obj_index swept per
// thread / conversation) is a REFETCH BOUND only — dedup and the delivery guarantee
// live in the queue store (queue.js), which records every item id ever enqueued. The
// cursor advances only after a sweep's items are durably enqueued (see producer.js).
// For threads the producer ALSO advances the bot's own Twist read marker after
// sweeping (a scale bound so the unread-threads list can't grow unbounded).
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

export function createCursorStore(filePath) {
  /** @type {{threads:Record<string,number>, conversations:Record<string,number>}} */
  let data = { threads: {}, conversations: {} };
  let loaded = false;
  let writeChain = Promise.resolve();

  async function load() {
    try {
      const raw = await readFile(filePath, "utf8");
      const parsed = JSON.parse(raw);
      data = {
        threads: parsed.threads ?? {},
        conversations: parsed.conversations ?? {},
      };
    } catch (err) {
      if (err.code !== "ENOENT") throw err;
    }
    loaded = true;
  }

  function persist() {
    const snapshot = JSON.stringify(data, null, 2);
    writeChain = writeChain.then(async () => {
      await mkdir(dirname(filePath), { recursive: true });
      await writeFile(filePath, snapshot);
    });
    return writeChain;
  }

  function ensureLoaded() {
    if (!loaded) throw new Error("twist cursor store used before load()");
  }

  return {
    load,
    /** @returns {boolean} true if this thread/conv has never been seen. */
    isFirstSight(kind, id) {
      ensureLoaded();
      return data[kind][String(id)] === undefined;
    },
    getCursor(kind, id) {
      ensureLoaded();
      const v = data[kind][String(id)];
      return v === undefined ? -Infinity : v;
    },
    async setCursor(kind, id, objIndex) {
      ensureLoaded();
      if (!Number.isFinite(objIndex)) return;
      const cur = data[kind][String(id)];
      if (cur === undefined || objIndex > cur) {
        data[kind][String(id)] = objIndex;
        await persist();
      }
    },
  };
}
