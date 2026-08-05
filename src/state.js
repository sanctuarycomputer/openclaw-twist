// Cursor persistence for the Twist channel. The cursor (highest obj_index swept per
// thread / conversation) is a REFETCH BOUND only — dedup and the delivery guarantee
// live in the queue store (queue.js), which records every item id ever enqueued. The
// cursor advances only after a sweep's items are durably enqueued (see producer.js).
// For threads the producer ALSO advances the bot's own Twist read marker after
// sweeping (a scale bound so the unread-threads list can't grow unbounded).
import { readFile, rename, mkdir, open } from "node:fs/promises";
import { dirname } from "node:path";

export function createCursorStore(filePath, { log } = {}) {
  /** @type {{threads:Record<string,number>, conversations:Record<string,number>}} */
  let data = { threads: {}, conversations: {} };
  let loaded = false;
  let writeChain = Promise.resolve();

  /**
   * @param {number} [now] epoch ms, used only to name a quarantined corrupt file.
   *   Passed in (never read from the clock here) so this module stays deterministic.
   */
  async function load(now = 0) {
    let raw;
    try {
      raw = await readFile(filePath, "utf8");
    } catch (err) {
      if (err.code !== "ENOENT") throw err; // real IO fault: fail loudly, don't invent empty cursors
    }
    if (raw !== undefined) {
      try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("cursor file is not an object");
        data = {
          threads: parsed.threads ?? {},
          conversations: parsed.conversations ?? {},
        };
      } catch (err) {
        // Same treatment as the queue store: an unreadable file (half-written before a hard
        // crash, hand-edited) must not boot-loop the account. Quarantine it and start empty
        // — every thread/conv then reads as first sight, so the grace window baselines the
        // backlog away instead of replying to history.
        const quarantine = `${filePath}.corrupt-${now}`;
        try {
          await rename(filePath, quarantine);
          log?.(`cursor store at ${filePath} is corrupt (${String(err)}) — quarantined to ${quarantine}, starting empty`);
        } catch (renameErr) {
          log?.(`cursor store at ${filePath} is corrupt (${String(err)}) and could not be quarantined (${String(renameErr)}) — starting empty; it will be overwritten on the next write`);
        }
        data = { threads: {}, conversations: {} };
      }
    }
    loaded = true;
  }

  function persist() {
    const snapshot = JSON.stringify(data, null, 2);
    // tmp + fsync + atomic rename: a crash mid-write must never leave a half-written cursor
    // file behind. A rejected write is also swallowed for CHAINING purposes only (this
    // call's own promise still rejects), so one transient ENOSPC can't poison every later
    // persist by leaving writeChain permanently rejected.
    const p = writeChain.catch(() => {}).then(async () => {
      await mkdir(dirname(filePath), { recursive: true });
      const tmp = `${filePath}.tmp`;
      const fh = await open(tmp, "w");
      try {
        await fh.writeFile(snapshot);
        await fh.sync();
      } finally {
        await fh.close();
      }
      await rename(tmp, filePath);
    });
    writeChain = p.catch(() => {});
    return p;
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
