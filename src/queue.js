// Persistent job-queue store for Twist ingestion. Single-process, single JSON
// file. Correctness contract: an item id, once seen (live or tombstoned), is
// never re-enqueued; every transition persists before callers proceed.
import { readFile, writeFile, rename, mkdir, open } from "node:fs/promises";
import { dirname } from "node:path";

const TERMINAL = new Set(["done", "skipped", "failed"]);
const PRUNE_AFTER_MS = 30 * 24 * 3600 * 1000;

export function createQueueStore(filePath) {
  let data = { items: {}, tombstones: [] };
  let tombstoneSet = new Set();
  let loaded = false;
  let writeChain = Promise.resolve();

  async function load() {
    try {
      const parsed = JSON.parse(await readFile(filePath, "utf8"));
      data = { items: parsed.items ?? {}, tombstones: parsed.tombstones ?? [] };
    } catch (err) {
      if (err.code !== "ENOENT") throw err;
    }
    tombstoneSet = new Set(data.tombstones);
    loaded = true;
  }
  function ensure() {
    if (!loaded) throw new Error("twist queue store used before load()");
  }
  function persist() {
    const snapshot = JSON.stringify(data, null, 1);
    writeChain = writeChain.then(async () => {
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
    return writeChain;
  }

  return {
    load,
    has: (id) => (ensure(), id in data.items || tombstoneSet.has(id)),
    get: (id) => (ensure(), data.items[id]),
    async enqueueAll(items, now) {
      ensure();
      let added = 0;
      for (const it of items) {
        if (it.id in data.items || tombstoneSet.has(it.id)) continue;
        data.items[it.id] = { ...it, state: "queued", attempts: it.attempts ?? 0, nextAttemptAt: it.nextAttemptAt ?? 0, enqueuedAt: it.enqueuedAt ?? now };
        added++;
      }
      if (added) await persist();
      return added;
    },
    async transition(id, patch, now) {
      ensure();
      const cur = data.items[id];
      if (!cur) throw new Error(`transition on unknown item ${id}`);
      const next = { ...cur, ...patch };
      if (TERMINAL.has(next.state) && next.finishedAt === undefined) next.finishedAt = now ?? patch.claimedAt ?? cur.claimedAt ?? cur.enqueuedAt;
      data.items[id] = next;
      await persist();
      return next;
    },
    itemsInState: (state) => (ensure(), Object.values(data.items).filter((i) => i.state === state)),
    nonTerminalCount: () => (ensure(), Object.values(data.items).filter((i) => !TERMINAL.has(i.state)).length),
    async prune(now) {
      ensure();
      let changed = false;
      for (const [id, it] of Object.entries(data.items)) {
        if (TERMINAL.has(it.state) && now - (it.finishedAt ?? it.enqueuedAt) > PRUNE_AFTER_MS) {
          delete data.items[id];
          tombstoneSet.add(id);
          changed = true;
        }
      }
      if (changed) {
        data.tombstones = [...tombstoneSet];
        await persist();
      }
    },
  };
}
