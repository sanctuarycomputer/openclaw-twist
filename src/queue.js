// Persistent job-queue store for Twist ingestion. Single-process, single JSON
// file. Correctness contract: an item id, once seen (live or tombstoned), is
// never re-enqueued; every transition persists before callers proceed.
import { readFile, rename, mkdir, open } from "node:fs/promises";
import { dirname } from "node:path";

const TERMINAL = new Set(["done", "skipped", "failed"]);
const PRUNE_AFTER_MS = 30 * 24 * 3600 * 1000;
// Every write gets its OWN tmp file. A shared `${filePath}.tmp` is a tearing hazard the
// moment two stores write the same path (a second process, or two live stores during an
// in-process restart): writer B truncates the tmp file that writer A is about to rename,
// and the rename publishes a partial snapshot. Unique names make the rename the only
// interleaving point, and rename is atomic.
let tmpCounter = 0;
const tmpName = (filePath) => `${filePath}.${process.pid}.${tmpCounter++}.tmp`;

export function createQueueStore(filePath, { log } = {}) {
  let data = { items: {}, tombstones: [] };
  let tombstoneSet = new Set();
  let loaded = false;
  let writeChain = Promise.resolve();
  // The most recent persist, WITH its rejection intact. `writeChain` deliberately swallows
  // failures so one bad write can't poison later chaining, which makes it useless as a
  // durability signal — it resolves whether or not the bytes landed. Callers asking "is
  // everything I mutated on disk?" need the answer to be no when it isn't.
  let lastPersist = Promise.resolve();

  /**
   * @param {number} [now] epoch ms, used only to name a quarantined corrupt file.
   *   Passed in (never read from the clock here) so this module stays deterministic.
   */
  async function load(now = 0) {
    let raw;
    try {
      raw = await readFile(filePath, "utf8");
    } catch (err) {
      if (err.code !== "ENOENT") throw err; // real IO fault: fail loudly, don't invent an empty queue
    }
    if (raw !== undefined) {
      try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("queue file is not an object");
        data = { items: parsed.items ?? {}, tombstones: parsed.tombstones ?? [] };
      } catch (err) {
        // The file exists but is unreadable — e.g. half-written before a hard crash, or
        // hand-edited. Throwing here would boot-loop the account forever, so quarantine it
        // and start empty. This cannot re-answer old messages: cursors still bound the
        // refetch, so only genuinely-unread items get re-enqueued.
        const quarantine = `${filePath}.corrupt-${now}`;
        try {
          await rename(filePath, quarantine);
          log?.(`queue store at ${filePath} is corrupt (${String(err)}) — quarantined to ${quarantine}, starting empty`);
        } catch (renameErr) {
          log?.(`queue store at ${filePath} is corrupt (${String(err)}) and could not be quarantined (${String(renameErr)}) — starting empty; it will be overwritten on the next write`);
        }
        data = { items: {}, tombstones: [] };
      }
    }
    tombstoneSet = new Set(data.tombstones);
    loaded = true;
  }
  function ensure() {
    if (!loaded) throw new Error("twist queue store used before load()");
  }
  function persist() {
    const snapshot = JSON.stringify(data, null, 1);
    // A rejected write must not permanently poison writeChain — otherwise every later
    // persist() chains off the same rejected promise and returns it rejected forever, even
    // though the underlying condition (e.g. a transient ENOSPC) may have cleared. Swallow
    // the previous failure for chaining purposes only; this call's own promise (`p`) still
    // rejects to its caller.
    const p = writeChain.catch(() => {}).then(async () => {
      await mkdir(dirname(filePath), { recursive: true });
      const tmp = tmpName(filePath);
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
    lastPersist = p;
    return p;
  }

  return {
    load,
    /**
     * Resolves once everything mutated so far is durably on disk; REJECTS if the most
     * recent write failed.
     *
     * This exists for the cursor invariant: "the cursor advances only after the enqueue
     * batch is durably persisted." `enqueueAll` only awaits a persist when it actually
     * added something, which was sufficient while sweeps were serialized — a sweep that
     * added nothing had nothing of its own to flush. Under concurrent sweeps that stopped
     * being true: sweep B can dedup to `added = 0` against rows sweep A has inserted in
     * memory but not yet fsynced, and B would then advance the cursor (a separate, much
     * smaller, much faster file) past items that are not on disk. A crash in that window
     * loses them silently — they sit below the cursor, so nothing ever refetches them.
     *
     * Awaiting this before any `setCursor` restores the happens-before edge for every
     * interleaving, without forcing pointless empty writes.
     *
     * Checking only the LAST persist is sufficient, not a gap: every persist snapshots the
     * whole `data` object, so a later successful write subsumes all earlier ones.
     */
    whenPersisted: () => lastPersist,
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
    selectClaimable(now, busyPeerIds, slotsFree) {
      ensure();
      if (slotsFree <= 0) return null;
      let best = null;
      for (const it of Object.values(data.items)) {
        if (it.state !== "queued" || it.nextAttemptAt > now || busyPeerIds.has(it.peerId)) continue;
        if (!best || it.postedTs < best.postedTs || (it.postedTs === best.postedTs && it.objIndex < best.objIndex)) best = it;
      }
      return best;
    },
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
