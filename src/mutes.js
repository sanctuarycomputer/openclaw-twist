// Mute list: Twist conversations and threads the bot must stop answering, shared with
// whatever else can see a runaway. Two writers today: Stacksbot's Spend Guard (a daily cost
// check, stacksbot scripts/lib/twist-mute.mjs, which owns the contract) and this plugin's
// own loop breaker (loop-breaker.js). The bot-sender guard stops flagged bots; this is how
// everything else gets stopped — a counterpart posting as a normal user, or a conversation
// that is simply costing too much.
//
// CONTRACT (version 1). Path: $OPENCLAW_STATE_DIR/twist-muted.json; no env var = no file =
// nothing muted.
//   { "version": 1,
//     "conversations": { "<convId>":   { "kind", "reason", "mutedAt": ISO, "until": ISO|null, "by" } },
//     "threads":       { "<threadId>": { …same… } } }
// An entry mutes while `until` is null or later than now. Unknown fields are ignored.
// Writers read, merge, write a temp file and rename it over; they never delete an entry and
// never rewrite one another writer (`by`) made. An unmute sets `until` to that moment.
import { closeSync, fsyncSync, openSync, readFileSync, renameSync, statSync, writeSync } from "node:fs";
import { join } from "node:path";

export const MUTE_FILE_NAME = "twist-muted.json";
export const RECHECK_MS = 10_000; // how often the file's identity is re-stat'd; a changed file is re-read
export const DROP_LOG_MS = 60_000; // one drop line per peer per minute; the rest are counted
// Hugh's own DM with the bot and the ops thread. A mute on either would cut Hugh off from
// the bot, so it is ignored with a warning; a runaway there is alerted, never silenced.
export const DEFAULT_NEVER_MUTE = Object.freeze(["conv:1367817", "thread:7882650"]);

/** `$OPENCLAW_STATE_DIR/twist-muted.json`, or null when the env var is unset (nothing is muted). */
export function muteFilePath(env = process.env) {
  return env.OPENCLAW_STATE_DIR ? join(env.OPENCLAW_STATE_DIR, MUTE_FILE_NAME) : null;
}

const isMap = (v) => !!v && typeof v === "object" && !Array.isArray(v);

/** Whether an entry mutes at `nowMs`. An unparseable `until` mutes nothing (fail open). */
export function isLive(entry, nowMs) {
  if (!isMap(entry)) return false;
  if (entry.until === null || entry.until === undefined) return true;
  return Date.parse(entry.until) > nowMs;
}

/** Queue peer id (`conv:<id>` / `thread:<id>`) → the file's bucket and key. null for anything else. */
export function peerTarget(peerId) {
  const m = /^(conv|thread):(\d+)$/.exec(String(peerId ?? ""));
  if (!m) return null;
  return { bucket: m[1] === "conv" ? "conversations" : "threads", id: m[2] };
}

/** Parse the file's text. Throws on anything that is not the contract's top-level shape. */
function parseMuteFile(text) {
  const raw = JSON.parse(text);
  if (!isMap(raw)) throw new Error("not a JSON object");
  return { ...raw, version: 1, conversations: isMap(raw.conversations) ? raw.conversations : {}, threads: isMap(raw.threads) ? raw.threads : {} };
}

/** tmp-file + fsync + rename, the same discipline as the queue and cursor files. */
function writeAtomic(path, data) {
  const tmp = `${path}.${process.pid}.${Date.now()}.tmp`;
  const fd = openSync(tmp, "w");
  try {
    writeSync(fd, `${JSON.stringify(data, null, 2)}\n`);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(tmp, path);
}

/**
 * @param {object} deps
 * @param {string|null} deps.path  the mute file, or null (nothing is ever muted from a file)
 * @param {string[]} [deps.neverMute] peer ids (`conv:<id>` / `thread:<id>`) a mute is never honoured for
 *
 * Reads are cheap by construction: at most one stat per RECHECK_MS, and a re-read only when
 * the file's identity (inode, size, mtime) changed. A missing or corrupt file mutes nothing —
 * failing closed would let one bad hand-edit silence the whole workspace — and is logged once
 * per distinct problem, not once per poll.
 *
 * A mute this process adds (`mute()`) but cannot write — no path, a write failure, an unreadable
 * file, an expired entry another writer owns — is kept in memory until it expires, so the peer
 * still stops here (until restart). A mute that WAS written lives only in the file, so a person
 * unmuting it there is obeyed at the next re-read.
 */
export function createMuteList({ path, neverMute = DEFAULT_NEVER_MUTE, log, now = Date.now }) {
  const protectedPeers = new Set((neverMute ?? []).map(String));
  let file = { version: 1, conversations: {}, threads: {} };
  let fileKey = null; // ino:size:mtime of what `file` was read from
  let checkedAt = -Infinity;
  let lastProblem = null;
  const local = new Map(); // peerId -> entry this process muted (in-memory twin of its file entry)
  const drops = new Map(); // peerId -> {loggedAt, suppressed}
  const protectWarned = new Set(); // `${peerId}|${mutedAt}`: one warning per ignored entry

  function problem(text) {
    if (text === lastProblem) return;
    lastProblem = text;
    log(text);
  }

  function reload(force = false) {
    if (!path) return;
    if (!force && now() - checkedAt < RECHECK_MS) return;
    checkedAt = now();
    let key;
    try {
      const st = statSync(path);
      key = `${st.ino}:${st.size}:${st.mtimeMs}`;
    } catch (err) {
      file = { version: 1, conversations: {}, threads: {} };
      fileKey = null;
      if (err?.code === "ENOENT") problem(`mute list ${path} not present: nothing muted`);
      else problem(`mute list ${path} unreadable (nothing muted): ${String(err)}`);
      return;
    }
    if (key === fileKey) return;
    fileKey = key;
    try {
      file = parseMuteFile(readFileSync(path, "utf8"));
      lastProblem = null;
    } catch (err) {
      file = { version: 1, conversations: {}, threads: {} };
      problem(`mute list ${path} is corrupt (nothing muted until it is fixed): ${String(err)}`);
    }
  }

  /** The file's entry for a peer (after a cheap freshness check), or undefined. */
  function fileEntry(peerId) {
    const t = peerTarget(peerId);
    if (!t) return undefined;
    reload();
    return file[t.bucket][t.id];
  }

  return {
    isProtected: (peerId) => protectedPeers.has(String(peerId)),

    /** The live entry muting this peer, or null. Protected peers are never muted (warned once per entry). */
    entryFor(peerId) {
      const key = String(peerId);
      const fromFile = fileEntry(key);
      const mine = local.get(key);
      const entry = isLive(fromFile, now()) ? fromFile : isLive(mine, now()) ? mine : null;
      if (!entry) return null;
      if (protectedPeers.has(key)) {
        const warnKey = `${key}|${entry.mutedAt}`;
        if (!protectWarned.has(warnKey)) {
          protectWarned.add(warnKey);
          log(`WARNING: ignoring mute of ${key} (by ${entry.by ?? "someone"}: ${entry.reason ?? "no reason given"}): it is in channels.twist.neverMute and is always answered`);
        }
        return null;
      }
      return entry;
    },

    isMuted(peerId) {
      return this.entryFor(peerId) !== null;
    },

    /** The file entry for a peer whether or not it is live (the loop breaker reads unmutes off it). */
    rawEntry: (peerId) => fileEntry(String(peerId)),

    /**
     * Mute a peer: in memory at once, and merged into the file when this process may write
     * it. Returns `{written, why?}`; `written:false` still mutes the peer in this process.
     *
     * The merge re-reads the file right before writing (not the cached copy) so a concurrent
     * writer's fresh entry is kept. A file that exists but does not parse is left alone: it
     * may be a person's half-finished edit, and overwriting it would drop every other entry.
     */
    mute(peerId, entry) {
      const key = String(peerId);
      if (protectedPeers.has(key)) return { written: false, why: "protected (channels.twist.neverMute)" };
      const memoryOnly = (why) => { local.set(key, entry); return { written: false, why }; };
      const t = peerTarget(key);
      if (!t) return memoryOnly(`not a mutable peer id: ${key}`);
      if (!path) return memoryOnly("no mute file (OPENCLAW_STATE_DIR is unset)");
      let current;
      try {
        current = parseMuteFile(readFileSync(path, "utf8"));
      } catch (err) {
        if (err?.code !== "ENOENT") return memoryOnly(`mute file unreadable, not overwriting it: ${String(err)}`);
        current = { version: 1, conversations: {}, threads: {} };
      }
      const existing = current[t.bucket][t.id];
      // Already muted in the file: that entry does the job, and its owner's unmute must win.
      if (isLive(existing, now())) { reload(true); return { written: false, why: `already muted by ${existing.by ?? "someone"}` }; }
      if (existing && existing.by !== entry.by) return memoryOnly(`${existing.by ?? "another writer"} owns that entry`);
      current[t.bucket][t.id] = entry;
      try {
        writeAtomic(path, current);
      } catch (err) {
        return memoryOnly(`write failed: ${String(err)}`);
      }
      local.delete(key);
      reload(true);
      return { written: true };
    },

    /** One log line per peer per DROP_LOG_MS, carrying how many drops it stood for. */
    noteDropped(item) {
      const key = String(item.peerId);
      const d = drops.get(key) ?? { loggedAt: -Infinity, suppressed: 0 };
      drops.set(key, d);
      if (now() - d.loggedAt < DROP_LOG_MS) { d.suppressed++; return; }
      const e = this.entryFor(key);
      const more = d.suppressed ? ` (+${d.suppressed} more since the last line)` : "";
      log(`dropped ${item.id} in muted ${key} (by ${e?.by ?? "?"} until ${e?.until ?? "unmuted"}): ${e?.reason ?? ""}${more}`);
      d.loggedAt = now();
      d.suppressed = 0;
    },
  };
}
