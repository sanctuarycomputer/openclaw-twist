// Wakes for Stacksbot's Urgent runner. The plugin's hard limits (the loop breaker, the bot
// filter) keep acting and alerting on their own; these files only tell the runner what they
// did, so it adopts the loop breaker's Critical DM as the incident's thread instead of sending a
// second one, and its judge hears about bot drops.
//
// CONTRACT: stacksbot docs/contracts/urgent.md, version 1.0. Directory
// $OPENCLAW_STATE_DIR/urgent (no env var: nothing is written).
// - wakes-YYYY-MM-DD.jsonl (UTC day of the write clock), append-only and shared with other
//   writers: each row is one JSON line of at most 4096 bytes including its "\n", written with
//   O_APPEND and ONE write(), so concurrent appends never interleave. Never seek, truncate,
//   rewrite or delete it.
// - bots.json, { at, ids: string[] }: this plugin is its only writer; replaced whole (temp file in
//   the same directory, then rename), so a reader never sees half a file.
// Stacksbot's tests own the shared fake; test/fixtures/urgent/wakes-sample.jsonl is its copy.
import * as nodeFs from "node:fs";
import { join } from "node:path";

export const MAX_LINE_BYTES = 4096;

/** `$OPENCLAW_STATE_DIR/urgent`, or null when the env var is unset or empty. */
export function urgentDir(env = process.env) {
  return env.OPENCLAW_STATE_DIR ? join(env.OPENCLAW_STATE_DIR, "urgent") : null;
}

const lineOf = (row) => Buffer.from(`${JSON.stringify(row)}\n`);

/**
 * Append one wake row to today's (UTC) wakes file. → whether the whole line was written.
 * A line over MAX_LINE_BYTES is retried once with `facts: { truncated: true }`, then refused.
 * Never throws: a wake that can't be written must not stop the guard that raised it.
 */
export function appendUrgentWake(row, { env = process.env, nowMs = Date.now(), fs = nodeFs } = {}) {
  try {
    const dir = urgentDir(env);
    if (!dir) return false;
    let buf = lineOf(row);
    if (buf.length > MAX_LINE_BYTES) buf = lineOf({ ...row, facts: { truncated: true } });
    if (buf.length > MAX_LINE_BYTES) return false;
    fs.mkdirSync(dir, { recursive: true });
    const fd = fs.openSync(join(dir, `wakes-${new Date(nowMs).toISOString().slice(0, 10)}.jsonl`), "a");
    try {
      if (fs.writeSync(fd, buf) === buf.length) return true;
      // A torn line: end the fragment so the next row starts on its own line.
      try {
        fs.writeSync(fd, "\n");
      } catch {
        // best effort
      }
      return false;
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    return false;
  }
}

/** Replace bots.json with these bot user ids. → whether it was written. Never throws. */
export function writeBots(ids, { env = process.env, nowMs = Date.now(), fs = nodeFs } = {}) {
  let tmp = null;
  try {
    const dir = urgentDir(env);
    if (!dir) return false;
    fs.mkdirSync(dir, { recursive: true });
    const path = join(dir, "bots.json");
    tmp = `${path}.${process.pid}.${nowMs}.tmp`;
    fs.writeFileSync(tmp, `${JSON.stringify({ at: new Date(nowMs).toISOString(), ids: [...ids].map(String) })}\n`);
    fs.renameSync(tmp, path);
    return true;
  } catch {
    if (tmp) {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        // best effort
      }
    }
    return false;
  }
}

/** The queue's peer id (`conv:<id>` or `thread:<id>`) → the Urgent subject (`twist:conv:<id>`). */
export const subjectOfPeer = (peerId) => `twist:${String(peerId)}`;
