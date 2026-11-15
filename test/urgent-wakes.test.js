import test from "node:test";
import assert from "node:assert/strict";
import * as nodeFs from "node:fs";
import { existsSync, mkdtempSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendUrgentWake, MAX_LINE_BYTES, subjectOfPeer, urgentDir, writeBots } from "../src/urgent-wakes.js";
import { createMuteList } from "../src/mutes.js";
import { createCriticalNotifier, createLoopBreaker, loopAlertText } from "../src/loop-breaker.js";

// Stacksbot's docs/contracts/urgent.md 1.0 fake, vendored byte for byte from
// scripts/stacksbot-sync/test/fixtures/urgent/wakes-sample.jsonl.
const FIXTURE = new URL("./fixtures/urgent/wakes-sample.jsonl", import.meta.url);
const NOW = Date.parse("2026-11-13T12:00:00Z");

const tmp = () => mkdtempSync(join(tmpdir(), "tw-urgent-"));
const wakesOf = (dir, day = "2026-11-13") => {
  const p = join(dir, "urgent", `wakes-${day}.jsonl`);
  return existsSync(p) ? readFileSync(p, "utf8").trim().split("\n").filter(Boolean).map((l) => JSON.parse(l)) : [];
};
const fixtureRows = () => readFileSync(FIXTURE, "utf8").trim().split("\n").map((l) => JSON.parse(l));

// A test-only port of contract §4's row rules (stacksbot scripts/lib/urgent-state.mjs wakeProblems),
// so every row this plugin writes is held to the same rules as the fixture's.
const SOURCE_SIGNALS = { hook: [1, 2, 3, 4, 5], heartbeat: [6, "heartbeat"], floor: [7, 8], cli: [null] };
const ISO = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})$/;
const isPlainObject = (v) => v !== null && typeof v === "object" && !Array.isArray(v);
const nonEmpty = (v) => typeof v === "string" && v.length > 0;
function isIsoTime(v) {
  const m = typeof v === "string" ? ISO.exec(v) : null;
  if (!m) return false;
  const [y, mo, d, h, mi, s = "0"] = m.slice(1).map((x) => (x === undefined ? undefined : Number(x)));
  const day = new Date(Date.UTC(y, mo - 1, d));
  return day.getUTCMonth() === mo - 1 && day.getUTCDate() === d && h <= 23 && mi <= 59 && Number(s) <= 59;
}
function wakeProblems(row) {
  if (!isPlainObject(row)) return ["row: not an object"];
  const out = [];
  if (!nonEmpty(row.id)) out.push("id");
  if (!isIsoTime(row.at)) out.push("at");
  const allowed = Object.hasOwn(SOURCE_SIGNALS, row.source) ? SOURCE_SIGNALS[row.source] : null;
  if (!allowed) out.push("source");
  else if (!Object.hasOwn(row, "signal") || !allowed.includes(row.signal)) out.push("signal");
  if (!nonEmpty(row.subject)) out.push("subject");
  if (!isPlainObject(row.facts)) out.push("facts");
  if (Object.hasOwn(row, "dm_posted") && typeof row.dm_posted !== "boolean") out.push("dm_posted");
  if (row.dm_posted === true) {
    const ok = nonEmpty(row.message_id) || (Number.isInteger(row.message_id) && row.message_id > 0);
    if (!ok) out.push("message_id: required with dm_posted");
  } else if (row.message_id != null) out.push("message_id: only with dm_posted true");
  return out;
}
const requiredFields = () => {
  const rows = fixtureRows();
  return Object.keys(rows[0]).filter((k) => rows.every((r) => Object.hasOwn(r, k))).sort();
};
/** Every row the plugin wrote has the fixture's required fields and passes the contract rules. */
function assertContractRow(row) {
  for (const k of requiredFields()) assert.ok(Object.hasOwn(row, k), `missing required field ${k}`);
  assert.deepEqual(wakeProblems(row), []);
}

function withTZ(tz, fn) {
  const before = process.env.TZ;
  process.env.TZ = tz;
  try {
    return fn();
  } finally {
    if (before === undefined) delete process.env.TZ;
    else process.env.TZ = before;
  }
}

/** A row whose serialised line (with its newline) is exactly `bytes` long. */
function rowOfLineBytes(bytes) {
  const base = { id: "pad", at: "2026-11-13T12:00:00.000Z", source: "floor", signal: 8, subject: "twist:conv:1", facts: { pad: "" } };
  const len = Buffer.byteLength(`${JSON.stringify(base)}\n`);
  return { ...base, facts: { pad: "x".repeat(bytes - len) } };
}

test("fixture: the contract's required fields are in every row, and the port accepts every row", () => {
  assert.deepEqual(requiredFields(), ["at", "facts", "id", "signal", "source", "subject"]);
  for (const row of fixtureRows()) assert.deepEqual(wakeProblems(row), [], row.id);
  const [anchor] = fixtureRows().filter((r) => r.dm_posted === true);
  assert.notDeepEqual(wakeProblems({ ...anchor, message_id: null }), []);
  assert.notDeepEqual(wakeProblems({ ...anchor, dm_posted: "yes" }), []);
  assert.notDeepEqual(wakeProblems({ ...anchor, signal: 3 }), []);
});

test("urgentDir: $OPENCLAW_STATE_DIR/urgent, or null off the box", () => {
  assert.equal(urgentDir({ OPENCLAW_STATE_DIR: "/state" }), join("/state", "urgent"));
  assert.equal(urgentDir({}), null);
  assert.equal(urgentDir({ OPENCLAW_STATE_DIR: "" }), null);
});

test("appendUrgentWake: one line per call, appended, in the UTC day's file", () => {
  const dir = tmp();
  const env = { OPENCLAW_STATE_DIR: dir };
  assert.equal(appendUrgentWake({ id: "a", facts: {} }, { env, nowMs: NOW }), true);
  assert.equal(appendUrgentWake({ id: "b", facts: { n: 1 } }, { env, nowMs: NOW }), true);
  assert.equal(appendUrgentWake({ id: "c", facts: {} }, { env, nowMs: Date.parse("2026-11-13T23:59:59.999Z") }), true);
  assert.equal(appendUrgentWake({ id: "d", facts: {} }, { env, nowMs: Date.parse("2026-11-14T00:00:00.000Z") }), true);
  assert.deepEqual(wakesOf(dir).map((w) => w.id), ["a", "b", "c"]);
  assert.deepEqual(wakesOf(dir, "2026-11-14").map((w) => w.id), ["d"]);
  assert.ok(readFileSync(join(dir, "urgent", "wakes-2026-11-13.jsonl"), "utf8").endsWith('{"id":"c","facts":{}}\n'));
  // The day is the UTC day, whatever the process's time zone.
  withTZ("America/New_York", () => assert.equal(appendUrgentWake({ id: "ny", facts: {} }, { env, nowMs: Date.parse("2026-11-14T03:00:00.000Z") }), true));
  withTZ("Asia/Tokyo", () => assert.equal(appendUrgentWake({ id: "tokyo", facts: {} }, { env, nowMs: Date.parse("2026-11-13T23:30:00.000Z") }), true));
  assert.deepEqual(wakesOf(dir, "2026-11-14").map((w) => w.id), ["d", "ny"]);
  assert.equal(wakesOf(dir).at(-1).id, "tokyo");
});

test("appendUrgentWake: 4096 bytes including the newline is the limit", () => {
  assert.equal(MAX_LINE_BYTES, 4096);
  const dir = tmp();
  const env = { OPENCLAW_STATE_DIR: dir };
  const exact = rowOfLineBytes(4096);
  assert.equal(appendUrgentWake(exact, { env, nowMs: NOW }), true);
  assert.equal(appendUrgentWake({ ...rowOfLineBytes(4097), id: "over" }, { env, nowMs: NOW }), true);
  const [a, b] = wakesOf(dir);
  assert.deepEqual(a.facts, exact.facts);
  assert.equal(Buffer.byteLength(`${JSON.stringify(a)}\n`), 4096);
  assert.deepEqual([b.id, b.facts], ["over", { truncated: true }]);
  // Multi-byte text counts in bytes, not characters.
  assert.equal(appendUrgentWake({ id: "é", facts: { s: "é".repeat(2100) } }, { env, nowMs: NOW }), true);
  assert.deepEqual(wakesOf(dir).at(-1).facts, { truncated: true });
  // Too long even without its facts: refused, nothing written.
  const fresh = tmp();
  assert.equal(appendUrgentWake({ id: "x".repeat(5000), facts: {} }, { env: { OPENCLAW_STATE_DIR: fresh }, nowMs: NOW }), false);
  assert.equal(existsSync(join(fresh, "urgent", "wakes-2026-11-13.jsonl")), false);
});

test("appendUrgentWake: opens for append, one write per row; a short write ends the fragment and fails", () => {
  const dir = tmp();
  const env = { OPENCLAW_STATE_DIR: dir };
  const calls = [];
  const spy = { ...nodeFs, openSync: (p, flag) => { calls.push(["open", flag]); return nodeFs.openSync(p, flag); }, writeSync: (fd, data) => { calls.push(["write", String(data).length]); return nodeFs.writeSync(fd, data); } };
  assert.equal(appendUrgentWake({ id: "a", facts: {} }, { env, nowMs: NOW, fs: spy }), true);
  assert.deepEqual(calls.map((c) => c[0]), ["open", "write"]);
  assert.equal(calls[0][1], "a");

  const writes = [];
  const short = {
    ...nodeFs,
    writeSync: (fd, data) => {
      writes.push(Buffer.isBuffer(data) ? data.toString() : data);
      if (writes.length === 1) return nodeFs.writeSync(fd, data.subarray(0, 5));
      return nodeFs.writeSync(fd, data);
    },
  };
  assert.equal(appendUrgentWake({ id: "torn", facts: {} }, { env, nowMs: NOW, fs: short }), false);
  assert.equal(writes.length, 2);
  assert.equal(writes[1], "\n");
  // The fragment is ended, so the next row starts on its own line.
  assert.equal(appendUrgentWake({ id: "next", facts: {} }, { env, nowMs: NOW }), true);
  const lines = readFileSync(join(dir, "urgent", "wakes-2026-11-13.jsonl"), "utf8").split("\n");
  assert.equal(lines.at(-2), '{"id":"next","facts":{}}');
});

test("appendUrgentWake: nothing off the box, and never throws", () => {
  assert.equal(appendUrgentWake({ id: "a", facts: {} }, { env: {}, nowMs: NOW }), false);
  assert.equal(appendUrgentWake({ id: "a", facts: {} }, { env: { OPENCLAW_STATE_DIR: "" }, nowMs: NOW }), false);
  const file = join(tmp(), "not-a-dir");
  writeFileSync(file, "x");
  assert.equal(appendUrgentWake({ id: "a", facts: {} }, { env: { OPENCLAW_STATE_DIR: file }, nowMs: NOW }), false);
  const throwingEnv = { get OPENCLAW_STATE_DIR() { throw new Error("boom"); } };
  assert.equal(appendUrgentWake({ id: "a", facts: {} }, { env: throwingEnv, nowMs: NOW }), false);
  const cyclic = { id: "c", facts: {} };
  cyclic.facts.self = cyclic;
  assert.equal(appendUrgentWake(cyclic, { env: { OPENCLAW_STATE_DIR: tmp() }, nowMs: NOW }), false);
});

test("writeBots: an atomic { at, ids } snapshot", () => {
  const dir = tmp();
  const env = { OPENCLAW_STATE_DIR: dir };
  const path = join(dir, "urgent", "bots.json");
  assert.equal(writeBots(new Set(["2601", 77]), { env, nowMs: NOW }), true);
  assert.deepEqual(JSON.parse(readFileSync(path, "utf8")), { at: "2026-11-13T12:00:00.000Z", ids: ["2601", "77"] });
  const ino = statSync(path).ino;
  assert.equal(writeBots(["5"], { env, nowMs: NOW + 1000 }), true);
  assert.deepEqual(JSON.parse(readFileSync(path, "utf8")), { at: "2026-11-13T12:00:01.000Z", ids: ["5"] });
  assert.notEqual(statSync(path).ino, ino, "replaced by rename, not rewritten in place");
  assert.deepEqual(readdirSync(join(dir, "urgent")), ["bots.json"]);

  const calls = [];
  const spy = {
    ...nodeFs,
    writeFileSync: (p, ...rest) => { calls.push(["write", p]); return nodeFs.writeFileSync(p, ...rest); },
    renameSync: (a, b) => { calls.push(["rename", a, b]); return nodeFs.renameSync(a, b); },
  };
  assert.equal(writeBots(["9"], { env, nowMs: NOW, fs: spy }), true);
  assert.equal(calls.length, 2);
  const [[w, tmpPath], [r, from, to]] = calls;
  assert.deepEqual([w, r, from, to], ["write", "rename", tmpPath, path]);
  assert.notEqual(tmpPath, path);
  assert.equal(join(tmpPath, ".."), join(path, ".."), "the temp file is in the same directory");

  const failing = { ...nodeFs, renameSync: () => { throw new Error("EXDEV"); } };
  assert.equal(writeBots(["1"], { env, nowMs: NOW, fs: failing }), false);
  assert.deepEqual(readdirSync(join(dir, "urgent")), ["bots.json"], "no temp file left behind");
  assert.deepEqual(JSON.parse(readFileSync(path, "utf8")).ids, ["9"]);

  assert.equal(writeBots(["1"], { env: {}, nowMs: NOW }), false);
});

test("subjectOfPeer: the queue's peer ids become the contract's Twist subjects", () => {
  assert.equal(subjectOfPeer("conv:1349360"), "twist:conv:1349360");
  assert.equal(subjectOfPeer("thread:9"), "twist:thread:9");
});

// ---- The loop breaker's anchor wake (Task 2) ----

const LB_CONFIG = { enabled: true, maxTurns: 2, windowMinutes: 30, muteHours: 6 };
const HOUR = 3600_000;

function breakerSetup({ notify = async () => ({ messageId: "4242", dm: true }), neverMute, file, mutePath, appendWake, env } = {}) {
  const dir = tmp();
  const path = mutePath === undefined ? join(dir, "twist-muted.json") : mutePath;
  if (file && path) writeFileSync(path, JSON.stringify(file));
  const clock = { t: NOW };
  const logs = [];
  const alerts = [];
  const mutes = createMuteList({ path, neverMute, log: () => {}, now: () => clock.t });
  const breaker = createLoopBreaker({
    mutes,
    notify: async (text) => {
      alerts.push({ text, wakesBefore: wakesOf(dir).length });
      return notify(text);
    },
    config: LB_CONFIG,
    log: (m) => logs.push(m),
    now: () => clock.t,
    env: env ?? { OPENCLAW_STATE_DIR: dir },
    ...(appendWake ? { appendWake } : {}),
  });
  /** Three turns 1s apart: the third trips (maxTurns 2). → the third admit's verdict. */
  const trip = async (peerId = "conv:1349360", kind = "dm", senderName = "Ada Bot") => {
    let last;
    for (let i = 0; i < 3; i++) {
      last = await breaker.admit({ id: `x${i}:${clock.t}`, kind: peerId.startsWith("conv:") ? "conv" : "thread", peerId, senderId: 2601, senderName }, { kind });
      clock.t += 1000;
    }
    return last;
  };
  return { dir, path, clock, logs, alerts, breaker, trip };
}

test("loop breaker: a muting trip appends the anchor wake after notify", async () => {
  const s = breakerSetup();
  assert.equal(await s.trip(), false);
  assert.equal(s.alerts.length, 1);
  assert.equal(s.alerts[0].wakesBefore, 0, "the wake is appended after notify resolved");
  const [w, ...rest] = wakesOf(s.dir);
  assert.equal(rest.length, 0);
  assertContractRow(w);
  const tripAt = NOW + 2000;
  assert.deepEqual(w, {
    id: `8:twist:conv:1349360:${tripAt}`,
    at: new Date(tripAt).toISOString(),
    source: "floor",
    signal: 8,
    subject: "twist:conv:1349360",
    dm_posted: true,
    message_id: "4242",
    facts: {
      by: "twist-loop-breaker",
      title: "Loop in my DM with Ada Bot (conversation 1349360)",
      what: "I answered 3 messages there in 1 minutes.",
      lever: "mute-conversation",
      lever_args: { conversation: "1349360" },
      turns: 3,
      minutes: 1,
      outcome: "muted",
    },
  });
  // The alert itself is exactly today's text.
  assert.equal(s.alerts[0].text, loopAlertText({ peerId: "conv:1349360", kind: "dm", senderName: "Ada Bot", turns: 3, minutes: 1, outcome: "muted", untilMs: tripAt + 6 * HOUR }));
});

test("loop breaker: a thread trip names the thread", async () => {
  const s = breakerSetup();
  await s.trip("thread:9", "thread", "Zapier");
  const [w] = wakesOf(s.dir);
  assertContractRow(w);
  assert.equal(w.subject, "twist:thread:9");
  assert.deepEqual([w.facts.title, w.facts.lever, w.facts.lever_args], ["Loop in thread 9", "mute-conversation", { thread: "9" }]);
});

test("loop breaker: no message id, or not a DM, means no DM was posted", async () => {
  for (const result of [null, undefined, { messageId: undefined, dm: true }, { messageId: "", dm: true }, { messageId: "5", dm: false }, { messageId: "5" }]) {
    const s = breakerSetup({ notify: async () => result });
    await s.trip();
    const [w] = wakesOf(s.dir);
    assertContractRow(w);
    assert.deepEqual([w.dm_posted, w.message_id], [false, null], JSON.stringify(result));
  }
});

test("loop breaker: protected and held trips wake once per alert, with nothing to undo", async () => {
  const p = breakerSetup({ neverMute: ["conv:1349360"] });
  assert.equal(await p.trip(), true);
  assert.equal(await p.trip(), true); // re-trips inside muteHours: no DM, so no wake
  assert.equal(p.alerts.length, 1);
  const pw = wakesOf(p.dir);
  assert.equal(pw.length, 1);
  assertContractRow(pw[0]);
  assert.deepEqual([pw[0].facts.outcome, pw[0].facts.lever, pw[0].facts.lever_args, pw[0].dm_posted], ["protected", "propose", {}, true]);
  assert.equal(p.alerts[0].text, loopAlertText({ peerId: "conv:1349360", kind: "dm", senderName: "Ada Bot", turns: 3, minutes: 1, outcome: "protected" }));

  // Held: the breaker's own entry, cut short by hand an hour after it was set.
  const mutedAt = new Date(NOW - 2 * HOUR).toISOString();
  const until = new Date(NOW - HOUR).toISOString();
  const h = breakerSetup({ file: { version: 1, conversations: { 1349360: { kind: "dm", mutedAt, until, by: "twist-loop-breaker", reason: "x" } }, threads: {} } });
  assert.equal(await h.trip(), true);
  assert.equal(await h.trip(), true);
  assert.equal(h.alerts.length, 1);
  const hw = wakesOf(h.dir);
  assert.equal(hw.length, 1);
  assertContractRow(hw[0]);
  assert.deepEqual([hw[0].facts.outcome, hw[0].facts.lever, hw[0].facts.lever_args], ["held", "propose", {}]);
  assert.equal(h.alerts[0].text, loopAlertText({ peerId: "conv:1349360", kind: "dm", senderName: "Ada Bot", turns: 3, minutes: 1, outcome: "held", heldSince: until }));
});

test("loop breaker: a peer already muted by someone else gets no undoable lever", async () => {
  const entry = { kind: "dm", mutedAt: new Date(NOW - HOUR).toISOString(), until: null, by: "spend-guard", reason: "spend" };
  const s = breakerSetup({ file: { version: 1, conversations: { 1349360: entry }, threads: {} } });
  assert.equal(await s.trip(), false);
  const [w] = wakesOf(s.dir);
  assertContractRow(w);
  assert.deepEqual([w.facts.outcome, w.facts.lever, w.facts.lever_args], ["muted", "propose", {}]);
  assert.match(s.alerts[0].text, /I've stopped answering it until/);
});

test("loop breaker: a memory-only mute gets no undoable lever", async () => {
  const s = breakerSetup({ mutePath: null });
  assert.equal(await s.trip(), false);
  const [w] = wakesOf(s.dir);
  assertContractRow(w);
  assert.deepEqual([w.facts.outcome, w.facts.lever, w.facts.lever_args], ["memory-only", "propose", {}]);
  assert.match(s.alerts[0].text, /or the next restart/);
});

test("loop breaker: a fault in the wake path changes nothing else", async () => {
  const thrower = breakerSetup({ appendWake: () => { throw new Error("disk on fire"); } });
  assert.equal(await thrower.trip(), false, "the tripping turn is still blocked");
  assert.equal(thrower.alerts.length, 1);
  assert.ok(thrower.logs.some((l) => /urgent wake not written for conv:1349360: Error: disk on fire/.test(l)));
  assert.ok(!thrower.logs.some((l) => /loop breaker failed/.test(l)));

  const badResult = breakerSetup({ notify: async () => ({ messageId: "1", get dm() { throw new Error("getter"); } }) });
  assert.equal(await badResult.trip(), false);
  assert.equal(badResult.alerts.length, 1);
  assert.ok(badResult.logs.some((l) => /urgent wake not written/.test(l)));

  const file = join(tmp(), "not-a-dir");
  writeFileSync(file, "x");
  const unwritable = breakerSetup({ env: { OPENCLAW_STATE_DIR: file } });
  assert.equal(await unwritable.trip(), false);
  assert.equal(unwritable.alerts.length, 1);
  assert.ok(unwritable.logs.some((l) => l === "urgent wake not written for conv:1349360"));
});

test("loop breaker: the wake goes where env says, and nowhere without it", async () => {
  const off = breakerSetup({ env: {} });
  assert.equal(await off.trip(), false);
  assert.equal(off.alerts.length, 1);
  assert.equal(existsSync(join(off.dir, "urgent")), false);
});

test("createCriticalNotifier: the post's id, whether it was a DM, or null after the fallback", async () => {
  const fallbacks = [];
  const logs = [];
  const make = (post) => createCriticalNotifier({ post, fallback: async (t) => { fallbacks.push(t); }, log: (l) => logs.push(l), to: "conv:1367817" });
  assert.deepEqual(await make(async () => ({ messageId: "7", kind: "conv" }))("hi"), { messageId: "7", dm: true });
  assert.deepEqual(await make(async () => ({ messageId: "8", kind: "thread" }))("hi"), { messageId: "8", dm: false });
  assert.equal(fallbacks.length, 0);
  assert.equal(await make(async () => { throw new Error("503"); })("rejected"), null);
  assert.equal(await make(() => { throw new Error("bad target"); })("thrown"), null);
  assert.deepEqual(fallbacks, ["rejected", "thrown"]);
  assert.deepEqual(logs, [
    "critical alert to conv:1367817 failed, sending it to the ops thread instead: Error: 503",
    "critical alert to conv:1367817 failed, sending it to the ops thread instead: Error: bad target",
  ]);
});
