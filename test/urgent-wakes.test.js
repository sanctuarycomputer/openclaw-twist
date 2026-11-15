import test from "node:test";
import assert from "node:assert/strict";
import * as nodeFs from "node:fs";
import { existsSync, mkdtempSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendUrgentWake, MAX_LINE_BYTES, subjectOfPeer, urgentDir, writeBots } from "../src/urgent-wakes.js";

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
