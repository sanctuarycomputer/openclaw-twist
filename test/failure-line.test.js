import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  PREFIX,
  VERSION,
  SOURCES,
  KINDS,
  INBOX_STEPS,
  CLASSES,
  CAUSES,
  SESSIONS,
  MAX_LINE_BYTES,
  SKEW_MS,
  MAX_DELAY_MS,
  FIELD_ORDER,
  OPTIONAL_DROP_ORDER,
  checkWhat,
  failureLine,
  failureCause,
  createRateLimiter,
  emitFailure,
  stdoutSink,
} from "../src/failure-line.js";
import { ALERT_HEADER, deadLetterAlertText, storageAlertText, backlogAlertText, hungTurnAlertText } from "../src/consumer.js";
import { loopAlertText } from "../src/loop-breaker.js";

const fixture = (name) => new URL(`./fixtures/failure-log/${name}`, import.meta.url);
const ROWS = readFileSync(fixture("lines.jsonl"), "utf8")
  .split("\n")
  .filter((l) => l.trim() !== "")
  .map((l) => JSON.parse(l));
const SELECTORS = JSON.parse(readFileSync(fixture("twist-selectors.json"), "utf8"));

const FID_RE = /^f-[0-9a-z]{8,9}-[0-9a-z]{4}$/;
const FIXED = Date.parse("2026-11-17T12:00:00.000Z");
const parse = (line) => JSON.parse(line.slice(PREFIX.length));
// A lowercase uuid, built at runtime so no hex run is written literally.
const UUID = ["1".repeat(8), "2222", "4333", "8444", "5".repeat(12)].join("-");

test("fixture parity: every contract fixture line is reproduced byte for byte", () => {
  assert.ok(ROWS.length >= 7);
  for (const row of ROWS) {
    const want = PREFIX + JSON.stringify(row) + "\n";
    assert.equal(failureLine(row), want);
    const got = [];
    assert.equal(emitFailure(row, { sink: (l) => got.push(l) }), true);
    assert.deepEqual(got, [want]);
  }
  assert.equal(VERSION, 1);
});

test("FIELD_ORDER is contract §3's key order, and every fixture line follows it", () => {
  // The output object is built in a fixed order, so a reordered FIELD_ORDER is caught only here.
  const order = ["v", "fid", "at", "kind", "source", "what", "class", "cause", "alerted", "streak", "session", "job_id", "run", "attempt", "suppressed"];
  assert.deepEqual(FIELD_ORDER, order);
  for (const row of ROWS) assert.deepEqual(Object.keys(row), order.filter((k) => Object.hasOwn(row, k)));
});

test("sources are home's 1.0 list", () => {
  const want = [
    "deploy-smoke-test",
    "fix-run-flagged",
    "gateway",
    "job-delivery",
    "job-failure-alert",
    "notion-sync-failure",
    "self-model-check",
    "spend-guard-credit",
    "spend-guard-session",
    "system-cron",
    "twist-inbox-backlog",
    "twist-inbox-dead-letter",
    "twist-inbox-hung-turn",
    "twist-inbox-loop-breaker",
    "twist-inbox-retry",
    "twist-inbox-storage",
  ];
  assert.deepEqual([...SOURCES].sort(), want);
  assert.ok(Object.isFrozen(SOURCES));
  for (const s of SELECTORS) assert.ok(SOURCES.includes(s.source), s.source);
});

test("the guard: checkWhat rules", () => {
  assert.equal(checkWhat("inbox", "conv:1"), null);
  assert.equal(checkWhat("inbox", "thread"), null);
  assert.equal(checkWhat("inbox", " Consumer "), "consumer");
  assert.equal(checkWhat("nope", "consumer"), null);

  const run20 = "a".repeat(19) + "1";
  const run19 = "a".repeat(18) + "1";
  assert.equal(run20.length, 20);
  assert.equal(checkWhat("job", run20), null);
  assert.equal(checkWhat("job", run19), run19);

  const prefixes = ["sk-", "ghp_", "gho_", "ghs_", "ghu_", "github_pat_", "xoxa-", "xoxb-", "xoxp-", "xoxr-", "xoxs-", "ntn_", "secret_", "akia", "eyj", "glpat-"];
  for (const p of prefixes) {
    assert.equal(checkWhat("job", `${p}abc`), null, `${p} at the start`);
    for (const sep of [".", "_", "/", ":", "-"]) assert.equal(checkWhat("job", `x${sep}${p}abc`), null, `${p} after ${sep}`);
  }
  assert.equal(checkWhat("job", "task-abc"), "task-abc", "a prefix inside a word is fine");

  assert.equal(checkWhat("job", "agent:x"), null);
  assert.equal(checkWhat("job", "a:b:c"), null);
  assert.equal(checkWhat("job", "a:b"), "a:b");
  assert.equal(checkWhat("job", "a".repeat(81)), null);
  assert.equal(checkWhat("job", "a".repeat(80)), "a".repeat(80));
});

test("the guard: failureLine closes the line", () => {
  assert.equal(failureLine(null), null);
  assert.equal(failureLine([]), null);
  assert.equal(failureLine("x"), null);
  assert.equal(
    failureLine({
      get kind() {
        throw new Error("x");
      },
    }),
    null,
  );

  const rand = () => 0;
  const now = () => FIXED;
  const line = failureLine(
    { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout", alerted: "true", fid: "bad", at: "nope", item: "x", sender: "y" },
    { now, rand },
  );
  const o = parse(line);
  assert.deepEqual(Object.keys(o), ["v", "fid", "at", "kind", "source", "what", "class", "cause", "alerted"]);
  assert.equal(o.alerted, false);
  assert.equal(o.fid, `f-${FIXED.toString(36)}-0000`);
  assert.equal(o.at, new Date(FIXED).toISOString());
});

test("the guard: only own properties are read (inherited fields never reach the line)", () => {
  const ITEM = "conv-msg:" + 1349360 + ":" + 77;
  const SECRET = "sk-" + "a1".repeat(12);
  const proto = {
    kind: "inbox",
    source: "twist-inbox-retry",
    what: "consumer",
    class: "retry",
    cause: "timeout",
    alerted: true,
    streak: 3,
    session: "cron",
    job_id: UUID,
    run: UUID,
    attempt: 2,
    suppressed: 4,
    fid: `f-${(FIXED - 1000).toString(36)}-abcd`,
    at: new Date(FIXED - 1000).toISOString(),
    item: ITEM,
    error: SECRET,
  };
  const line = failureLine(Object.create(proto), { now: () => FIXED, rand: () => 0 });
  const o = parse(line);
  assert.deepEqual(o, { v: 1, fid: `f-${FIXED.toString(36)}-0000`, at: new Date(FIXED).toISOString(), kind: "?", source: "?", what: "?", class: "unknown", cause: "unknown", alerted: false });
  for (const needle of [ITEM, SECRET, "1349360", UUID, "twist-inbox-retry", "consumer"]) assert.ok(!line.includes(needle), needle);
});

test("the guard: object-valued ids are never written, even when their toString is valid", () => {
  const SENDER = "Ada " + "Lovelace";
  const validFid = `f-${(FIXED - 1000).toString(36)}-abcd`;
  const asObject = (s) => ({ toString: () => s, sender: SENDER });
  const f = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout" };
  const line = failureLine({ ...f, run: asObject(UUID), job_id: asObject(UUID), fid: asObject(validFid) }, { now: () => FIXED, rand: () => 0 });
  const o = parse(line);
  assert.equal(Object.hasOwn(o, "run"), false);
  assert.equal(Object.hasOwn(o, "job_id"), false);
  assert.equal(o.fid, `f-${FIXED.toString(36)}-0000`, "fid regenerated");
  for (const needle of [SENDER, UUID, validFid]) assert.ok(!line.includes(needle), needle);
  // The same values as strings are kept, so the drop above is the type check's doing.
  const kept = parse(failureLine({ ...f, run: UUID, job_id: UUID, fid: validFid }));
  assert.deepEqual([kept.run, kept.job_id, kept.fid], [UUID, UUID, validFid]);
});

test("the guard: what is at most 80 characters for a model too", () => {
  const m80 = "a/" + "b".repeat(78);
  const m81 = "a/" + "b".repeat(79);
  assert.equal(m81.length, 81);
  assert.equal(checkWhat("model", m80), m80);
  assert.equal(checkWhat("model", m81), null);
  assert.equal(parse(failureLine({ kind: "model", what: m81 })).what, "?");
  assert.equal(parse(failureLine({ kind: "model", what: m80 })).what, m80);
});

test("the fid's time part is the clock truncated to whole ms", () => {
  const o = parse(failureLine({ kind: "inbox" }, { now: () => FIXED + 0.5, rand: () => 0 }));
  assert.equal(o.fid, `f-${FIXED.toString(36)}-0000`);
});

test("stdoutSink writes the line to process.stdout once", () => {
  const line = failureLine({ kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout" });
  const calls = [];
  const original = process.stdout.write;
  process.stdout.write = (...args) => {
    calls.push(args);
    return true;
  };
  try {
    stdoutSink(line);
  } finally {
    process.stdout.write = original;
  }
  assert.deepEqual(calls, [[line]]);
});

test("rate limiter: the suppressed count is capped at 999999", () => {
  const f = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout", alerted: false };
  const lim = createRateLimiter();
  assert.deepEqual(lim.admit(f, 0), { write: true });
  for (let i = 0; i < 1_000_001; i++) lim.admit(f, 0);
  assert.deepEqual(lim.admit(f, 60_000), { write: true, suppressed: 999999 });
});

test("the guard: an over-long line drops optional fields in OPTIONAL_DROP_ORDER", () => {
  const fields = {
    fid: `f-${FIXED.toString(36)}-0000`,
    at: new Date(FIXED).toISOString(),
    kind: "job",
    source: "job-failure-alert",
    what: "daily-digest",
    class: "exec-failed",
    cause: "timeout",
    alerted: true,
    streak: 9999,
    session: "subagent",
    job_id: UUID,
    run: UUID,
    attempt: 99,
    suppressed: 999999,
  };
  const full = { v: 1, ...fields };
  const expected = [];
  for (let k = 0; k <= OPTIONAL_DROP_ORDER.length; k++) {
    const o = { ...full };
    for (const key of OPTIONAL_DROP_ORDER.slice(0, k)) delete o[key];
    expected.push(PREFIX + JSON.stringify(o) + "\n");
  }
  assert.deepEqual(OPTIONAL_DROP_ORDER, ["suppressed", "attempt", "run", "job_id", "streak", "session"]);
  for (let k = 0; k < expected.length; k++) {
    const bytes = Buffer.byteLength(expected[k]);
    assert.equal(failureLine(fields, { maxBytes: bytes }), expected[k], `fits after dropping ${k}`);
    if (k + 1 < expected.length) assert.equal(failureLine(fields, { maxBytes: bytes - 1 }), expected[k + 1], `one byte short after dropping ${k}`);
  }
  const at300 = failureLine(fields, { maxBytes: 300 });
  assert.equal(at300, expected.find((l) => Buffer.byteLength(l) <= 300) ?? expected.at(-1));
  assert.ok(Object.keys(parse(at300)).length < Object.keys(full).length);
  assert.equal(failureLine(fields, { maxBytes: 10 }), expected.at(-1), "required fields are never dropped");
});

test("failureCause: one sample per contract §3 row, first row wins", () => {
  const cases = [
    ["Payment Required", "billing"],
    ["rate limit hit", "rate-limited"],
    ["got 429", "http-429"],
    ["rate limit 429", "rate-limited"],
    ["timed out after 500 ms", "timeout"],
    ["ECONNRESET", "connection"],
    ["Twist comments/add HTTP 503", "http-5xx"],
    ["status: 404", "http-4xx"],
    ["boom", "unknown"],
    [null, "none"],
    [undefined, "none"],
    ["  ", "none"],
  ];
  for (const [text, cause] of cases) assert.equal(failureCause(text), cause, String(text));
  assert.equal(
    failureCause({
      toString() {
        throw new Error("x");
      },
    }),
    "unknown",
  );
});

test("no text in the line: markers in every field never reach it", () => {
  const ITEM = "conv-msg:" + 1349360 + ":" + 77;
  const COMMENT = String(9_000_000 + 4321);
  const CONV = "conv:" + 1367817;
  const SENDER = "Ada " + "Lovelace";
  const ERR = "Error: ECONNRESET talking to " + "twist.com for Ada";
  const SECRET = "sk-" + "a1".repeat(12);
  const MARKERS = { ITEM, COMMENT, CONV, SENDER, ERR, SECRET };
  const needles = [...Object.values(MARKERS), "1349360", "9004321", "1367817"];
  const lower = (s) => s.toLowerCase();
  const leaks = (text) => needles.filter((n) => text.includes(n) || text.includes(lower(n)));
  const fieldsWith = (kind, v) => ({ kind, what: v, source: v, class: v, cause: v, session: v, run: v, job_id: v, streak: v, attempt: v, item: v, sender: v, error: v });
  const opts = { now: () => FIXED, rand: () => 0 };

  for (const [name, v] of Object.entries(MARKERS)) {
    // inbox: the whole line, `what` included.
    const inbox = failureLine(fieldsWith("inbox", v), opts);
    assert.deepEqual(leaks(inbox), [], `inbox ${name}`);
    assert.equal(parse(inbox).what, "?");
    // A marker as the kind itself.
    assert.deepEqual(leaks(failureLine({ ...fieldsWith(v, v) }, opts)), [], `kind ${name}`);

    // Contract §3: a job or tool name may be digits or `x:<n>`, so `what` keeps COMMENT and CONV there; the plugin only writes inbox.
    for (const kind of ["job", "tool"]) {
      const o = parse(failureLine(fieldsWith(kind, v), opts));
      const what = o.what;
      delete o.what;
      assert.deepEqual(leaks(JSON.stringify(o)), [], `${kind} ${name} outside what`);
      if ([ITEM, SENDER, ERR, SECRET].includes(v)) assert.equal(what, "?", `${kind} ${name} what`);
    }
  }
});

test("rate limiter: one line per key per window", () => {
  const f = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout", alerted: false };
  const lim = createRateLimiter();
  assert.deepEqual(lim.admit(f, 0), { write: true });
  assert.deepEqual(lim.admit(f, 59_999), { write: false });
  assert.deepEqual(lim.admit({ ...f, cause: "connection" }, 1_000), { write: true });
  assert.deepEqual(lim.admit(f, 60_000), { write: true, suppressed: 1 });

  const fresh = createRateLimiter();
  assert.deepEqual(fresh.admit({ ...f, alerted: true }, 0), { write: true });
  assert.deepEqual(fresh.admit({ ...f, alerted: true }, 1), { write: true });
  assert.deepEqual(fresh.admit(f, 2), { write: true }, "alerted lines do not consume the window");

  const got = [];
  const sink = (l) => got.push(l);
  const limiter = createRateLimiter();
  let t = FIXED;
  const now = () => t;
  assert.equal(emitFailure(f, { sink, now, limiter }), true);
  t += 1_000;
  assert.equal(emitFailure(f, { sink, now, limiter }), false);
  t += 60_000;
  assert.equal(emitFailure(f, { sink, now, limiter }), true);
  assert.equal(got.length, 2);
  assert.equal(parse(got[0]).suppressed, undefined);
  assert.equal(parse(got[1]).suppressed, 1);
});

test("emitFailure faults: never throws, reports false, reverts the limiter", async () => {
  const f = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout" };
  const now = () => FIXED;
  const boom = () => {
    throw new Error("x");
  };
  assert.equal(emitFailure(f, {}), false);
  assert.equal(emitFailure(f), false);

  // A throwing sink reverts its admit: the next call writes, without suppressed.
  const limiter = createRateLimiter();
  assert.equal(emitFailure(f, { sink: boom, now, limiter }), false);
  const got = [];
  assert.equal(emitFailure(f, { sink: (l) => got.push(l), now, limiter }), true);
  assert.equal(parse(got[0]).suppressed, undefined);

  // A pending suppressed count survives a failed sink and goes on the next line.
  const lim2 = createRateLimiter();
  let t = FIXED;
  const clock = () => t;
  const got2 = [];
  assert.equal(emitFailure(f, { sink: (l) => got2.push(l), now: clock, limiter: lim2 }), true);
  t += 1_000;
  assert.equal(emitFailure(f, { sink: (l) => got2.push(l), now: clock, limiter: lim2 }), false);
  t += 60_000;
  assert.equal(emitFailure(f, { sink: boom, now: clock, limiter: lim2 }), false);
  t += 1;
  assert.equal(emitFailure(f, { sink: (l) => got2.push(l), now: clock, limiter: lim2 }), true);
  assert.equal(parse(got2.at(-1)).suppressed, 1);

  // A rejected promise: false, and no unhandled rejection.
  const unhandled = [];
  const onUnhandled = (e) => unhandled.push(e);
  process.on("unhandledRejection", onUnhandled);
  try {
    assert.equal(emitFailure(f, { sink: () => Promise.reject(new Error("x")) }), false);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 5));
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
  assert.deepEqual(unhandled, []);

  // Thenables whose then getter or method throws.
  assert.equal(
    emitFailure(f, {
      sink: () => ({
        get then() {
          throw new Error("x");
        },
      }),
    }),
    false,
  );
  assert.equal(emitFailure(f, { sink: () => ({ then: boom }) }), false);

  // The thenable path reverts exactly once.
  let reverts = 0;
  const counting = { admit: () => ({ write: true }), revert: () => reverts++ };
  assert.equal(emitFailure(f, { sink: () => ({ then: boom }), now, limiter: counting }), false);
  assert.equal(reverts, 1);

  // A revert that throws, with a throwing sink.
  const badRevert = { admit: () => ({ write: true }), revert: boom };
  assert.equal(emitFailure(f, { sink: boom, now, limiter: badRevert }), false);

  for (const bad of [null, undefined, "x", 5, []]) assert.equal(emitFailure(bad, { sink: () => {} }), false);
});

test("other error arms: odd clocks, random sources and forged dates", () => {
  assert.equal(
    checkWhat("job", {
      toString() {
        throw new Error("x");
      },
    }),
    null,
  );

  const f = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout" };
  const before = Date.now();
  const line = failureLine(f, { now: () => NaN });
  const after = Date.now();
  assert.notEqual(line, null);
  const o = parse(line);
  const at = Date.parse(o.at);
  assert.ok(at >= before && at <= after, "at from Date.now()");
  assert.match(o.fid, FID_RE);
  const fidMs = parseInt(o.fid.split("-")[1], 36);
  assert.ok(fidMs >= before && fidMs <= after, "fid from Date.now()");

  assert.match(parse(failureLine(f, { now: () => FIXED, rand: () => NaN })).fid, FID_RE);
  assert.match(parse(failureLine(f, { now: () => 5 })).fid, FID_RE);

  const proxied = new Proxy(new Date(0), {});
  assert.equal(parse(failureLine({ ...f, at: proxied }, { now: () => FIXED })).at, new Date(FIXED).toISOString());
  const forged = { getTime: () => 0 };
  assert.equal(parse(failureLine({ ...f, at: forged }, { now: () => FIXED })).at, new Date(FIXED).toISOString());

  const got = [];
  assert.equal(emitFailure(f, { sink: (l) => got.push(l), now: () => NaN }), true);
  assert.match(parse(got[0]).fid, FID_RE);
  assert.ok(Number.isFinite(Date.parse(parse(got[0]).at)));

  const limiter = createRateLimiter();
  assert.equal(emitFailure(f, { sink: () => {}, now: () => NaN, limiter }), true);
  assert.equal(emitFailure(f, { sink: () => {}, now: () => NaN, limiter }), false, "a NaN clock still rate-limits");
});

test("selectors: each vendored selector matches its own builder's text and no other", () => {
  const url = ALERT_HEADER.match(/\]\((.+)\)$/)[1];
  const now = FIXED;
  const waiting = (n) => Array.from({ length: n }, (_, i) => ({ senderName: i % 2 ? "Ada" : "Grace", postedTs: now / 1000 - 600 - i }));
  const loops = [];
  for (const outcome of ["muted", "memory-only", "held", "protected"]) {
    for (const kind of ["dm", "thread"]) {
      loops.push(loopAlertText({ peerId: kind === "dm" ? "conv:42" : "thread:42", kind, senderName: "Ada", turns: 41, minutes: 12, outcome, untilMs: now, why: "disk full", heldSince: "2026-11-17 11:00 UTC" }));
    }
  }
  const texts = {
    "inbox-dead-letter": [deadLetterAlertText({ senderName: "Ada" }, 6, "boom")],
    "inbox-storage": [storageAlertText([]), storageAlertText(waiting(2))],
    "inbox-backlog": [backlogAlertText(waiting(51), now)],
    "inbox-hung-turn": [hungTurnAlertText(30)],
    "inbox-loop-breaker": loops,
  };
  assert.deepEqual(SELECTORS.map((s) => s.id).sort(), Object.keys(texts).sort());
  const res = Object.fromEntries(SELECTORS.map((s) => [s.id, new RegExp(s.body.source, s.body.flags)]));
  for (const s of SELECTORS) {
    for (const text of texts[s.id]) {
      const lines = text.split("\n");
      assert.equal(lines[0], `# ${s.header} via [Stacksbot](` + url + ")");
      const body = lines.slice(1).find((l) => l.trim() !== "");
      const matching = SELECTORS.filter((o) => res[o.id].test(body)).map((o) => o.id);
      assert.deepEqual(matching, [s.id], body);
    }
  }
});

// Pins for the guards that only home's compare-port.mjs checked (twist-plugin-315). Each input is one that only
// its named guard decides. Expected constants and lists are written out from the contract, never read from the module.
const PIN_OPTS = { now: () => FIXED, rand: () => 0 };
const PIN_F = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout" };
const longest = (list) => list.reduce((a, b) => (b.length > a.length ? b : a));

test("pin: MAX_LINE_BYTES", () => {
  assert.equal(MAX_LINE_BYTES, 512);
  // Contract §4: the fullest legal line is 452 bytes, and it is written whole under default options.
  const fields = {
    fid: `f-${(36 ** 8).toString(36)}-abcd`,
    at: new Date(FIXED).toISOString(),
    kind: "model",
    source: longest(SOURCES),
    what: "a/" + "b".repeat(78),
    class: longest(CLASSES),
    cause: longest(CAUSES),
    alerted: false,
    streak: 9999,
    session: longest(SESSIONS),
    job_id: UUID,
    run: UUID,
    attempt: 99,
    suppressed: 999999,
  };
  assert.equal(fields.fid.split("-")[1].length, 9);
  const line = failureLine(fields);
  assert.equal(line, PREFIX + JSON.stringify({ v: 1, ...fields }) + "\n");
  assert.equal(Object.keys(parse(line)).length, 15);
  assert.equal(Buffer.byteLength(line), 452);
});

test("pin: PREFIX literal", () => {
  assert.equal(PREFIX, "STACKSBOT_FAILURE ");
  assert.ok(failureLine({ kind: "inbox" }).startsWith("STACKSBOT_FAILURE {"));
});

test("pin: checkWhat turn rule", () => {
  assert.equal(checkWhat("turn", "cron"), "cron");
  assert.equal(checkWhat("turn", "daily-digest"), null);
  assert.equal(parse(failureLine({ kind: "turn", what: "daily-digest" })).what, "?");
  // The value is a legal name, so only the turn rule rejects it.
  assert.equal(checkWhat("job", "daily-digest"), "daily-digest");
  // Contract §3: a turn's what is one of these five, written out (task-1 T1-1); another list's value is not.
  for (const s of ["cron", "user", "heartbeat", "subagent", "other"]) assert.equal(checkWhat("turn", s), s, s);
  for (const v of ["consumer", "backlog", "inbox"]) assert.equal(checkWhat("turn", v), null, v);
});

test("pin: checkWhat model rule", () => {
  assert.equal(checkWhat("model", "anthropic/claude-x"), "anthropic/claude-x");
  assert.equal(checkWhat("model", "daily-digest"), null);
  assert.equal(parse(failureLine({ kind: "model", what: "anthropic/claude-x" })).what, "anthropic/claude-x");
  assert.equal(parse(failureLine({ kind: "model", what: "daily-digest" })).what, "?");
  assert.equal(checkWhat("job", "daily-digest"), "daily-digest");
  // Contract §3's provider/model pattern (task-1 T1-2): more slashes and a colon after the first slash are fine;
  // an empty or punctuation-led part is not.
  for (const v of ["a/b/c", "openai/gpt-5:mini", "x.y_z-1/m.n_o-2"]) assert.equal(checkWhat("model", v), v, v);
  // Each part may be one character, and may start with a digit (task-1 T1-6, T1-7).
  for (const v of ["x/y", "9x/y", "x/9y", "9/9"]) assert.equal(checkWhat("model", v), v, v);
  for (const v of ["/x", "x/", "-x/y", "x/-y", ".x/y", "x/.y", "a:b/c"]) assert.equal(checkWhat("model", v), null, v);
  // Each part must start with a letter or digit, and the pattern is anchored at both ends (task-1 T1-4).
  for (const c of ["_", ".", "-", ":", "/"]) {
    assert.equal(checkWhat("model", `${c}x/y`), null, `${c}x/y`);
    assert.equal(checkWhat("model", `x/${c}y`), null, `x/${c}y`);
  }
  for (const v of ["!x/y", "x/y!", "x/y z"]) assert.equal(checkWhat("model", v), null, v);
});

test("pin: name, uuid and fid patterns", () => {
  // A job or tool name (contract §3): starts with a letter or digit, then letters, digits and _ . / : -.
  for (const v of ["a.b", "a/b", "a_b", "a-b", "a:b", "9", "x", "9a", "a9"]) assert.equal(checkWhat("job", v), v, v);
  for (const v of ["-x", "_x", ".x", "/x", ":x", "a b", "a+b", "!x", "x!"]) assert.equal(checkWhat("job", v), null, v);

  // job_id and run: a lowercase uuid, the whole value.
  const HEXY = ["a".repeat(8), "bbbb", "4ccc", "8ddd", "e".repeat(12)].join("-"); // has letters, so case shows
  const kept = parse(failureLine({ ...PIN_F, job_id: HEXY, run: HEXY }, PIN_OPTS));
  assert.deepEqual([kept.job_id, kept.run], [HEXY, HEXY]);
  const bare = failureLine(PIN_F, PIN_OPTS);
  for (const bad of [HEXY.toUpperCase(), `x${HEXY}`, `${HEXY}x`, HEXY.slice(0, -1) + "g"]) {
    assert.equal(failureLine({ ...PIN_F, job_id: bad, run: bad }, PIN_OPTS), bare, bad);
  }
  const groups = HEXY.split("-");
  for (let i = 0; i < groups.length; i++) {
    for (const g of [groups[i].slice(1), groups[i] + groups[i][0]]) {
      const bad = groups.map((x, j) => (j === i ? g : x)).join("-");
      assert.equal(failureLine({ ...PIN_F, job_id: bad, run: bad }, PIN_OPTS), bare, bad);
    }
  }

  // fid: f-<8 or 9 base-36 chars>-<4 base-36 chars>; anything else is replaced by a generated one.
  const generated = `f-${FIXED.toString(36)}-0000`;
  for (const fid of [`f-${"1".repeat(8)}-abcd`, `f-${"1".repeat(9)}-abcd`, `f-${"z".repeat(8)}-1234`]) assert.equal(parse(failureLine({ ...PIN_F, fid }, PIN_OPTS)).fid, fid, fid);
  for (const fid of [`f-${"1".repeat(8)}-abcd`.toUpperCase(), `f-${"1".repeat(8)}-ABCD`, `f-${"1".repeat(8)}_abcd`, `f${"1".repeat(8)}-abcd`]) {
    assert.equal(parse(failureLine({ ...PIN_F, fid }, PIN_OPTS)).fid, generated, fid);
  }
  for (const fid of [`f-${"1".repeat(7)}-abcd`, `f-${"1".repeat(10)}-abcd`, `f-${"1".repeat(8)}-abcde`, `f-${"1".repeat(8)}-abc`, `x-${"1".repeat(8)}-abcd`, `xf-${"1".repeat(8)}-abcd`]) {
    assert.equal(parse(failureLine({ ...PIN_F, fid }, PIN_OPTS)).fid, generated, fid);
  }
});

// Contract §3's patterns written as plain checks, independent of the module's regexes (task-1 T1-10 to T1-12).
const LOWER = "abcdefghijklmnopqrstuvwxyz";
const DIGIT = "0123456789";
const inSet = (set) => (c) => set.includes(c);
const alnum = inSet(LOWER + DIGIT);
const nameBody = inSet(LOWER + DIGIT + "_./:-");
const providerBody = inSet(LOWER + DIGIT + "_.-");
const hex = inSet(DIGIT + "abcdef");
/** A job or tool name: [a-z0-9] then [a-z0-9_./:-]. */
const contractName = (v) => v.length > 0 && alnum(v[0]) && [...v.slice(1)].every(nameBody);
/** provider/model: each part starts with [a-z0-9]; the provider holds [a-z0-9_.-], the model [a-z0-9_./:-]. */
function contractModel(v) {
  const i = v.indexOf("/");
  if (i < 0) return false;
  const [p, m] = [v.slice(0, i), v.slice(i + 1)];
  return p.length > 0 && alnum(p[0]) && [...p.slice(1)].every(providerBody) && m.length > 0 && alnum(m[0]) && [...m.slice(1)].every(nameBody);
}
/** A lowercase uuid: hex in groups of 8-4-4-4-12. */
const contractUuid = (v) => v.length === 36 && [...v].every((c, i) => ([8, 13, 18, 23].includes(i) ? c === "-" : hex(c)));
/** A fid: f-<8 or 9 of [0-9a-z]>-<4 of [0-9a-z]>. */
function contractFid(v) {
  const parts = v.split("-");
  return parts.length === 3 && parts[0] === "f" && [8, 9].includes(parts[1].length) && parts[2].length === 4 && [...parts[1] + parts[2]].every(alnum);
}
// Every printable ASCII character, plus tab, newline and a non-ASCII letter (task-1 T1-13, T1-14).
const PROBES = [...Array.from({ length: 95 }, (_, i) => String.fromCharCode(32 + i)), "\t", "\n", "\u00e9"];
/** Every value made by putting one probe, or the position's own character uppercased, at one position of `value`. */
const swaps = (value) =>
  [...value].flatMap((own, i) => [...PROBES, own.toUpperCase()].map((c) => value.slice(0, i) + c + value.slice(i + 1)));

test("pin: every position of each pattern, swapped", () => {
  // checkWhat trims and lowercases before it checks, and refuses two colons; the name base has no colon and the model
  // base one, and both are too short for the secret-shape rule (a single swap makes no token prefix).
  const norm = (v) => v.trim().toLowerCase();
  for (const v of swaps("ab.c-d_e/f")) {
    const n = norm(v);
    const want = contractName(n) && n.split(":").length <= 2 ? n : null;
    assert.equal(checkWhat("job", v), want, `job ${JSON.stringify(v)}`);
  }
  for (const v of swaps("a1.b-c_d/e.f_g-h:9")) {
    const n = norm(v);
    const want = contractModel(n) && n.split(":").length <= 2 ? n : null;
    assert.equal(checkWhat("model", v), want, `model ${JSON.stringify(v)}`);
  }
  const uuid = ["a1b2".repeat(2), "c3d4", "4e5f", "8a9b", "c0d1".repeat(3)].join("-");
  for (const v of swaps(uuid)) {
    const o = parse(failureLine({ ...PIN_F, run: v }, PIN_OPTS));
    assert.equal(o.run, contractUuid(v) ? v : undefined, `uuid ${JSON.stringify(v)}`);
  }
  const generated = `f-${FIXED.toString(36)}-0000`;
  for (const base of [`f-${"a1b2".repeat(2)}-c3d4`, `f-${"a1b2".repeat(2)}z-c3d4`]) {
    for (const v of swaps(base)) {
      const o = parse(failureLine({ ...PIN_F, fid: v }, PIN_OPTS));
      assert.equal(o.fid, contractFid(v) ? v : generated, `fid ${JSON.stringify(v)}`);
    }
  }
});

test("pin: attempt upper bound", () => {
  assert.equal(parse(failureLine({ ...PIN_F, attempt: 99 }, PIN_OPTS)).attempt, 99);
  assert.deepEqual(failureLine({ ...PIN_F, attempt: 100 }, PIN_OPTS), failureLine(PIN_F, PIN_OPTS));
});

test("pin: optional field bounds", () => {
  const kept = [
    ["streak", 9999],
    ["suppressed", 999999],
    ["attempt", 1],
    ["streak", 1],
    ["suppressed", 1],
  ];
  for (const [key, value] of kept) assert.equal(parse(failureLine({ ...PIN_F, [key]: value }, PIN_OPTS))[key], value, `${key} ${value} kept`);
  const dropped = [
    ["streak", 10000],
    ["suppressed", 1000000],
    ["attempt", 0],
    ["streak", 0],
    ["suppressed", 0],
  ];
  const bare = failureLine(PIN_F, PIN_OPTS);
  for (const [key, value] of dropped) assert.deepEqual(failureLine({ ...PIN_F, [key]: value }, PIN_OPTS), bare, `${key} ${value} dropped`);
});

test("pin: CLASSES, CAUSES and SESSIONS are the contract's", () => {
  // Contract §3, written out.
  const classes = [
    "interrupted",
    "exec-failed",
    "mcp-tool",
    "incomplete-turn",
    "subagent",
    "timeout",
    "session-init",
    "unknown",
    "unschedulable",
    "fanout",
    "delivery-failed",
    "smoke-regression",
    "capped",
    "stale",
    "unavailable",
    "runaway-session",
    "billing",
    "fix-run-flagged",
    "retry",
    "dead-letter",
    "loop-trip",
    "storage",
    "backlog",
    "hung-turn",
    "tool-error",
    "turn-error",
    "model-error",
  ];
  const causes = ["http-4xx", "http-5xx", "http-429", "timeout", "connection", "billing", "rate-limited", "bot-down", "none", "unknown"];
  const sessions = ["cron", "user", "heartbeat", "subagent", "other"];
  assert.deepEqual([classes.length, causes.length, sessions.length], [27, 10, 5]);
  assert.deepEqual([...CLASSES].sort(), [...classes].sort());
  assert.deepEqual([...CAUSES].sort(), [...causes].sort());
  assert.deepEqual([...SESSIONS].sort(), [...sessions].sort());
  for (const c of classes) assert.equal(parse(failureLine({ ...PIN_F, class: c }, PIN_OPTS)).class, c, c);
  for (const c of causes) assert.equal(parse(failureLine({ ...PIN_F, cause: c }, PIN_OPTS)).cause, c, c);
  for (const s of sessions) assert.equal(parse(failureLine({ ...PIN_F, session: s }, PIN_OPTS)).session, s, s);
});

test("pin: SKEW_MS and MAX_DELAY_MS", () => {
  assert.equal(SKEW_MS, 60000);
  assert.equal(MAX_DELAY_MS, 420000);
});

test("pin: enum fields need a string", () => {
  const SENDER = "Grace " + "Hopper";
  const asObject = (s) => ({ toString: () => s, sender: SENDER });
  const legal = { kind: "inbox", source: "twist-inbox-retry", class: "retry", cause: "timeout", session: "cron" };
  const control = parse(failureLine({ ...legal, what: "consumer" }, PIN_OPTS));
  for (const key of Object.keys(legal)) assert.equal(control[key], legal[key], `${key} kept as a string`);
  const rejected = {
    kind: (o) => assert.equal(o.kind, "?"),
    source: (o) => assert.equal(o.source, "?"),
    class: (o) => assert.equal(o.class, "unknown"),
    cause: (o) => assert.equal(o.cause, "unknown"),
    session: (o) => assert.equal(Object.hasOwn(o, "session"), false),
  };
  for (const key of Object.keys(legal)) {
    for (const bad of [asObject(legal[key]), [legal[key]]]) {
      const line = failureLine({ ...legal, what: "consumer", [key]: bad }, PIN_OPTS);
      rejected[key](parse(line));
      assert.ok(!line.includes(SENDER), `${key}: no sender`);
    }
  }
});

test("pin: KINDS and INBOX_STEPS are the contract's", () => {
  // Contract §3, written out.
  const kinds = ["job", "sync", "check", "inbox", "tool", "turn", "model"];
  const steps = ["consumer", "dead-letter", "loop-breaker", "storage", "backlog", "watchdog"];
  assert.deepEqual([kinds.length, steps.length], [7, 6]);
  assert.deepEqual([...KINDS].sort(), [...kinds].sort());
  assert.deepEqual([...INBOX_STEPS].sort(), [...steps].sort());
  for (const k of kinds) assert.equal(parse(failureLine({ ...PIN_F, kind: k }, PIN_OPTS)).kind, k, k);
  for (const k of ["extra", "inboxes", "Job", ""]) assert.equal(parse(failureLine({ ...PIN_F, kind: k }, PIN_OPTS)).kind, "?", k);
  for (const s of steps) assert.equal(checkWhat("inbox", s), s, s);
  for (const s of ["queue", "retry", "consumers", "dead_letter"]) assert.equal(checkWhat("inbox", s), null, s);
});

test("pin: streak, attempt and suppressed need an integer", () => {
  const bare = failureLine(PIN_F, PIN_OPTS);
  for (const key of ["streak", "attempt", "suppressed"]) {
    for (const bad of [1.5, 2.000001, "2", "1", Number.NaN, Infinity, true, 2n]) {
      assert.deepEqual(failureLine({ ...PIN_F, [key]: bad }, PIN_OPTS), bare, `${key} ${String(bad)} dropped`);
    }
    assert.equal(parse(failureLine({ ...PIN_F, [key]: 2 }, PIN_OPTS))[key], 2, `${key} 2 kept`);
  }
});

test("pin: checkWhat with no value, and the long-run rule's letter and digit", () => {
  for (const kind of ["job", "sync", "check", "tool", "inbox", "turn", "model"]) {
    assert.equal(checkWhat(kind, undefined), null, `${kind} undefined`);
    assert.equal(checkWhat(kind, null), null, `${kind} null`);
  }
  const { what: _omit, ...noWhat } = PIN_F;
  assert.equal(parse(failureLine({ ...noWhat, kind: "job", source: "job-failure-alert" }, PIN_OPTS)).what, "?");
  assert.equal(parse(failureLine({ ...noWhat, kind: "job", source: "job-failure-alert", what: null }, PIN_OPTS)).what, "?");
  // Contract §3: only a run of 20+ that holds both a letter and a digit is secret-shaped.
  const digits = "1".repeat(20);
  const letters = "b".repeat(20);
  const mixed = "b".repeat(19) + "1";
  const mixedFirst = "1" + "b".repeat(19);
  assert.equal(checkWhat("job", digits), digits, "20 digits kept");
  assert.equal(checkWhat("job", letters), letters, "20 letters kept");
  assert.equal(checkWhat("job", `x-${digits}9`), `x-${digits}9`, "21 digits kept");
  assert.equal(checkWhat("job", mixed), null, "letters then a digit refused");
  assert.equal(checkWhat("job", mixedFirst), null, "a digit then letters refused");
  assert.equal(checkWhat("job", `x-${mixed.slice(1)}`), `x-${mixed.slice(1)}`, "19 mixed kept");
});

test("pin: failureCause, one sample per alternative of each contract §3 row", () => {
  // Contract §3's cause table, each alternative on its own, in both cases.
  const cases = [
    ["billing issue", "billing"],
    ["BILLING", "billing"],
    ["your credit balance is too low", "billing"],
    ["Credit Balance", "billing"],
    ["insufficient credit", "billing"],
    ["insufficient_credit", "billing"],
    ["insufficientcredit", "billing"],
    ["payment required", "billing"],
    ["PAYMENT REQUIRED", "billing"],
    ["402", "billing"],
    ["got 402 back", "billing"],
    ["x402", "unknown"],
    ["4021", "unknown"],
    ["rate limit", "rate-limited"],
    ["ratelimit", "rate-limited"],
    ["RATE_LIMIT", "rate-limited"],
    ["429", "http-429"],
    ["x429", "unknown"],
    ["4290", "unknown"],
    ["timed out", "timeout"],
    ["TIMED OUT", "timeout"],
    ["timedout", "timeout"],
    ["time out", "timeout"],
    ["timeout", "timeout"],
    ["Timeout", "timeout"],
    ["aborted", "timeout"],
    ["ABORTED", "timeout"],
    ["ETIMEDOUT", "timeout"],
    ["etimedout", "timeout"],
    ["ECONNREFUSED", "connection"],
    ["econnrefused", "connection"],
    ["ECONNRESET", "connection"],
    ["econnreset", "connection"],
    ["ENOTFOUND", "connection"],
    ["enotfound", "connection"],
    ["EAI_AGAIN", "connection"],
    ["eai_again", "connection"],
    ["socket hang up", "connection"],
    ["SOCKET HANG UP", "connection"],
    ["http 503", "http-5xx"],
    ["HTTP 500", "http-5xx"],
    ["status 599", "http-5xx"],
    ["STATUS 502", "http-5xx"],
    ["code 503", "http-5xx"],
    ["CODE 503", "http-5xx"],
    ["code503", "unknown"],
    ["code: 503", "http-5xx"],
    ["code:::503", "http-5xx"],
    ["code::::503", "unknown"],
    ["xcode 503", "unknown"],
    ["code 5030", "unknown"],
    ["http 404", "http-4xx"],
    ["HTTP 400", "http-4xx"],
    ["status 499", "http-4xx"],
    ["STATUS 403", "http-4xx"],
    ["code 404", "http-4xx"],
    ["CODE 404", "http-4xx"],
    ["code: 404", "http-4xx"],
    ["code:::404", "http-4xx"],
    ["code::::404", "unknown"],
    ["xcode 404", "unknown"],
    ["code 4040", "unknown"],
  ];
  for (const [text, cause] of cases) assert.equal(failureCause(text), cause, text);
});

test("pin: rate limiter clock steps, pruning and maxKeys", () => {
  const f = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout", alerted: false };
  const a = f;
  const b = { ...f, cause: "connection" };
  const c = { ...f, cause: "none" };

  // A clock that steps back ends the window rather than stretching it.
  const back = createRateLimiter();
  assert.deepEqual(back.admit(a, 100_000), { write: true });
  assert.deepEqual(back.admit(a, 50_000), { write: true });
  assert.deepEqual(back.admit(a, 50_001), { write: false });

  // A full map prunes only finished windows with nothing pending, so a pending count is kept (contract §7).
  const prune = createRateLimiter({ maxKeys: 2 });
  assert.deepEqual(prune.admit(a, 0), { write: true });
  assert.deepEqual(prune.admit(a, 1), { write: false });
  assert.deepEqual(prune.admit(b, 0), { write: true });
  assert.deepEqual(prune.admit(c, 60_000), { write: true });
  assert.deepEqual(prune.admit(a, 60_000), { write: true, suppressed: 1 });
  assert.deepEqual(prune.admit(c, 60_001), { write: false }, "c took b's finished entry");

  // A window still open is not pruned; a key with no room is written untracked.
  const full = createRateLimiter({ maxKeys: 1 });
  assert.deepEqual(full.admit(a, 0), { write: true });
  assert.deepEqual(full.admit(b, 0), { write: true });
  assert.deepEqual(full.admit(b, 1), { write: true }, "b is untracked: the map holds one key");
  assert.deepEqual(full.admit(a, 1), { write: false }, "a's open window was not pruned");
});

test("pin: a real Date as at is kept", () => {
  const at = new Date(FIXED - 5_000);
  assert.equal(parse(failureLine({ ...PIN_F, at }, PIN_OPTS)).at, at.toISOString());
});

test("pin: failureCause rows are tried in contract §3's order", () => {
  // Contract §3's table, top to bottom, with a text that only its own row matches.
  const rows = [
    ["billing", "billing"],
    ["rate limit", "rate-limited"],
    ["429", "http-429"],
    ["timeout", "timeout"],
    ["ECONNRESET", "connection"],
    ["http 503", "http-5xx"],
    ["status 404", "http-4xx"],
  ];
  for (const [text, cause] of rows) assert.equal(failureCause(text), cause, text);
  for (let i = 0; i < rows.length; i++) {
    for (let j = i + 1; j < rows.length; j++) {
      const [a, first] = rows[i];
      const [b] = rows[j];
      assert.equal(failureCause(`${a} ${b}`), first, `${a} ${b}`);
      assert.equal(failureCause(`${b} ${a}`), first, `${b} ${a}`);
    }
  }
  // Any other value is String()ed.
  assert.equal(failureCause(429), "http-429");
  assert.equal(failureCause(402), "billing");
  assert.equal(failureCause(new Error("ECONNRESET")), "connection");
  assert.equal(failureCause("code404"), "unknown");
});

test("pin: the rate limiter's key is all five fields, and a new window starts a new count", () => {
  const f = { kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause: "timeout", alerted: false };
  const other = { kind: "tool", source: "twist-inbox-dead-letter", what: "dead-letter", class: "dead-letter", cause: "connection" };
  for (const key of Object.keys(other)) {
    const lim = createRateLimiter();
    assert.deepEqual(lim.admit(f, 0), { write: true });
    assert.deepEqual(lim.admit({ ...f, [key]: other[key] }, 1), { write: true }, `${key} alone makes a new key`);
    assert.deepEqual(lim.admit(f, 2), { write: false });
  }
  // Contract §7: suppressed counts the lines dropped since the key's last line, so it starts again at each window.
  const lim = createRateLimiter();
  assert.deepEqual(lim.admit(f, 0), { write: true });
  assert.deepEqual(lim.admit(f, 1), { write: false });
  assert.deepEqual(lim.admit(f, 60_000), { write: true, suppressed: 1 });
  assert.deepEqual(lim.admit(f, 120_000), { write: true }, "nothing dropped since the counted line");
  assert.deepEqual(lim.admit(f, 120_001), { write: false });
  assert.deepEqual(lim.admit(f, 120_002), { write: false });
  assert.deepEqual(lim.admit(f, 180_000), { write: true, suppressed: 2 });
});

test("pin: the secret-shape and session-key rules hold for every free-text kind", () => {
  // Contract §3: "For every kind ... two more rules". inbox and turn take only fixed names, so neither rule can be reached there.
  const mixed = "b".repeat(19) + "1";
  const prefix = ["s", "k-"].join("");
  const values = {
    job: { ok: "nightly-run", secret: mixed, token: `${prefix}x`, agent: "agent:x", colons: "a:b:c" },
    sync: { ok: "fatal", secret: mixed, token: `${prefix}x`, agent: "agent:x", colons: "a:b:c" },
    check: { ok: "stale", secret: mixed, token: `${prefix}x`, agent: "agent:x", colons: "a:b:c" },
    tool: { ok: "twist_search", secret: mixed, token: `${prefix}x`, agent: "agent:x", colons: "a:b:c" },
    model: { ok: "a/b", secret: `a/${mixed}`, token: `a/${prefix}x`, colons: "a/b:c:d" },
  };
  for (const [kind, v] of Object.entries(values)) {
    assert.equal(checkWhat(kind, v.ok), v.ok, `${kind} control`);
    for (const rule of ["secret", "token", "agent", "colons"]) {
      if (v[rule] === undefined) continue;
      assert.equal(checkWhat(kind, v[rule]), null, `${kind} ${rule}`);
    }
  }
  assert.equal(checkWhat("job", "agent-x:y"), "agent-x:y", "one colon and no agent: prefix is kept");
});

/** A random source that steps through base36 digits 0, 1, 2, ... in turn, one per call, so each fid tail is known. */
const steppedRand = () => {
  let i = 0;
  return () => ((i++ % 36) + 0.5) / 36;
};

test("pin: v, at and alerted take only their contract §3 values", () => {
  const opts = { now: () => FIXED, rand: () => 0 };
  // `v` is always 1, whatever the caller gives.
  for (const v of [2, 0, "1", null, true, 1.5]) assert.equal(parse(failureLine({ ...PIN_F, v }, opts)).v, 1, `v ${String(v)}`);
  // `alerted` is true only for the boolean true.
  assert.equal(parse(failureLine({ ...PIN_F, alerted: true }, opts)).alerted, true);
  for (const alerted of [1, "1", "true", [1], [true], {}, { valueOf: () => true }, false, 0, null]) {
    assert.equal(parse(failureLine({ ...PIN_F, alerted }, opts)).alerted, false, `alerted ${JSON.stringify(alerted)}`);
  }
  // `at` is written in toISOString() form only; a time in another form is converted, a non-time gives the clock.
  const at = (value) => parse(failureLine({ ...PIN_F, at: value }, opts)).at;
  assert.equal(at("2026-11-17T12:00:00Z"), "2026-11-17T12:00:00.000Z");
  assert.equal(at("2026-11-17T13:00:00+01:00"), "2026-11-17T12:00:00.000Z");
  assert.equal(at("17 Nov 2026 12:00 UTC"), "2026-11-17T12:00:00.000Z");
  assert.equal(at(Date.parse("2026-11-16T00:00:00.000Z")), "2026-11-16T00:00:00.000Z");
  for (const bad of ["soon", "", Number.NaN, Infinity, {}, [FIXED]]) assert.equal(at(bad), new Date(FIXED).toISOString(), `at ${String(bad)}`);
});

test("pin: the fid's tail is four base36 digits from the random source", () => {
  const ms36 = FIXED.toString(36);
  assert.equal(parse(failureLine(PIN_F, { now: () => FIXED, rand: steppedRand() })).fid, `f-${ms36}-0123`);
  const picks = [0.5 / 36, 1.5 / 36, 10.5 / 36, 35.5 / 36];
  let k = 0;
  assert.equal(parse(failureLine(PIN_F, { now: () => FIXED, rand: () => picks[k++] })).fid, `f-${ms36}-01az`);
});

test("pin: emitFailure reads the caller's fields once and passes its clock and random source on", () => {
  let reads = 0;
  const fields = { ...PIN_F, alerted: false };
  Object.defineProperty(fields, "what", { enumerable: true, get: () => (reads++, "consumer") });
  const got = [];
  const sink = (l) => got.push(l);
  const limiter = createRateLimiter();
  const rand = steppedRand();
  let t = FIXED;
  const now = () => t;
  assert.equal(emitFailure(fields, { sink, now, rand, limiter }), true);
  t += 1_000;
  assert.equal(emitFailure(fields, { sink, now, rand, limiter }), false);
  t += 60_000;
  assert.equal(emitFailure(fields, { sink, now, rand, limiter }), true);
  assert.equal(reads, 3, "one read of what per call");
  assert.equal(got.length, 2);
  const [first, counted] = got.map(parse);
  assert.equal(first.fid, `f-${FIXED.toString(36)}-0123`);
  assert.equal(first.at, new Date(FIXED).toISOString());
  // The dropped call drew 4567; the counted line keeps the fid made by its own one read.
  assert.equal(counted.fid, `f-${(FIXED + 61_000).toString(36)}-89ab`);
  assert.equal(counted.at, new Date(FIXED + 61_000).toISOString());
  assert.equal(counted.suppressed, 1);
});

test("pin: the name pattern holds for every name kind, and what is String()ed", () => {
  for (const kind of ["job", "sync", "check", "tool"]) {
    assert.equal(checkWhat(kind, "a-b_c.d/e:f"), "a-b_c.d/e:f", `${kind} control`);
    for (const bad of ["a b", "_x", "-x", ".x", "a#b", "aé", "a,b", "a".repeat(81)]) assert.equal(checkWhat(kind, bad), null, `${kind} ${bad}`);
    assert.equal(checkWhat(kind, 5), "5", `${kind} 5`);
    assert.equal(checkWhat(kind, 0), "0", `${kind} 0`);
    assert.equal(checkWhat(kind, false), "false", `${kind} false`);
    assert.equal(checkWhat(kind, " Mixed-Case "), "mixed-case", `${kind} trimmed and lowercased`);
  }
});

test("pin: the default limiter tracks 10000 keys", () => {
  const f = { kind: "inbox", source: "twist-inbox-retry", class: "retry", cause: "timeout", alerted: false };
  const key = (i) => ({ ...f, what: `w${i}` });
  const lim = createRateLimiter();
  for (let i = 0; i < 10_000; i++) assert.deepEqual(lim.admit(key(i), 0), { write: true });
  assert.deepEqual(lim.admit(key(10_000), 0), { write: true });
  assert.deepEqual(lim.admit(key(10_000), 1), { write: true }, "the 10001st key is written untracked");
  assert.deepEqual(lim.admit(key(9_999), 1), { write: false }, "the 10000th key is tracked");
  assert.deepEqual(lim.admit(key(0), 1), { write: false }, "the first key is tracked");
});
