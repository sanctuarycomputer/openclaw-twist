import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  PREFIX,
  VERSION,
  SOURCES,
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
});

test("pin: checkWhat model rule", () => {
  assert.equal(checkWhat("model", "anthropic/claude-x"), "anthropic/claude-x");
  assert.equal(checkWhat("model", "daily-digest"), null);
  assert.equal(parse(failureLine({ kind: "model", what: "anthropic/claude-x" })).what, "anthropic/claude-x");
  assert.equal(parse(failureLine({ kind: "model", what: "daily-digest" })).what, "?");
  assert.equal(checkWhat("job", "daily-digest"), "daily-digest");
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
