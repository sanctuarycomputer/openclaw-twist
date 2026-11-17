// The consumer's and the loop breaker's failure lines (spec What changes 3 and 4): one `inbox` line per retry (rate-limited), dead letter,
// storage, backlog and hung-turn alert, `alerted` from the alert's result, and nothing from the item or the error
// text but the closed `cause`.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createConsumer, BACKOFF_MS, MAX_ATTEMPTS, HIGH_WATER, HUNG_TURN_ALERT_MS, backlogAlertText } from "../src/consumer.js";
import { PREFIX } from "../src/failure-line.js";
import { createOpsAlert } from "../src/ops-alert.js";
import { createMuteList, RECHECK_MS } from "../src/mutes.js";
import { createCriticalNotifier, createLoopBreaker } from "../src/loop-breaker.js";

const BOT = 634870;
const T0 = Date.parse("2031-03-04T05:06:07.000Z");
const SEC = T0 / 1000;
const MENTION = `[Bot](twist-mention://${BOT}) hello`;
const iso = (ms) => new Date(ms).toISOString();
const FID_RE = /^f-[0-9a-z]{8,9}-[0-9a-z]{4}$/;

function baseItem(id, over = {}) {
  return {
    id, kind: "conv", peerId: `conv:${id}`, conversationId: 1, messageId: 500,
    objIndex: 0, senderId: 427360, senderName: "Hugh", content: MENTION, postedTs: SEC,
    firstSightBacklog: false, state: "queued", attempts: 0, nextAttemptAt: 0, enqueuedAt: T0, ...over,
  };
}

/**
 * A consumer over a real queue in a temp dir. Lines go to `lines` unless `failureSink` is given (null: none passed). `alert` is the result a test sets: a function of (text, clock)
 * whose return (or throw) is what the consumer's alert returns; the default resolves undefined, as every
 * existing stub does. `rawAlert` replaces the alert outright.
 */
async function harness({ items = [], clock = { t: T0 }, runTurn, probe, alert, rawAlert, failureSink, failureLimiter } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistfl-"));
  const queue = createQueueStore(join(dir, "queue.json"));
  await queue.load();
  await queue.enqueueAll(items, clock.t);
  const calls = { turns: [], reacts: [], alerts: [], replies: [] };
  const lines = [];
  const logs = [];
  const consumer = createConsumer({
    queue, botUserId: BOT, now: () => clock.t, log: (s) => logs.push(s),
    classifyPeer: async () => "groupdm",
    admission: async () => ({ admit: true, admission: "dispatch", commandAuthorized: false }),
    runTurn: runTurn ?? (async (item) => { calls.turns.push(item.id); }),
    probe: probe ?? (async () => false),
    react: async (item, verb, emoji) => { calls.reacts.push([item.id, verb, emoji]); },
    alert: rawAlert ?? ((text) => {
      calls.alerts.push(text);
      return alert ? alert(text, clock) : Promise.resolve(undefined);
    }),
    replyInPlace: async (item, text) => { calls.replies.push([item.id, text]); },
    // `failureSink: null` builds the consumer without one, so it falls back to its default.
    ...(failureSink === null ? {} : { failureSink: failureSink ?? ((l) => lines.push(l)) }),
    ...(failureLimiter ? { failureLimiter } : {}),
  });
  return { queue, consumer, calls, clock, lines, logs };
}
const flushMicrotasks = () => new Promise((resolve) => setImmediate(resolve));
const once = async (consumer) => { await consumer.tick(); await consumer.idle(); };

/** Each line: the prefix, one `\n` at the end, a valid fid; returns the parsed line without the fid. */
function parsed(lines) {
  return lines.map((l) => {
    assert.ok(l.startsWith(PREFIX), l);
    assert.ok(l.endsWith("\n"), l);
    assert.equal(l.indexOf("\n"), l.length - 1, l);
    const { fid, ...rest } = JSON.parse(l.slice(PREFIX.length));
    assert.match(fid, FID_RE);
    return rest;
  });
}

const retryLine = (at, cause, attempt, extra = {}) => ({ v: 1, at: iso(at), kind: "inbox", source: "twist-inbox-retry", what: "consumer", class: "retry", cause, alerted: false, attempt, ...extra });
const deadLine = (at, cause, alerted) => ({ v: 1, at: iso(at), kind: "inbox", source: "twist-inbox-dead-letter", what: "dead-letter", class: "dead-letter", cause, alerted });
const storageLine = (at, alerted, cause = "unknown") => ({ v: 1, at: iso(at), kind: "inbox", source: "twist-inbox-storage", what: "storage", class: "storage", cause, alerted });
const backlogLine = (at, alerted) => ({ v: 1, at: iso(at), kind: "inbox", source: "twist-inbox-backlog", what: "backlog", class: "backlog", cause: "none", alerted });
const hungLine = (at, alerted) => ({ v: 1, at: iso(at), kind: "inbox", source: "twist-inbox-hung-turn", what: "watchdog", class: "hung-turn", cause: "none", alerted });

// ------------------------------------------------------------------- the five scenarios, shared by several tests

/** A retryable failure: claimed as attempt `attempts + 1`, rejecting with `message`. */
async function retryScenario({ message = "socket hang up", attempts = 1, items, ...opts } = {}) {
  const h = await harness({
    items: items ?? [baseItem("conv-msg:1", { attempts })],
    runTurn: async () => { throw new Error(message); },
    ...opts,
  });
  await once(h.consumer);
  return h;
}
/** An exhausted item: claimed as attempt MAX_ATTEMPTS, rejecting with `message`. */
async function deadLetterScenario({ message = "socket hang up", items, ...opts } = {}) {
  const h = await harness({
    items: items ?? [baseItem("conv-msg:1", { attempts: MAX_ATTEMPTS - 1 })],
    runTurn: async () => { throw new Error(message); },
    ...opts,
  });
  await once(h.consumer);
  return h;
}
/** A store whose claim persist rejects ENOSPC (`message`). */
async function storageScenario({ message = "ENOSPC: no space left on device", items, ...opts } = {}) {
  const h = await harness({ items: items ?? [baseItem("conv-msg:1")], ...opts });
  const real = h.queue.transition.bind(h.queue);
  h.queue.transition = async (id, patch, now) => {
    if (patch.state === "processing") throw new Error(message);
    return real(id, patch, now);
  };
  await h.consumer.tick();
  await flushMicrotasks();
  await h.consumer.idle();
  return h;
}
/** HIGH_WATER + 1 items waiting for a reply. */
async function backlogScenario({ items, ...opts } = {}) {
  const h = await harness({ items: items ?? Array.from({ length: HIGH_WATER + 1 }, (_, i) => baseItem(`hw-${i}`)), ...opts });
  await once(h.consumer);
  return h;
}
/** A turn left running past HUNG_TURN_ALERT_MS; returns the detection time as `hungAt`. */
async function hungScenario({ items, ...opts } = {}) {
  let release;
  const gate = new Promise((r) => { release = r; });
  const h = await harness({ items: items ?? [baseItem("conv-msg:1")], runTurn: async () => { await gate; }, ...opts });
  await h.consumer.tick();
  await flushMicrotasks();
  h.clock.t += HUNG_TURN_ALERT_MS + 1;
  const hungAt = h.clock.t;
  await h.consumer.tick();
  await flushMicrotasks();
  release();
  await h.consumer.idle();
  return { ...h, hungAt };
}

// ------------------------------------------------------------------- A2.1 retry

test("A2.1 a retryable failure on attempt 2 writes one retry line; the log, state and backoff are unchanged", async () => {
  const { queue, lines, logs, clock } = await retryScenario({ attempts: 1 });
  assert.deepEqual(parsed(lines), [retryLine(T0, "connection", 2)]);
  assert.ok(logs.some((s) => s.startsWith("turn failed for conv-msg:1 (attempt 2)")), logs.join("\n"));
  assert.equal(queue.get("conv-msg:1").state, "queued");
  assert.equal(queue.get("conv-msg:1").nextAttemptAt, clock.t + BACKOFF_MS[1]);
});

test("A2.1 the attempt is the item's own: attempt 4 with another cause", async () => {
  const { lines, logs } = await retryScenario({ attempts: 3, message: "timed out" });
  assert.deepEqual(parsed(lines), [retryLine(T0, "timeout", 4)]);
  assert.ok(logs.some((s) => s.startsWith("turn failed for conv-msg:1 (attempt 4)")));
});

// ------------------------------------------------------------------- A2.2 rate limit

test("A2.2 retries share the limiter: one line per cause per 60 s, the next one carries suppressed", async () => {
  const clock = { t: T0 };
  const err = { A: "socket hang up", B: "socket hang up", C: "socket hang up", D: "timed out" };
  const h = await harness({
    clock,
    runTurn: async (item) => {
      if (item.attempts === 1) throw new Error(err[item.id]);
      h.calls.turns.push(item.id);
    },
  });
  const { queue, consumer, lines } = h;
  const at = async (offsetS, id) => {
    clock.t = T0 + offsetS * 1000;
    if (id) await queue.enqueueAll([baseItem(id)], clock.t);
    await once(consumer);
  };
  await at(0, "A");
  await at(10, "B");
  await at(20, "D");
  assert.deepEqual(parsed(lines), [retryLine(T0, "connection", 1), retryLine(T0 + 20_000, "timeout", 1)]);
  await at(55); // A, B and D are past their 30 s backoff and now succeed: no line
  for (const id of ["A", "B", "D"]) assert.equal(queue.get(id).state, "done", id);
  assert.equal(lines.length, 2);
  await at(61, "C");
  assert.deepEqual(parsed(lines), [
    retryLine(T0, "connection", 1),
    retryLine(T0 + 20_000, "timeout", 1),
    retryLine(T0 + 61_000, "connection", 1, { suppressed: 1 }),
  ]);
});

test("A2.2 a permanent error (404) writes no line", async () => {
  const { queue, lines } = await retryScenario({
    items: [baseItem("conv-msg:1")],
    runTurn: async () => { throw Object.assign(new Error("gone"), { status: 404 }); },
  });
  assert.equal(queue.get("conv-msg:1").reason, "gone");
  assert.deepEqual(lines, []);
});

// ------------------------------------------------------------------- A2.3 / A2.4 dead letter

test("A2.3 an exhausted item writes exactly one dead-letter line and no retry line", async () => {
  const { queue, lines, calls } = await deadLetterScenario({ message: "socket hang up" });
  assert.equal(queue.get("conv-msg:1").state, "failed");
  assert.equal(calls.alerts.length, 1);
  assert.deepEqual(parsed(lines), [deadLine(T0, "connection", true)]);
});

test("A2.3 boot recovery's poison item writes one dead-letter line, cause unknown", async () => {
  const h = await harness({ items: [baseItem("conv-msg:1", { attempts: MAX_ATTEMPTS })], probe: async () => false });
  await h.queue.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: MAX_ATTEMPTS });
  await h.consumer.recoverOrphans();
  assert.equal(h.queue.get("conv-msg:1").state, "failed");
  assert.deepEqual(parsed(h.lines), [deadLine(T0, "unknown", true)]);
});

test("A2.4 two dead letters 10 s apart whose alerts resolve false: two lines, both alerted false", async () => {
  const clock = { t: T0 };
  const h = await harness({
    clock,
    items: [baseItem("a", { attempts: MAX_ATTEMPTS - 1 })],
    runTurn: async () => { throw new Error("socket hang up"); },
    alert: async () => false,
  });
  await once(h.consumer);
  clock.t = T0 + 10_000;
  await h.queue.enqueueAll([baseItem("b", { attempts: MAX_ATTEMPTS - 1 })], clock.t);
  await once(h.consumer);
  assert.equal(h.calls.alerts.length, 2);
  assert.deepEqual(parsed(h.lines), [deadLine(T0, "connection", false), deadLine(T0 + 10_000, "connection", false)]);
});

// ------------------------------------------------------------------- A2.5 / A2.5b alerted from the alert's result

// The alert is slow: it advances the clock 5 s before it settles, so `at` must be read before it.
const slow = (result) => async (_text, clock) => {
  clock.t += 5_000;
  if (result === "reject") throw new Error("post failed");
  return result;
};
const RESULTS = [[undefined, true], [true, true], ["reject", true], [false, false]];

for (const [result, alerted] of RESULTS) {
  const name = result === "reject" ? "rejects" : `resolves ${result}`;

  test(`A2.5 storage: an alert that ${name} gives alerted ${alerted}, at the detection time`, async () => {
    const { lines, calls } = await storageScenario({ alert: slow(result) });
    assert.equal(calls.alerts.length, 1);
    assert.deepEqual(parsed(lines), [storageLine(T0, alerted)]);
  });

  test(`A2.5 backlog: an alert that ${name} gives alerted ${alerted}, at the detection time`, async () => {
    const { lines, calls } = await backlogScenario({ alert: slow(result) });
    assert.equal(calls.alerts.length, 1);
    assert.deepEqual(parsed(lines), [backlogLine(T0, alerted)]);
  });

  test(`A2.5 hung turn: an alert that ${name} gives alerted ${alerted}, at the detection time`, async () => {
    const { lines, calls, hungAt } = await hungScenario({ alert: slow(result) });
    assert.equal(calls.alerts.length, 1);
    assert.deepEqual(parsed(lines), [hungLine(hungAt, alerted)]);
  });

  test(`A2.5 dead letter: an alert that ${name} gives alerted ${alerted}, at the detection time`, async () => {
    const { lines, calls } = await deadLetterScenario({ alert: slow(result) });
    assert.equal(calls.alerts.length, 1);
    assert.deepEqual(parsed(lines), [deadLine(T0, "connection", alerted)]);
  });
}

test("A2.5b a rejecting storage or backlog alert is logged by safeAlert as today and counts as alerted", async () => {
  const s = await storageScenario({ alert: async () => { throw new Error("post failed"); } });
  assert.ok(s.logs.includes("alert delivery failed: Error: post failed"), s.logs.join("\n"));
  assert.deepEqual(parsed(s.lines), [storageLine(T0, true)]);
  const b = await backlogScenario({ alert: async () => { throw new Error("post failed"); } });
  assert.ok(b.logs.includes("alert delivery failed: Error: post failed"), b.logs.join("\n"));
  assert.deepEqual(parsed(b.lines), [backlogLine(T0, true)]);
});

test("A2.5b a storage or backlog alert that throws synchronously counts as alerted", async () => {
  const thrower = () => { throw new Error("sync boom"); };
  const s = await storageScenario({ alert: thrower });
  assert.ok(s.logs.includes("alert delivery failed: Error: sync boom"));
  assert.deepEqual(parsed(s.lines), [storageLine(T0, true)]);
  const b = await backlogScenario({ alert: thrower });
  assert.ok(b.logs.includes("alert delivery failed: Error: sync boom"));
  assert.deepEqual(parsed(b.lines), [backlogLine(T0, true)]);
});

test("A2.5b a rejecting dead-letter or hung-turn alert counts as alerted", async () => {
  const reject = async () => { throw new Error("post failed"); };
  const d = await deadLetterScenario({ alert: reject });
  assert.deepEqual(parsed(d.lines), [deadLine(T0, "connection", true)]);
  const g = await hungScenario({ alert: reject });
  assert.deepEqual(parsed(g.lines), [hungLine(g.hungAt, true)]);
});

// ------------------------------------------------------------------- A2.6 end to end through createOpsAlert

test("A2.6 a real ops alert holding back a 1 h old repeat of the backlog incident: nothing posted, alerted false", async () => {
  const clock = { t: T0 };
  const posts = [];
  const opsLogs = [];
  const items = Array.from({ length: HIGH_WATER + 1 }, (_, i) => baseItem(`hw-${i}`));
  const earlier = backlogAlertText(items.slice(0, 7), T0 - 3600_000);
  const rawAlert = createOpsAlert({
    target: () => ({ kind: "thread", id: 777 }),
    readRecent: async () => [{ creator: BOT, posted_ts: (clock.t - 3600_000) / 1000, content: earlier }],
    post: async (arg) => { posts.push(arg); },
    botUserId: BOT,
    log: (s) => opsLogs.push(s),
    now: () => clock.t,
  });
  const { lines } = await backlogScenario({ clock, items, rawAlert });
  assert.deepEqual(posts, []);
  assert.equal(opsLogs.length, 1);
  assert.match(opsLogs[0], /^alert not repeated/);
  assert.deepEqual(parsed(lines), [backlogLine(T0, false)]);
});

// ------------------------------------------------------------------- A1.3 no text

test("A1.3 no item, comment, conversation or user id, sender name or error text reaches a line", async () => {
  const n1 = String(800_000 + 7_331);
  const n2 = String(600_000 + 4_219);
  const ITEM = `conv-msg:${n1}:${n2}`;
  const CONV = String(9_000_000 + 41_377);
  const COMMENT = String(5_000_000 + 28_613);
  const USER = String(3_000_000 + 77_101);
  const SENDER = "Quillon Brackenridge";
  const ERR = "upstream said bramblewick ECONNRESET";
  const SECRET = "sk-" + "a1".repeat(12);
  const marked = (suffix, over = {}) => baseItem(`${ITEM}${suffix}`, {
    peerId: `conv:${CONV}${suffix}`, conversationId: Number(CONV), commentId: Number(COMMENT), senderId: Number(USER), senderName: SENDER, ...over,
  });
  const message = `${ERR} ${SECRET} for ${ITEM} in ${CONV}`;
  const all = [];
  all.push(...(await retryScenario({ message, items: [marked("r", { attempts: 1 })] })).lines);
  all.push(...(await deadLetterScenario({ message, items: [marked("d", { attempts: MAX_ATTEMPTS - 1 })] })).lines);
  const poison = await harness({ items: [marked("p", { attempts: MAX_ATTEMPTS })] });
  await poison.queue.transition(`${ITEM}p`, { state: "processing", claimedAt: T0, attempts: MAX_ATTEMPTS });
  await poison.consumer.recoverOrphans();
  all.push(...poison.lines);
  all.push(...(await storageScenario({ message: `ENOSPC ${message}`, items: [marked("s")] })).lines);
  all.push(...(await backlogScenario({ items: Array.from({ length: HIGH_WATER + 1 }, (_, i) => marked(`b${i}`)) })).lines);
  all.push(...(await hungScenario({ items: [marked("h")] })).lines);
  assert.equal(all.length, 6, all.join(""));
  for (const line of all) {
    for (const marker of [ITEM, n1, n2, CONV, COMMENT, USER, SENDER, "Quillon", ERR, "bramblewick", SECRET, "a1a1a1"]) {
      assert.ok(!line.includes(marker), `${marker} in ${line}`);
    }
  }
});

// ------------------------------------------------------------------- A2.8 a broken sink or limiter changes no outcome

const SCENARIOS = { retry: retryScenario, "dead-letter": deadLetterScenario, storage: storageScenario, backlog: backlogScenario, "hung-turn": hungScenario };
const outcome = ({ queue, calls }) => ({
  states: Object.fromEntries(["queued", "processing", "done", "failed", "skipped"].flatMap((s) => queue.itemsInState(s).map((i) => [i.id, s]))),
  reacts: calls.reacts,
  replies: calls.replies,
  alerts: calls.alerts,
});
const FAULTS = {
  "a sink that throws": { failureSink: () => { throw new Error("sink down"); } },
  "a sink returning a rejected promise": { failureSink: () => Promise.reject(new Error("x")) },
  "a limiter whose admit throws": { failureLimiter: { admit() { throw new Error("limiter down"); }, revert() {} } },
};

for (const [fault, deps] of Object.entries(FAULTS)) {
  test(`A2.8 ${fault} leaves every consumer outcome as a working sink's`, async () => {
    const unhandled = [];
    const onUnhandled = (reason) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      for (const [name, scenario] of Object.entries(SCENARIOS)) {
        const working = await scenario();
        assert.ok(working.lines.length >= 1, name);
        const broken = await scenario(deps);
        assert.deepEqual(outcome(broken), outcome(working), name);
      }
      await new Promise((r) => setTimeout(r, 20));
      assert.deepEqual(unhandled, []);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });
}

// ------------------------------------------------------------------- the default sink, the unlimited alert lines, one line per incident

test("T3-1 without a failureSink the consumer writes its line to process.stdout", async () => {
  const realWrite = process.stdout.write;
  const written = [];
  process.stdout.write = function (chunk, ...rest) {
    if (typeof chunk === "string" && chunk.startsWith(PREFIX)) { written.push(chunk); return true; }
    return realWrite.call(this, chunk, ...rest);
  };
  try {
    await retryScenario({ attempts: 1, failureSink: null });
  } finally {
    process.stdout.write = realWrite;
  }
  assert.equal(written.length, 1, written.join(""));
  assert.deepEqual(parsed(written), [retryLine(T0, "connection", 2)]);
});

test("T3-2/T3-3 alert lines never pass the limiter: one that refuses everything still lets them through", async () => {
  const refuse = () => ({ admit: () => ({ write: false }), revert() {} });
  assert.deepEqual((await retryScenario({ failureLimiter: refuse() })).lines, []); // the retry line does pass it
  assert.deepEqual(parsed((await deadLetterScenario({ failureLimiter: refuse() })).lines), [deadLine(T0, "connection", true)]);
  assert.deepEqual(parsed((await storageScenario({ failureLimiter: refuse() })).lines), [storageLine(T0, true)]);
  assert.deepEqual(parsed((await backlogScenario({ failureLimiter: refuse() })).lines), [backlogLine(T0, true)]);
  const g = await hungScenario({ failureLimiter: refuse() });
  assert.deepEqual(parsed(g.lines), [hungLine(g.hungAt, true)]);
});

test("T3-2 two turns hung in the same tick, both alerts held back as repeats: two lines, both alerted false", async () => {
  const g = await hungScenario({ items: [baseItem("a"), baseItem("b")], alert: async () => false });
  assert.equal(g.calls.alerts.length, 2);
  assert.deepEqual(parsed(g.lines), [hungLine(g.hungAt, false), hungLine(g.hungAt, false)]);
});

test("T3-4 a second storage failure in the same process writes no second line", async () => {
  const s = await storageScenario();
  await s.consumer.tick();
  await flushMicrotasks();
  assert.ok(s.logs.filter((l) => l.startsWith("queue persist failed")).length >= 2, s.logs.join("\n")); // it did fail again
  assert.equal(s.calls.alerts.length, 1);
  assert.deepEqual(parsed(s.lines), [storageLine(T0, true)]);
});

test("T3-4 a later tick still over high water writes no second backlog line", async () => {
  const gates = [];
  const b = await harness({
    items: Array.from({ length: HIGH_WATER + 10 }, (_, i) => baseItem(`hw-${i}`)),
    runTurn: () => new Promise((resolve) => gates.push(resolve)), // turns stay running: only 3 items leave the queue
  });
  await b.consumer.tick();
  await flushMicrotasks();
  await b.consumer.tick();
  await flushMicrotasks();
  assert.ok(b.queue.itemsInState("queued").length > HIGH_WATER); // still over the mark on the second tick
  assert.equal(b.calls.alerts.length, 1);
  assert.deepEqual(parsed(b.lines), [backlogLine(T0, true)]);
  gates.forEach((g) => g());
  await b.consumer.idle();
});

test("T3-5 the storage cause is read from the error: a timeout says timeout", async () => {
  const s = await storageScenario({ message: "ETIMEDOUT writing the queue" });
  assert.deepEqual(parsed(s.lines), [storageLine(T0, true, "timeout")]);
});

// ------------------------------------------------------------------- the loop breaker (What changes 4)

const loopLine = (at, alerted) => ({ v: 1, at: iso(at), kind: "inbox", source: "twist-inbox-loop-breaker", what: "loop-breaker", class: "loop-trip", cause: "none", alerted });
const LOOP_PEER = "conv:4242";

/**
 * A `maxTurns: 1` breaker over a real mute file in a temp dir; wakes are captured. `notify` is the Critical's
 * result (a function of the text; default `{ messageId: "1", dm: true }`). Lines go to `lines` unless
 * `failureSink` is given (null: none passed, so the default sink).
 */
function loopHarness({ notify, neverMute, failureSink } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistlbfl-"));
  const path = join(dir, "twist-muted.json");
  const clock = { t: T0 };
  const lines = [];
  const logs = [];
  const alerts = [];
  const wakes = [];
  const verdicts = [];
  const mutes = createMuteList({ path, ...(neverMute ? { neverMute } : {}), log: (m) => logs.push(m), now: () => clock.t });
  const breaker = createLoopBreaker({
    mutes,
    notify: async (text) => {
      alerts.push(text);
      return notify ? notify(text) : { messageId: "1", dm: true };
    },
    config: { enabled: true, maxTurns: 1, windowMinutes: 30, muteHours: 6 },
    log: (m) => logs.push(m),
    now: () => clock.t,
    env: {},
    appendWake: (row, opts) => { wakes.push({ row, opts }); return true; },
    ...(failureSink === null ? {} : { failureSink: failureSink ?? ((l) => lines.push(l)) }),
  });
  /** Two turns 1 s apart: the second trips. Returns the trip time. */
  const trip = async (peerId = LOOP_PEER, senderName = "Ada Bot") => {
    for (let i = 0; i < 2; i++) {
      if (i) clock.t += 1000;
      verdicts.push(await breaker.admit({ id: `x${i}:${clock.t}`, kind: "conv", peerId, senderId: 2601, senderName }, { kind: "dm" }));
    }
    const at = clock.t;
    clock.t += 1000;
    return at;
  };
  /** A person cuts the breaker's mute of `peerId` short (twist-mute.mjs sets until = now), seen after a recheck. */
  const unmuteByHand = (peerId = LOOP_PEER) => {
    const file = JSON.parse(readFileSync(path, "utf8"));
    file.conversations[peerId.split(":")[1]].until = new Date(clock.t).toISOString();
    writeFileSync(path, JSON.stringify(file));
    clock.t += RECHECK_MS;
  };
  const muteFile = () => (existsSync(path) ? readFileSync(path, "utf8") : null);
  return { dir, breaker, clock, lines, logs, alerts, wakes, verdicts, mutes, trip, unmuteByHand, muteFile };
}

const failingPost = async () => { throw new Error("503"); };
const criticalVia = (fallbackResult) => createCriticalNotifier({ post: failingPost, fallback: async () => fallbackResult, log: () => {}, to: "conv:1367817" });

test("A2.7 a trip whose Critical posts writes one loop-trip line, alerted true, at the trip time", async () => {
  const h = loopHarness();
  const at = await h.trip();
  assert.deepEqual(h.verdicts, [true, false]);
  assert.equal(h.mutes.isMuted(LOOP_PEER), true);
  assert.deepEqual(parsed(h.lines), [loopLine(at, true)]);
});

test("A2.7 through a real createCriticalNotifier: a fallback that sent the alert is alerted true", async () => {
  const h = loopHarness({ notify: criticalVia(undefined) });
  const at = await h.trip();
  assert.deepEqual(parsed(h.lines), [loopLine(at, true)]);
  assert.equal(h.wakes.length, 1);
  assert.equal(h.wakes[0].row.dm_posted, false);
  assert.equal(h.wakes[0].row.message_id, null);
});

test("A2.7 through a real createCriticalNotifier: a fallback held back as a repeat is alerted false; the wake has no DM", async () => {
  const h = loopHarness({ notify: criticalVia(false) });
  const at = await h.trip();
  assert.deepEqual(h.verdicts, [true, false]);
  assert.equal(h.mutes.isMuted(LOOP_PEER), true);
  assert.deepEqual(parsed(h.lines), [loopLine(at, false)]);
  assert.equal(h.wakes.length, 1);
  assert.equal(h.wakes[0].row.dm_posted, false);
  assert.equal(h.wakes[0].row.message_id, null);
});

test("A2.7 createCriticalNotifier: a post's id; null after a fallback that sent; held after one that held back", async () => {
  const ok = createCriticalNotifier({ post: async () => ({ messageId: "7", kind: "conv" }), fallback: async () => false, log: () => {}, to: "conv:1367817" });
  assert.deepEqual(await ok("hi"), { messageId: "7", dm: true });
  assert.equal(await criticalVia(undefined)("hi"), null);
  assert.deepStrictEqual(await criticalVia(false)("hi"), { messageId: undefined, dm: false, held: true });
});

test("A2.7 a protected peer: the first trip alerts (true), a second inside muteHours sends nothing (false)", async () => {
  const h = loopHarness({ neverMute: [LOOP_PEER] });
  const first = await h.trip();
  const second = await h.trip();
  assert.deepEqual(h.verdicts, [true, true, true, true]);
  assert.equal(h.alerts.length, 1);
  assert.equal(h.wakes.length, 1);
  assert.deepEqual(parsed(h.lines), [loopLine(first, true), loopLine(second, false)]);
});

test("A2.7 a held peer (its mute cut short by hand): the first held trip alerts (true), a second inside muteHours does not (false)", async () => {
  const h = loopHarness();
  const muted = await h.trip();
  h.unmuteByHand();
  const held1 = await h.trip();
  const held2 = await h.trip();
  assert.deepEqual(h.verdicts, [true, false, true, true, true, true]);
  assert.equal(h.alerts.length, 2);
  assert.match(h.alerts[1], /still answering it because someone unmuted it/);
  assert.deepEqual(parsed(h.lines), [loopLine(muted, true), loopLine(held1, true), loopLine(held2, false)]);
});

test("A1.3 (loop half) no peer id, conversation id or sender name reaches a loop-trip line", async () => {
  const CONV = String(9_000_000 + 41_377);
  const SENDER = "Quillon Brackenridge";
  const all = [];
  const m = loopHarness();
  await m.trip(`conv:${CONV}`, SENDER);
  all.push(...m.lines);
  const p = loopHarness({ neverMute: [`conv:${CONV}`] });
  await p.trip(`conv:${CONV}`, SENDER);
  await p.trip(`conv:${CONV}`, SENDER);
  all.push(...p.lines);
  assert.equal(all.length, 3, all.join(""));
  assert.ok(m.alerts[0].includes(CONV) && m.alerts[0].includes(SENDER), "the markers were in play");
  for (const line of all) {
    for (const marker of [CONV, `conv:${CONV}`, SENDER, "Quillon", "Ada"]) assert.ok(!line.includes(marker), `${marker} in ${line}`);
  }
});

/** Everything a trip does apart from its line (the logs with the run's temp dir taken out). */
const loopOutcome = (h) => ({ verdicts: h.verdicts, alerts: h.alerts, wakes: h.wakes, mutes: h.muteFile(), logs: h.logs.map((l) => l.split(h.dir).join("<dir>")) });

test("A2.7 a Critical result whose held getter throws: no line, and the trip is otherwise a plain posted one", async () => {
  const plain = loopHarness();
  await plain.trip();
  const odd = loopHarness({
    notify: () => ({ messageId: "1", dm: true, get held() { throw new Error("getter boom"); } }),
  });
  await odd.trip();
  assert.deepEqual(odd.lines, []);
  assert.deepEqual(odd.verdicts, [true, false]);
  assert.equal(odd.mutes.isMuted(LOOP_PEER), true);
  assert.deepEqual(loopOutcome(odd), loopOutcome(plain));
});

const LOOP_SCENARIOS = {
  muted: async (opts) => { const h = loopHarness(opts); await h.trip(); return h; },
  "fallback held": async (opts) => { const h = loopHarness({ notify: criticalVia(false), ...opts }); await h.trip(); return h; },
  protected: async (opts) => { const h = loopHarness({ neverMute: [LOOP_PEER], ...opts }); await h.trip(); await h.trip(); return h; },
  held: async (opts) => { const h = loopHarness(opts); await h.trip(); h.unmuteByHand(); await h.trip(); await h.trip(); return h; },
};
const LOOP_FAULTS = {
  "a sink that throws": { failureSink: () => { throw new Error("sink down"); } },
  "a sink returning a rejected promise": { failureSink: () => Promise.reject(new Error("x")) },
};

for (const [fault, deps] of Object.entries(LOOP_FAULTS)) {
  test(`A2.8 ${fault} leaves every loop-breaker outcome as a working sink's`, async () => {
    const unhandled = [];
    const onUnhandled = (reason) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      for (const [name, scenario] of Object.entries(LOOP_SCENARIOS)) {
        const working = await scenario({});
        assert.ok(working.lines.length >= 1, name);
        const broken = await scenario(deps);
        assert.deepEqual(loopOutcome(broken), loopOutcome(working), name);
      }
      await new Promise((r) => setTimeout(r, 20));
      assert.deepEqual(unhandled, []);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });
}

test("T3-1 (loop half) without a failureSink the loop breaker writes its line to process.stdout", async () => {
  const realWrite = process.stdout.write;
  const written = [];
  process.stdout.write = function (chunk, ...rest) {
    if (typeof chunk === "string" && chunk.startsWith(PREFIX)) { written.push(chunk); return true; }
    return realWrite.call(this, chunk, ...rest);
  };
  let at;
  try {
    at = await loopHarness({ failureSink: null }).trip();
  } finally {
    process.stdout.write = realWrite;
  }
  assert.equal(written.length, 1, written.join(""));
  assert.deepEqual(parsed(written), [loopLine(at, true)]);
});
