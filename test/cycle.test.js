import test from "node:test";
import assert from "node:assert/strict";
import {
  CYCLE_DEADLINE_MS,
  MAX_PENDING_HINTS,
  composeCycleSignal,
  createHintFunnel,
} from "../src/cycle.js";

const hint = (kind, id) => ({ kind, id: String(id) });
const keys = (hints) => hints.map((h) => `${h.kind}:${h.id}`).sort();

// ------------------------------------------------------------------- funnel

test("funnel: distinct containers accumulate and drain once", () => {
  const f = createHintFunnel();
  assert.equal(f.push(hint("thread", 1)), "queued");
  assert.equal(f.push(hint("conversation", 2)), "queued");
  assert.equal(f.size(), 2);

  const { hints, sweepAll } = f.drain();
  assert.deepEqual(keys(hints), ["conversation:2", "thread:1"]);
  assert.equal(sweepAll, false);
  assert.equal(f.size(), 0, "drain resets");
  assert.deepEqual(f.drain(), { hints: [], sweepAll: false }, "a second drain is empty");
});

test("funnel: repeated hints for one container coalesce to a single entry", () => {
  const f = createHintFunnel();
  f.push(hint("thread", 1));
  f.push(hint("thread", 1));
  f.push(hint("thread", 1));
  assert.equal(f.size(), 1);
  assert.equal(f.drain().hints.length, 1);
});

test('funnel: the reserved "all" hint asks for a full sweep without occupying a slot', () => {
  const f = createHintFunnel();
  assert.equal(f.push({ kind: "all", id: "*" }), "full-sweep");
  assert.equal(f.size(), 0);

  const { hints, sweepAll } = f.drain();
  assert.deepEqual(hints, []);
  assert.equal(sweepAll, true);
  assert.equal(f.drain().sweepAll, false, "the full-sweep request is consumed, not sticky");
});

// H1(a): a flood must not queue unbounded work. Dropping hints is always safe because a
// full poll is a strict superset of any set of targeted sweeps.
test("funnel: exceeding maxPending CLEARS the map and falls back to a full poll", () => {
  const logs = [];
  const f = createHintFunnel({ maxPending: 4, log: (m) => logs.push(m) });

  for (let i = 1; i <= 4; i++) assert.equal(f.push(hint("thread", i)), "queued");
  assert.equal(f.size(), 4, "at the cap, hints are still retained");

  assert.equal(f.push(hint("thread", 5)), "overflow");
  assert.equal(f.size(), 0, "the map is cleared, not left at the cap");

  const { hints, sweepAll } = f.drain();
  assert.deepEqual(hints, [], "no targeted sweeps survive an overflow");
  assert.equal(sweepAll, true, "a full poll covers every dropped hint");
  assert.match(logs.join("\n"), /exceeded 4 pending containers/);
});

test("funnel: overflow is driven by DISTINCT containers, not raw event volume", () => {
  const f = createHintFunnel({ maxPending: 4 });
  for (let i = 0; i < 500; i++) f.push(hint("thread", 1)); // one container, hammered
  assert.equal(f.size(), 1);
  assert.equal(f.drain().sweepAll, false);
});

test("funnel: it recovers after an overflow (the cap is not sticky)", () => {
  const f = createHintFunnel({ maxPending: 2 });
  f.push(hint("thread", 1));
  f.push(hint("thread", 2));
  f.push(hint("thread", 3)); // overflow
  f.drain();

  assert.equal(f.push(hint("thread", 9)), "queued");
  const { hints, sweepAll } = f.drain();
  assert.deepEqual(keys(hints), ["thread:9"]);
  assert.equal(sweepAll, false);
});

// H1(b): an independent bound on the cycle body's cost, so no future change to how hints
// reach the map can make a cycle unbounded without tripping this too.
test("funnel: drain caps hints per cycle and promotes the remainder to a full poll", () => {
  const logs = [];
  const f = createHintFunnel({ maxPending: 32, maxPerCycle: 2, log: (m) => logs.push(m) });
  for (let i = 1; i <= 5; i++) f.push(hint("thread", i));

  const { hints, sweepAll } = f.drain();
  assert.equal(hints.length, 2, "the cycle body's cost is bounded");
  assert.equal(sweepAll, true, "the untouched containers are covered by the full poll");
  assert.match(logs.join("\n"), /truncated to 2 containers/);
  assert.equal(f.size(), 0, "the remainder is dropped, not carried forward into a backlog");
});

test("funnel: defaults cap at MAX_PENDING_HINTS", () => {
  const f = createHintFunnel();
  for (let i = 1; i <= MAX_PENDING_HINTS; i++) f.push(hint("thread", i));
  assert.equal(f.size(), MAX_PENDING_HINTS);
  assert.equal(f.push(hint("thread", MAX_PENDING_HINTS + 1)), "overflow");
  assert.equal(f.drain().sweepAll, true);
});

test("funnel: requestFullSweep forces a full poll on the next drain only", () => {
  const f = createHintFunnel();
  f.requestFullSweep();
  assert.equal(f.fullSweepPending(), true);
  assert.equal(f.drain().sweepAll, true);
  assert.equal(f.drain().sweepAll, false);
});

// ------------------------------------------------------- composeCycleSignal

test("composeCycleSignal: composes the account signal with a fresh deadline", () => {
  const calls = [];
  const account = new AbortController().signal;
  const fakeDeadline = new AbortController().signal;
  const composed = new AbortController().signal;

  const result = composeCycleSignal(account, {
    deadlineMs: 1234,
    timeout: (ms) => (calls.push(["timeout", ms]), fakeDeadline),
    any: (signals) => (calls.push(["any", signals.length]), composed),
  });

  assert.deepEqual(calls, [
    ["timeout", 1234],
    ["any", 2],
  ]);
  assert.equal(result.signal, composed);
  assert.equal(result.deadline, fakeDeadline, "the deadline is returned separately so the caller can tell WHY it aborted");
});

test("composeCycleSignal: with no account signal, the deadline alone is the signal", () => {
  const fakeDeadline = new AbortController().signal;
  const result = composeCycleSignal(undefined, { timeout: () => fakeDeadline, any: () => assert.fail("any must not be called") });
  assert.equal(result.signal, fakeDeadline);
  assert.equal(result.deadline, fakeDeadline);
});

test("composeCycleSignal: degrades to the account signal when the host lacks AbortSignal.any", () => {
  const account = new AbortController().signal;
  const result = composeCycleSignal(account, { timeout: () => new AbortController().signal, any: null });
  assert.equal(result.signal, account, "still cancellable on shutdown");
  assert.equal(result.deadline, null, "and the caller knows there is no deadline to blame");
});

test("composeCycleSignal: degrades when the host lacks AbortSignal.timeout", () => {
  const account = new AbortController().signal;
  const result = composeCycleSignal(account, { timeout: null });
  assert.equal(result.signal, account);
  assert.equal(result.deadline, null);
});

test("composeCycleSignal: the real composition aborts when the account signal aborts", async () => {
  const controller = new AbortController();
  const { signal, deadline } = composeCycleSignal(controller.signal, { deadlineMs: 60_000 });
  assert.equal(signal.aborted, false);

  controller.abort();
  await new Promise((r) => setImmediate(r));
  assert.equal(signal.aborted, true);
  assert.equal(deadline.aborted, false, "shutdown is distinguishable from a blown deadline");
});

test("composeCycleSignal: the real composition aborts when the deadline expires", async () => {
  const controller = new AbortController();
  const { signal, deadline } = composeCycleSignal(controller.signal, { deadlineMs: 5 });

  await new Promise((r) => setTimeout(r, 25));
  assert.equal(deadline.aborted, true);
  assert.equal(signal.aborted, true);
  assert.equal(controller.signal.aborted, false, "the account signal is untouched — only this cycle is cancelled");
});

test("composeCycleSignal: each call gets its own deadline (cycles do not share a clock)", () => {
  const account = new AbortController().signal;
  const a = composeCycleSignal(account, { deadlineMs: 60_000 });
  const b = composeCycleSignal(account, { deadlineMs: 60_000 });
  assert.notEqual(a.deadline, b.deadline);
  assert.notEqual(a.signal, b.signal);
});

test("CYCLE_DEADLINE_MS is two minutes", () => {
  assert.equal(CYCLE_DEADLINE_MS, 120_000);
});
