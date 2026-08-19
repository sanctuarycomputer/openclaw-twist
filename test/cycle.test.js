import test from "node:test";
import assert from "node:assert/strict";
import {
  CYCLE_DEADLINE_MS,
  MAX_CONCURRENT_HINT_SWEEPS,
  MAX_PENDING_HINTS,
  composeCycleSignal,
  createHintFunnel,
  createHintSweeper,
} from "../src/cycle.js";

const hint = (kind, id) => ({ kind, id: String(id) });
const keys = (hints) => hints.map((h) => `${h.kind}:${h.id}`).sort();
const hintKeyOf = (h) => `${h.kind}:${h.id}`;

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

// -------------------------------------------------------- createHintSweeper

/** A sweep whose completion the test controls. */
function deferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}
const swept = { swept: true };

test("sweeper: a hint starts a sweep immediately and pokes onSwept when it lands", async () => {
  const gate = deferred();
  const seen = [];
  const s = createHintSweeper({ sweep: async (h) => { seen.push(hintKeyOf(h)); return await gate.promise; }, onSwept: (h) => seen.push(`swept:${hintKeyOf(h)}`) });

  assert.equal(s.dispatch(hint("thread", 1)), "started");
  await null;
  assert.deepEqual(seen, ["thread:1"], "the sweep began without waiting for anything");
  assert.equal(s.size(), 1);

  gate.resolve(swept);
  await s.idle();
  assert.deepEqual(seen, ["thread:1", "swept:thread:1"]);
  assert.equal(s.size(), 0, "the slot is released");
});

// The whole point of the bypass: a hint must not queue behind a poll cycle.
test("sweeper: a sweep completes even while the exclusive chain is wedged forever", async () => {
  const acked = [];
  // Stand-in for a cycle that never finishes — nothing in the sweeper may await it.
  const wedgedChain = new Promise(() => {});
  const s = createHintSweeper({
    sweep: async (h) => { acked.push(`ack:${hintKeyOf(h)}`); return swept; },
    onSwept: () => { void wedgedChain; },
  });

  s.dispatch(hint("conversation", 7));
  await Promise.race([s.idle(), wedgedChain]);

  assert.deepEqual(acked, ["ack:conversation:7"], "the ⏳ went out despite the blocked chain");
  assert.equal(s.size(), 0);
});

test("sweeper: a second hint for a container already sweeping is dropped", async () => {
  const gate = deferred();
  let calls = 0;
  const s = createHintSweeper({ sweep: async () => { calls++; return await gate.promise; } });

  assert.equal(s.dispatch(hint("thread", 1)), "started");
  assert.equal(s.dispatch(hint("thread", 1)), "in-flight");
  assert.equal(s.dispatch(hint("thread", 1)), "in-flight");
  assert.equal(s.size(), 1);

  gate.resolve(swept);
  await s.idle();
  assert.equal(calls, 1, "the running sweep fetches current state, so it already covers them");
});

test("sweeper: different containers sweep concurrently", async () => {
  const gates = { "thread:1": deferred(), "conversation:1": deferred(), "thread:2": deferred() };
  const started = [];
  const s = createHintSweeper({
    sweep: async (h) => { started.push(hintKeyOf(h)); return await gates[hintKeyOf(h)].promise; },
  });

  s.dispatch(hint("thread", 1));
  s.dispatch(hint("conversation", 1)); // same id, different kind
  s.dispatch(hint("thread", 2));
  await null;

  assert.deepEqual(started.sort(), ["conversation:1", "thread:1", "thread:2"]);
  assert.equal(s.size(), 3, "all three in flight at once — no serialization between containers");

  for (const g of Object.values(gates)) g.resolve(swept);
  await s.idle();
  assert.equal(s.size(), 0);
});

test("sweeper: a container can be swept again once its previous sweep finishes", async () => {
  let gate = deferred();
  let calls = 0;
  const s = createHintSweeper({ sweep: async () => { calls++; return await gate.promise; } });

  s.dispatch(hint("thread", 1));
  gate.resolve(swept);
  await s.idle();

  gate = deferred();
  assert.equal(s.dispatch(hint("thread", 1)), "started", "the guard is per-sweep, not permanent");
  gate.resolve(swept);
  await s.idle();
  assert.equal(calls, 2);
});

test("sweeper: beyond maxConcurrent, hints degrade instead of spawning unbounded sweeps", async () => {
  const gate = deferred();
  const degraded = [];
  const s = createHintSweeper({
    maxConcurrent: 2,
    sweep: async () => await gate.promise,
    onDegrade: (h, reason) => degraded.push([hintKeyOf(h), reason]),
  });

  assert.equal(s.dispatch(hint("thread", 1)), "started");
  assert.equal(s.dispatch(hint("thread", 2)), "started");
  assert.equal(s.dispatch(hint("thread", 3)), "at-capacity");
  assert.equal(s.size(), 2, "the cap holds");
  assert.deepEqual(degraded, [["thread:3", "at-capacity"]], "the caller is told, so it can fall back");

  gate.resolve(swept);
  await s.idle();
  assert.equal(s.dispatch(hint("thread", 3)), "started", "capacity frees up again");
});

test("sweeper: a declined sweep routes to onDegrade with its reason, not onSwept", async () => {
  const events = [];
  const s = createHintSweeper({
    sweep: async () => ({ swept: false, reason: "untracked-thread" }),
    onSwept: () => events.push("swept"),
    onDegrade: (h, reason) => events.push(`degrade:${reason}`),
  });

  s.dispatch(hint("thread", 1));
  await s.idle();
  assert.deepEqual(events, ["degrade:untracked-thread"], "the untracked gate still degrades to a full poll");
});

test("sweeper: a rejecting sweep is contained, logged, and frees the container", async () => {
  const logs = [];
  const events = [];
  const s = createHintSweeper({
    sweep: async () => { throw new Error("twist exploded"); },
    onSwept: () => events.push("swept"),
    onDegrade: () => events.push("degrade"),
    log: (m) => logs.push(m),
  });

  s.dispatch(hint("thread", 1));
  await s.idle();

  assert.deepEqual(events, [], "a failure is neither a sweep nor a degrade — the poll still covers it");
  assert.match(logs.join("\n"), /twist exploded/);
  assert.equal(s.size(), 0, "the slot is released");
  assert.equal(s.dispatch(hint("thread", 1)), "started", "and the container is not wedged");
});

// A synchronously-throwing sweep is the ordering trap: `finally` must not run before the
// in-flight entry exists, or the container would be blocked forever.
test("sweeper: a SYNCHRONOUSLY throwing sweep does not wedge the container", async () => {
  const logs = [];
  const s = createHintSweeper({
    sweep: () => { throw new Error("sync boom"); },
    log: (m) => logs.push(m),
  });

  s.dispatch(hint("thread", 1));
  await s.idle();

  assert.equal(s.size(), 0, "no stale in-flight entry");
  assert.match(logs.join("\n"), /sync boom/);
  assert.equal(s.dispatch(hint("thread", 1)), "started");
});

test("sweeper: sweep is required", () => {
  assert.throws(() => createHintSweeper({}), /sweep is required/);
});

test("MAX_CONCURRENT_HINT_SWEEPS is a small, deliberate bound", () => {
  assert.ok(MAX_CONCURRENT_HINT_SWEEPS >= 1 && MAX_CONCURRENT_HINT_SWEEPS <= 8);
});
