import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createConsumer, BACKOFF_MS, MAX_ATTEMPTS, MAX_GLOBAL_TURNS, HIGH_WATER, HUNG_TURN_ALERT_MS, isPermanentError } from "../src/consumer.js";

const BOT = 634870;
const T0 = 1_785_900_000_000;
const MENTION = `[Bot](twist-mention://${BOT}) hello`;

function baseItem(id, over = {}) {
  return {
    id, kind: "conv", peerId: "conv:1", conversationId: 1, messageId: 500,
    objIndex: 0, senderId: 427360, senderName: "Hugh", content: MENTION, postedTs: 1785900000,
    firstSightBacklog: false, state: "queued", attempts: 0, nextAttemptAt: 0, enqueuedAt: T0, ...over,
  };
}
async function harness({ items = [], clock = { t: T0 }, runTurn, probe, classifyPeer, admission, queue: sharedQueue, inFlight, calls: sharedCalls } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistc-"));
  const queue = sharedQueue ?? createQueueStore(join(dir, "queue.json"));
  if (!sharedQueue) await queue.load();
  await queue.enqueueAll(items, clock.t);
  const calls = sharedCalls ?? { turns: [], reacts: [], alerts: [], replies: [] };
  const consumer = createConsumer({
    queue, botUserId: BOT, now: () => clock.t, log: () => {},
    ...(inFlight ? { inFlight } : {}),
    classifyPeer: classifyPeer ?? (async () => "groupdm"),
    admission: admission ?? (async () => ({ admit: true, admission: "dispatch", commandAuthorized: false })),
    runTurn: runTurn ?? (async (item) => { calls.turns.push(item.id); }),
    probe: probe ?? (async () => false),
    react: async (item, verb, emoji) => { calls.reacts.push([item.id, verb, emoji]); },
    alert: async (text) => { calls.alerts.push(text); },
    replyInPlace: async (item, text) => { calls.replies.push([item.id, text]); },
  });
  return { queue, consumer, calls, clock };
}
async function drain(consumer) { await consumer.tick(); await consumer.idle(); await consumer.tick(); await consumer.idle(); } // claim pass + settle pass
// Flush pending microtasks without waiting on any promise to settle. Needed where a
// claimed item's settle() is deliberately left in-flight (blocked on an external gate) —
// consumer.idle() would hang in that case since it awaits ALL tracked settle promises.
const flushMicrotasks = () => new Promise((resolve) => setImmediate(resolve));

test("happy path: mention dispatched, reactions cycled, item done", async () => {
  const { queue, consumer, calls } = await harness({ items: [baseItem("conv-msg:500")] });
  await drain(consumer);
  assert.deepEqual(calls.turns, ["conv-msg:500"]);
  assert.equal(queue.get("conv-msg:500").state, "done");
  assert.deepEqual(calls.reacts.map((r) => `${r[1]}:${r[2]}`), ["add:⏳", "remove:⏳", "add:✅"]);
});

test("policy skips are terminal and reasoned: no-mention / backlog / admission", async () => {
  const { queue, consumer, calls } = await harness({
    items: [
      baseItem("conv-msg:1", { content: "no mention here" }),
      baseItem("conv-msg:2", { peerId: "conv:2", firstSightBacklog: true }),
      baseItem("conv-msg:3", { peerId: "conv:3" }),
    ],
    admission: async (item) => ({ admit: item.id !== "conv-msg:3", admission: item.id === "conv-msg:3" ? "deny" : "dispatch", commandAuthorized: false }),
  });
  await drain(consumer); await drain(consumer);
  assert.equal(queue.get("conv-msg:1").reason, "no-mention");
  assert.equal(queue.get("conv-msg:2").reason, "backlog");
  assert.equal(queue.get("conv-msg:3").reason, "admission:deny");
  for (const id of ["conv-msg:1", "conv-msg:2", "conv-msg:3"]) assert.equal(queue.get(id).state, "skipped");
  assert.equal(calls.turns.length, 0); // none of these should ever have reached runTurn
});

test("dms bypass the mention requirement", async () => {
  const { queue, consumer, calls } = await harness({
    items: [baseItem("conv-msg:9", { content: "plain question" })],
    classifyPeer: async () => "dm",
  });
  await drain(consumer);
  assert.deepEqual(calls.turns, ["conv-msg:9"]);
  assert.equal(queue.get("conv-msg:9").state, "done");
});

test("transient failure rides the backoff ladder then dead-letters loudly", async () => {
  const clock = { t: T0 };
  const { queue, consumer, calls } = await harness({
    clock, runTurn: async () => { throw new Error("model exploded"); },
    items: [baseItem("conv-msg:500")],
  });
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    await drain(consumer);
    const it = queue.get("conv-msg:500");
    if (attempt < MAX_ATTEMPTS) {
      assert.equal(it.state, "queued", `attempt ${attempt}`);
      assert.equal(it.nextAttemptAt, clock.t + BACKOFF_MS[attempt - 1]);
      clock.t = it.nextAttemptAt;
    } else {
      assert.equal(it.state, "failed");
    }
  }
  assert.equal(calls.replies.length, 1);            // in-place apology
  assert.equal(calls.alerts.length, 1);             // ops alert
  assert.ok(calls.reacts.some((r) => r[2] === "❌"));
});

test("permanent errors classify to skipped:gone without retries", async () => {
  const err = Object.assign(new Error("Message not found"), { status: 404 });
  const { queue, consumer } = await harness({ runTurn: async () => { throw err; }, items: [baseItem("conv-msg:500")] });
  await drain(consumer);
  assert.equal(queue.get("conv-msg:500").state, "skipped");
  assert.equal(queue.get("conv-msg:500").reason, "gone");
  assert.equal(isPermanentError(err), true);
  // Ruling: generic 400s are NOT permanent — they ride the retry ladder like anything else.
  assert.equal(isPermanentError(Object.assign(new Error("bad request"), { status: 400 })), false);
});

test("per-peer serialization: second item in same peer waits, then runs", async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const { queue, consumer, calls } = await harness({
    runTurn: async (item) => { calls.turns.push(item.id); if (item.id === "conv-msg:1") await gate; },
    items: [baseItem("conv-msg:1", { postedTs: 100 }), baseItem("conv-msg:2", { postedTs: 200 })],
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1", "conv-msg:1"].slice(0, 1)); // only first claimed
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, 1); // still waiting on same peer
  release(); await Promise.resolve(); await drain(consumer);
  assert.equal(calls.turns.length, 2);
  assert.equal(queue.get("conv-msg:2").state, "done");
});

test("boot recovery: orphan with reply-after-claim is done; silent orphan requeues", async () => {
  const { queue, consumer } = await harness({
    items: [baseItem("conv-msg:1", { peerId: "conv:1" }), baseItem("conv-msg:2", { peerId: "conv:2" })],
    probe: async (item) => item.id === "conv-msg:1",
  });
  await queue.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: 1 });
  await queue.transition("conv-msg:2", { state: "processing", claimedAt: T0, attempts: 2 });
  await consumer.recoverOrphans();
  assert.equal(queue.get("conv-msg:1").state, "done");
  assert.equal(queue.get("conv-msg:2").state, "queued");
  assert.equal(queue.get("conv-msg:2").attempts, 2); // attempts preserved
});

test("recoverOrphans: a rejecting probe requeues the item rather than throwing", async () => {
  const { queue, consumer } = await harness({
    items: [baseItem("conv-msg:1")],
    probe: async () => { throw new Error("probe blew up"); },
  });
  await queue.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: 1 });
  await assert.doesNotReject(() => consumer.recoverOrphans());
  assert.equal(queue.get("conv-msg:1").state, "queued");
});

test("MAX_GLOBAL_TURNS caps concurrent claims across distinct peers", async () => {
  const gates = [];
  const items = [1, 2, 3, 4].map((n) => baseItem(`g-${n}`, { peerId: `conv:g-${n}` }));
  const { consumer, calls } = await harness({
    items,
    runTurn: async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); },
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS); // only 3 of the 4 distinct-peer items started
  assert.equal(consumer.inFlightCount(), MAX_GLOBAL_TURNS);
  gates.forEach((g) => g());
  await consumer.idle();
});

// An in-process channel restart (stopChannel + startChannel, no drain) leaves the outgoing
// consumer's turns running while a NEW consumer is built over the same queue file. The
// shared store alone does not protect the invariants — a live turn's own item is
// "processing" (so it can't be re-claimed), but OTHER queued items on that peer would still
// look claimable to a consumer whose in-flight map is empty. The shared map is what makes
// per-peer exclusion and the global cap span instances.
test("restart overlap: a second consumer sharing queue+inFlight respects per-peer exclusion", async () => {
  const gates = [];
  const inFlight = new Map();
  const calls = { turns: [], reacts: [], alerts: [], replies: [] };
  const runTurn = async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); };
  // Instance A ("outgoing"): claims peer P's oldest item; its turn never settles.
  const a = await harness({
    inFlight, calls, runTurn,
    items: [baseItem("conv-msg:1", { peerId: "conv:P", postedTs: 100 }), baseItem("conv-msg:2", { peerId: "conv:P", postedTs: 200 })],
  });
  await a.consumer.tick();
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1"]);

  // Instance B ("incoming"): same queue store, same in-flight map, fresh consumer.
  const b = await harness({ queue: a.queue, inFlight, calls, runTurn });
  await b.consumer.tick();
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1"]); // conv-msg:2 NOT claimed — peer P is busy
  assert.equal(b.consumer.inFlightCount(), 1);   // B sees A's turn

  gates.forEach((g) => g());
  await a.consumer.idle();
  await b.consumer.tick(); // peer freed: B now runs the second item
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1", "conv-msg:2"]);
  gates.forEach((g) => g()); // release the second turn's gate too
  await b.consumer.idle();
  assert.equal(b.queue.get("conv-msg:2").state, "done");
});

test("restart overlap: MAX_GLOBAL_TURNS spans consumer instances via the shared inFlight map", async () => {
  const gates = [];
  const inFlight = new Map();
  const calls = { turns: [], reacts: [], alerts: [], replies: [] };
  const runTurn = async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); };
  const a = await harness({
    inFlight, calls, runTurn,
    items: Array.from({ length: MAX_GLOBAL_TURNS }, (_, i) => baseItem(`g-${i}`, { peerId: `conv:g-${i}` })),
  });
  await a.consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS);

  // Fresh consumer, distinct peers still queued — but every global slot is already taken.
  const b = await harness({
    queue: a.queue, inFlight, calls, runTurn,
    items: [baseItem("g-extra", { peerId: "conv:g-extra" })],
  });
  await b.consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS); // no 2×cap during the overlap
  assert.equal(b.queue.get("g-extra").state, "queued");

  gates.forEach((g) => g());
  await a.consumer.idle(); await b.consumer.idle();
});

test("HIGH_WATER alert fires once when queue depth exceeds the threshold, not again next tick", async () => {
  const gates = [];
  const items = Array.from({ length: HIGH_WATER + 1 }, (_, i) => baseItem(`hw-${i}`, { peerId: `conv:hw-${i}` }));
  const { consumer, calls } = await harness({
    items,
    runTurn: async () => new Promise((resolve) => gates.push(resolve)), // never settles: items stay non-terminal
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("high-water")).length, 1);
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("high-water")).length, 1); // no re-alert
  gates.forEach((g) => g());
  await consumer.idle();
});

test("hung-turn alert fires once after HUNG_TURN_ALERT_MS, not again next tick", async () => {
  const clock = { t: T0 };
  let release;
  const gate = new Promise((r) => { release = r; });
  const { consumer, calls } = await harness({
    clock, items: [baseItem("conv-msg:1")],
    runTurn: async () => { await gate; },
  });
  await consumer.tick();
  await flushMicrotasks();
  clock.t += HUNG_TURN_ALERT_MS + 1;
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("running >")).length, 1);
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("running >")).length, 1); // no re-alert
  release();
  await consumer.idle();
});
