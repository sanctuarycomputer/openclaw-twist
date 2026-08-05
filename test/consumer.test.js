import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createConsumer, BACKOFF_MS, MAX_ATTEMPTS, isPermanentError } from "../src/consumer.js";

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
async function harness({ items = [], clock = { t: T0 }, runTurn, probe, classifyPeer, admission } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistc-"));
  const queue = createQueueStore(join(dir, "queue.json"));
  await queue.load();
  await queue.enqueueAll(items, clock.t);
  const calls = { turns: [], reacts: [], alerts: [], replies: [] };
  const consumer = createConsumer({
    queue, botUserId: BOT, now: () => clock.t, log: () => {},
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
  const { queue, consumer } = await harness({
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
