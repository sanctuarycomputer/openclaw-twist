// test/queue.test.js
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";

const T0 = 1_785_900_000_000;
function item(id, over = {}) {
  return {
    id, kind: "conv", peerId: "conv:1", conversationId: 1, messageId: id.split(":")[1],
    objIndex: 0, senderId: 42, senderName: "Hugh", content: "hi", postedTs: 1785900000,
    firstSightBacklog: false, state: "queued", attempts: 0, nextAttemptAt: 0,
    enqueuedAt: T0, ...over,
  };
}
function freshStore() {
  const dir = mkdtempSync(join(tmpdir(), "twistq-"));
  return { path: join(dir, "queue.json"), dir };
}

test("enqueueAll persists new items once and dedups by id", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  assert.equal(await q.enqueueAll([item("conv-msg:1"), item("conv-msg:2")], T0), 2);
  assert.equal(await q.enqueueAll([item("conv-msg:2"), item("conv-msg:3")], T0), 1);
  assert.equal(q.itemsInState("queued").length, 3);
  const reloaded = createQueueStore(path);
  await reloaded.load();
  assert.equal(reloaded.itemsInState("queued").length, 3); // survived restart
});

test("transition merges patch, stamps finishedAt on terminal, persists", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1")], T0);
  const claimed = await q.transition("conv-msg:1", { state: "processing", claimedAt: T0 + 5, attempts: 1 });
  assert.equal(claimed.state, "processing");
  const done = await q.transition("conv-msg:1", { state: "done" }, T0 + 9);
  assert.equal(done.finishedAt, T0 + 9);
  const reloaded = createQueueStore(path);
  await reloaded.load();
  assert.equal(reloaded.get("conv-msg:1").state, "done");
});

test("prune tombstones old terminal items; tombstoned ids still dedup", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1")], T0);
  await q.transition("conv-msg:1", { state: "done" }, T0);
  const THIRTYONE_DAYS = 31 * 24 * 3600 * 1000;
  await q.prune(T0 + THIRTYONE_DAYS);
  assert.equal(q.get("conv-msg:1"), undefined);
  assert.equal(q.has("conv-msg:1"), true); // tombstone
  assert.equal(await q.enqueueAll([item("conv-msg:1")], T0 + THIRTYONE_DAYS), 0);
});

test("crash simulation: a reload at any point sees a consistent store", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1"), item("conv-msg:2")], T0);
  await q.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: 1 });
  // "crash": reopen from disk without any shutdown hook
  const q2 = createQueueStore(path);
  await q2.load();
  assert.equal(q2.get("conv-msg:1").state, "processing");
  assert.equal(q2.get("conv-msg:2").state, "queued");
  assert.equal(q2.nonTerminalCount(), 2);
  // file is valid JSON at rest (atomic rename, never partial)
  JSON.parse(readFileSync(path, "utf8"));
});

test("nonTerminalCount counts queued+processing only", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1"), item("conv-msg:2"), item("conv-msg:3")], T0);
  await q.transition("conv-msg:1", { state: "skipped", reason: "no-mention" }, T0);
  await q.transition("conv-msg:2", { state: "processing", claimedAt: T0, attempts: 1 });
  assert.equal(q.nonTerminalCount(), 2);
});

test("selectClaimable: oldest first, skips busy peers, honors backoff and slots", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([
    item("conv-msg:1", { peerId: "conv:1", postedTs: 100 }),
    item("conv-msg:2", { peerId: "conv:1", postedTs: 50 }),
    item("conv-msg:3", { peerId: "conv:2", postedTs: 200, nextAttemptAt: T0 + 999_999 }),
    item("conv-msg:4", { peerId: "conv:3", postedTs: 300 }),
  ], T0);
  assert.equal(q.selectClaimable(T0, new Set(), 3).id, "conv-msg:2");       // oldest overall
  assert.equal(q.selectClaimable(T0, new Set(["conv:1"]), 3).id, "conv-msg:4"); // conv:1 busy, 3 backing off
  assert.equal(q.selectClaimable(T0 + 1_000_000, new Set(["conv:1", "conv:3"]), 3).id, "conv-msg:3"); // backoff elapsed
  assert.equal(q.selectClaimable(T0, new Set(), 0), null);                   // no slots
});
