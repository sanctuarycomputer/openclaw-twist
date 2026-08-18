import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createCursorStore } from "../src/state.js";
import { createProducer, MAX_PAGES_PER_POLL } from "../src/producer.js";

const BOT = 634870;
const NOW_MS = 1_785_900_000_000;
const FRESH_TS = 1_785_890_000; // seconds; poller "boot minus grace"

// Mirrors the real API's ordering contract: a bare {limit} call returns the NEWEST `limit`
// items (Twist defaults to order_by=desc); passing fromObjIndex returns the OLDEST `limit`
// items at/above that index (order_by=asc + from_obj_index).
function fakeClient(state) {
  const page = (id, all, opts = {}) => {
    const limit = opts.limit ?? 30;
    (state.fetches ??= []).push({ id, limit, fromObjIndex: opts.fromObjIndex ?? null });
    if (opts.fromObjIndex == null) return all.slice(-limit);
    return all.filter((it) => (it.obj_index ?? 0) >= opts.fromObjIndex).slice(0, limit);
  };
  return {
    getUnreadConversations: async () => state.convs ?? [],
    getUnreadThreads: async () => state.threads ?? [],
    getConversationMessages: async (id, opts) => page(id, state.convMsgs?.[id] ?? [], opts),
    getThreadComments: async (id, opts) => page(id, state.threadComments?.[id] ?? [], opts),
    getThread: async (id) => state.threadObjs?.[id],
    markThreadRead: async (id, idx) => { (state.marked ??= []).push([id, idx]); },
  };
}
async function build(state, { logs, fastAck } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistp-"));
  const queue = createQueueStore(join(dir, "queue.json"));
  const cursors = createCursorStore(join(dir, "cursors.json"));
  await queue.load(); await cursors.load();
  const producer = createProducer({ client: fakeClient(state), queue, cursors, botUserId: BOT, freshSinceTs: FRESH_TS, now: () => NOW_MS, log: (m) => logs?.push(m), fastAck });
  return { queue, cursors, producer };
}
const MENTION = `[Bot](twist-mention://${BOT}) ping`;
// Ascending run of messages, all fresh and all from a human.
const msgRun = (from, to) =>
  Array.from({ length: to - from + 1 }, (_, i) => ({
    id: 1000 + from + i, obj_index: from + i, creator: 427360, posted_ts: FRESH_TS + from + i, content: `m${from + i}`,
  }));

test("enqueues fresh conversation messages incl. obj_index 0; excludes self; advances cursor", async () => {
  const { queue, cursors, producer } = await build({
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [
      { id: 501, obj_index: 0, creator: 427360, posted_ts: FRESH_TS + 100, content: "hey bot" },
      { id: 502, obj_index: 1, creator: BOT, posted_ts: FRESH_TS + 101, content: "self" },
    ] },
  });
  await producer.pollOnce();
  assert.equal(queue.has("conv-msg:501"), true);
  assert.equal(queue.has("conv-msg:502"), false);
  assert.equal(queue.get("conv-msg:501").firstSightBacklog, false);
  assert.equal(cursors.getCursor("conversations", 9), 1); // advanced past self post too
});

test("first-sight backlog items are enqueued flagged, not dropped", async () => {
  const { queue, producer } = await build({
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [{ id: 501, obj_index: 0, creator: 427360, posted_ts: FRESH_TS - 5000, content: "old" }] },
  });
  await producer.pollOnce();
  assert.equal(queue.get("conv-msg:501").firstSightBacklog, true);
});

test("thread sweep enqueues comments AND synthesizes the opening post on first sight", async () => {
  const { queue, producer } = await build({
    threads: [{ thread_id: 7, channel_id: 3, direct_mention: true }],
    threadObjs: { 7: { id: 7, title: "Budget?", content: "[Bot](twist-mention://634870) thoughts?", creator: 427360, posted_ts: FRESH_TS + 50 } },
    threadComments: { 7: [{ id: 88, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 60, content: "ping" }] },
  });
  await producer.pollOnce();
  assert.equal(queue.has("thread-post:7"), true);
  assert.equal(queue.has("thread-comment:88"), true);
  assert.equal(queue.get("thread-post:7").peerId, "thread:7");
});

test("second poll is a no-op (idempotent), cursor bounds refetch", async () => {
  const state = {
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [{ id: 501, obj_index: 0, creator: 427360, posted_ts: FRESH_TS + 100, content: "hey" }] },
  };
  const { queue, producer } = await build(state);
  await producer.pollOnce();
  await producer.pollOnce();
  assert.equal(queue.itemsInState("queued").length, 1);
});

// A burst larger than one fetch window used to be lost forever: the newest-N fetch never
// saw the older items, but the cursor jumped past them anyway. The producer now pages
// FORWARD from the cursor, so the cursor only crosses items it actually enqueued.
test("burst above the cursor is paged forward, not truncated to the newest window", async () => {
  const state = { convs: [{ conversation_id: 9 }], convMsgs: { 9: msgRun(1, 75) } };
  const { queue, cursors, producer } = await build(state);
  await cursors.setCursor("conversations", 9, 30); // not first sight: swept up to obj_index 30

  await producer.pollOnce();

  const enqueued = queue.itemsInState("queued");
  assert.equal(enqueued.length, 45); // 31..75 — all of them, not just the newest 30
  assert.deepEqual(enqueued.map((i) => i.objIndex).sort((a, b) => a - b), msgRun(31, 75).map((m) => m.obj_index));
  assert.equal(cursors.getCursor("conversations", 9), 75);
  // every fetch was an ascending page anchored at the live cursor + 1
  assert.deepEqual(state.fetches.map((f) => f.fromObjIndex), [31, 61]);
});

test("pagination stops at MAX_PAGES_PER_POLL and the next poll resumes exactly where it stopped", async () => {
  const total = 30 * MAX_PAGES_PER_POLL + 10; // one poll's worth of pages, plus a remainder
  const state = { convs: [{ conversation_id: 9 }], convMsgs: { 9: msgRun(1, total) } };
  const logs = [];
  const { queue, cursors, producer } = await build(state, { logs });
  await cursors.setCursor("conversations", 9, 0);

  await producer.pollOnce();
  assert.equal(queue.itemsInState("queued").length, 30 * MAX_PAGES_PER_POLL);
  assert.equal(cursors.getCursor("conversations", 9), 30 * MAX_PAGES_PER_POLL); // only over what was fetched
  assert.match(logs.join("\n"), /page cap/);

  await producer.pollOnce();
  assert.equal(queue.itemsInState("queued").length, total); // remainder picked up, nothing skipped
  assert.equal(cursors.getCursor("conversations", 9), total);
});

test("first sight still baselines on the newest window (backlog flags intact)", async () => {
  const old = msgRun(1, 20).map((m) => ({ ...m, posted_ts: FRESH_TS - 9000 })); // pre-boot backlog
  const state = { convs: [{ conversation_id: 9 }], convMsgs: { 9: [...old, ...msgRun(21, 40)] } };
  const { queue, cursors, producer } = await build(state);

  await producer.pollOnce();

  assert.deepEqual(state.fetches.map((f) => f.fromObjIndex), [null]); // desc window, no pagination
  assert.equal(queue.itemsInState("queued").length, 30);             // newest window only
  assert.equal(queue.has("conv-msg:1011"), true);                    // obj_index 11 (oldest of the window)
  assert.equal(queue.get("conv-msg:1011").firstSightBacklog, true);  // posted before the grace window
  assert.equal(queue.get("conv-msg:1040").firstSightBacklog, false); // fresh
  assert.equal(cursors.getCursor("conversations", 9), 40);
});

// The ⏳ used to be added when the CONSUMER claimed an item, which is queue-depth dependent:
// a mention behind 15 unprocessed comments waited 15 ticks for any sign of life. The ack is
// now fired at ingestion — but only ever AFTER the batch is durably persisted.
test("fast-ack fires once per newly-enqueued item, after it is persisted", async () => {
  const acked = [];
  const state = {
    threads: [{ thread_id: 7, channel_id: 3, direct_mention: true }],
    threadComments: { 7: [{ id: 88, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 60, content: MENTION }] },
  };
  const { producer, queue } = await build(state, { fastAck: async (item) => { acked.push([item.id, queue.has(item.id)]); } });
  await producer.pollOnce();
  assert.deepEqual(acked, [["thread-comment:88", true]]); // durable before acknowledged
});

test("fast-ack is never fired for first-sight backlog items", async () => {
  const acked = [];
  const { queue, producer } = await build({
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [
      { id: 501, obj_index: 0, creator: 427360, posted_ts: FRESH_TS - 5000, content: MENTION },
      { id: 502, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 10, content: MENTION },
    ] },
  }, { fastAck: async (item) => { acked.push(item.id); } });
  await producer.pollOnce();
  assert.equal(queue.get("conv-msg:501").firstSightBacklog, true);
  assert.deepEqual(acked, ["conv-msg:502"]); // the backlog mention is enqueued, never acknowledged
});

// The ack is cosmetic; ingestion is not. A reaction API blip must not cost a message.
test("a rejecting fast-ack is logged and never derails the sweep", async () => {
  const logs = [];
  const { queue, cursors, producer } = await build(
    { convs: [{ conversation_id: 9 }], convMsgs: { 9: msgRun(1, 3) } },
    { logs, fastAck: async (item) => { throw new Error(`reactions down for ${item.id}`); } },
  );
  await producer.pollOnce();
  assert.equal(queue.itemsInState("queued").length, 3);   // every item still enqueued
  assert.equal(cursors.getCursor("conversations", 9), 3); // sweep ran to completion
  assert.equal(logs.filter((m) => m.includes("fast-ack failed")).length, 3);
});

test("re-seen items are not re-acked (at most one ack per item, ever)", async () => {
  const acked = [];
  const state = { convs: [{ conversation_id: 9 }], convMsgs: { 9: msgRun(1, 2) } };
  const { cursors, producer } = await build(state, { fastAck: async (item) => { acked.push(item.id); } });
  await producer.pollOnce();
  assert.deepEqual(acked, ["conv-msg:1001", "conv-msg:1002"]);
  // Rewind the cursor so the SAME messages are refetched and re-offered to the queue: the
  // enqueue is a no-op (dedup by id) and so is the ack.
  await cursors.setCursor("conversations", 9, 0);
  await producer.pollOnce();
  assert.deepEqual(acked, ["conv-msg:1001", "conv-msg:1002"]);
});

test("first-sight thread with no comments does not mark as read", async () => {
  const state = {
    threads: [{ thread_id: 7, channel_id: 3, direct_mention: true }],
    threadObjs: { 7: { id: 7, title: "Empty thread", content: "No comments yet", creator: 427360, posted_ts: FRESH_TS + 50 } },
    threadComments: { 7: [] },
  };
  const { queue, producer } = await build(state);
  await producer.pollOnce();
  assert.equal(state.marked, undefined);
  assert.equal(queue.has("thread-post:7"), true);
});
