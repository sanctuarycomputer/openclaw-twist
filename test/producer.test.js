import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createCursorStore } from "../src/state.js";
import { createProducer } from "../src/producer.js";

const BOT = 634870;
const NOW_MS = 1_785_900_000_000;
const FRESH_TS = 1_785_890_000; // seconds; poller "boot minus grace"

function fakeClient(state) {
  return {
    getUnreadConversations: async () => state.convs ?? [],
    getUnreadThreads: async () => state.threads ?? [],
    getConversationMessages: async (id) => state.convMsgs?.[id] ?? [],
    getThreadComments: async (id) => state.threadComments?.[id] ?? [],
    getThread: async (id) => state.threadObjs?.[id],
    markThreadRead: async (id, idx) => { (state.marked ??= []).push([id, idx]); },
  };
}
async function build(state) {
  const dir = mkdtempSync(join(tmpdir(), "twistp-"));
  const queue = createQueueStore(join(dir, "queue.json"));
  const cursors = createCursorStore(join(dir, "cursors.json"));
  await queue.load(); await cursors.load();
  const producer = createProducer({ client: fakeClient(state), queue, cursors, botUserId: BOT, freshSinceTs: FRESH_TS, now: () => NOW_MS, log: () => {} });
  return { queue, cursors, producer };
}

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
