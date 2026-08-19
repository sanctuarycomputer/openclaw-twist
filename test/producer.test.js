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
    getUnreadConversations: async () => ((state.unreadCalls ??= []).push("convs"), state.convs ?? []),
    getUnreadThreads: async () => ((state.unreadCalls ??= []).push("threads"), state.threads ?? []),
    getConversationMessages: async (id, opts) => page(id, state.convMsgs?.[id] ?? [], opts),
    getThreadComments: async (id, opts) => page(id, state.threadComments?.[id] ?? [], opts),
    getThread: async (id) => {
      (state.getThreadCalls ??= []).push(id);
      if (state.getThreadThrows) throw new Error("threads/getone exploded");
      return state.threadObjs?.[id];
    },
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

// ---------------------------------------------------------------- sweepContainer
//
// The targeted entry point webhook hints funnel into. Its whole job is to reuse the poll
// path's per-container internals verbatim, so these tests mostly assert SAMENESS: the
// cursor, dedup, fast-ack and mark-read behaviour must be indistinguishable from pollOnce.

test("sweepContainer(conversation) sweeps a TRACKED conversation without touching the unread lists", async () => {
  const state = {
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [{ id: 501, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 100, content: "hey bot" }] },
  };
  const { queue, cursors, producer } = await build(state);
  await cursors.setCursor("conversations", 9, 0); // the poll already tracks this conversation

  const outcome = await producer.sweepContainer({ kind: "conversation", id: 9 });

  assert.deepEqual(outcome, { swept: true });
  assert.equal(queue.has("conv-msg:501"), true);
  assert.equal(queue.get("conv-msg:501").peerId, "conv:9");
  assert.equal(cursors.getCursor("conversations", 9), 1);
  assert.equal(state.unreadCalls, undefined, "a targeted sweep must not fetch the unread lists");
});

// Conversations are gated for a different reason than threads: pollOnce only ever sweeps
// conversations off the UNREAD list, so an ungated hint would let whoever holds the URL
// token name ANY conversation id and have the bot fetch it — an authenticated enumeration
// primitive. Gating on "already tracked" removes it.
test("sweepContainer(conversation) does NOT sweep an untracked conversation — no enumeration primitive", async () => {
  const state = {
    convMsgs: { 9: msgRun(1, 5) },
  };
  const { queue, cursors, producer } = await build(state);
  assert.equal(cursors.isFirstSight("conversations", 9), true, "precondition: never tracked");

  const outcome = await producer.sweepContainer({ kind: "conversation", id: 9 });

  assert.deepEqual(outcome, { swept: false, reason: "untracked-conversation" });
  assert.equal(queue.itemsInState("queued").length, 0, "nothing durable was created");
  assert.equal(state.fetches, undefined, "the conversation was never fetched — nothing is learned about it");
  assert.equal(cursors.isFirstSight("conversations", 9), true, "and it is still untracked");
});

test("sweepContainer gate is per-container: a tracked conversation sweeps while an untracked one degrades", async () => {
  const state = { convMsgs: { 9: msgRun(1, 2), 10: msgRun(1, 2) } };
  const { queue, cursors, producer } = await build(state);
  await cursors.setCursor("conversations", 9, 0);

  assert.deepEqual(await producer.sweepContainer({ kind: "conversation", id: 9 }), { swept: true });
  assert.deepEqual(await producer.sweepContainer({ kind: "conversation", id: 10 }), {
    swept: false,
    reason: "untracked-conversation",
  });
  assert.deepEqual(
    queue.itemsInState("queued").map((i) => i.conversationId),
    [9, 9],
  );
});

// The gate that keeps the webhook purely ACCELERATIVE. pollOnce filters unread threads to
// direct_mention before sweeping, so a thread the bot was never mentioned in is never
// first-sighted. A hint names whatever container the upstream integration is installed on,
// so without this gate a workspace-wide integration would first-sight every chattered-in
// thread and turn all of it into durable queue rows (with 30-day tombstones).
test("sweepContainer(thread) does NOT sweep an untracked thread — it asks for a full poll instead", async () => {
  const state = {
    threadObjs: { 7: { id: 7, channel_id: 3, content: "opening", creator: 427360, posted_ts: FRESH_TS + 50 } },
    threadComments: { 7: msgRun(1, 5) },
  };
  const { queue, cursors, producer } = await build(state);
  assert.equal(cursors.isFirstSight("threads", 7), true, "precondition: never tracked");

  const outcome = await producer.sweepContainer({ kind: "thread", id: 7 });

  assert.deepEqual(outcome, { swept: false, reason: "untracked-thread" });
  assert.equal(queue.itemsInState("queued").length, 0, "nothing durable was created");
  assert.equal(queue.has("thread-post:7"), false);
  assert.equal(state.fetches, undefined, "no comments were even fetched");
  assert.equal(state.getThreadCalls, undefined, "no metadata call either");
  assert.equal(cursors.isFirstSight("threads", 7), true, "and the thread is still untracked");
});

test("sweepContainer(thread) sweeps a TRACKED thread (a cursor exists), resolving channel_id from the API", async () => {
  const state = {
    threadObjs: { 7: { id: 7, channel_id: 3, title: "Budget?" } },
    threadComments: { 7: [{ id: 88, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 60, content: "ping" }] },
  };
  const { queue, cursors, producer } = await build(state);
  await cursors.setCursor("threads", 7, 0); // the poll already tracks this thread

  const outcome = await producer.sweepContainer({ kind: "thread", id: 7 });

  assert.deepEqual(outcome, { swept: true });
  assert.equal(queue.get("thread-comment:88").channelId, 3);
  assert.ok(state.getThreadCalls.includes(7), "channel_id came from an authenticated threads/getone");
  assert.deepEqual(state.marked, [[7, 1]], "mark-read still runs");
});

test("sweepContainer(thread) gate is per-thread: a tracked thread sweeps while an untracked one degrades", async () => {
  const state = {
    threadObjs: { 7: { id: 7, channel_id: 3 }, 8: { id: 8, channel_id: 3 } },
    threadComments: { 7: msgRun(1, 2), 8: msgRun(1, 2) },
  };
  const { queue, cursors, producer } = await build(state);
  await cursors.setCursor("threads", 7, 0);

  assert.deepEqual(await producer.sweepContainer({ kind: "thread", id: 7 }), { swept: true });
  assert.deepEqual(await producer.sweepContainer({ kind: "thread", id: 8 }), {
    swept: false,
    reason: "untracked-thread",
  });
  assert.deepEqual(
    queue.itemsInState("queued").map((i) => i.threadId),
    [7, 7],
  );
});

// Hints arrive as strings; the poll path treats Twist ids as numbers (markThreadRead POSTs
// them). The coercion happens once, at this boundary.
test("sweepContainer coerces a string id to a number at the boundary", async () => {
  const state = {
    convMsgs: { 9: [{ id: 501, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 100, content: "hey" }] },
    threadObjs: { 7: { id: 7, channel_id: 3 } },
    threadComments: { 7: [{ id: 88, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 60, content: "ping" }] },
  };
  const { queue, cursors, producer } = await build(state);
  await cursors.setCursor("conversations", 9, 0);
  await cursors.setCursor("threads", 7, 0);

  await producer.sweepContainer({ kind: "conversation", id: "9" });
  await producer.sweepContainer({ kind: "thread", id: "7" });

  assert.equal(queue.get("conv-msg:501").conversationId, 9, "a number, not the string \"9\"");
  assert.equal(queue.get("thread-comment:88").threadId, 7);
  assert.deepEqual(state.marked, [[7, 1]], "markThreadRead gets a numeric id like the poll path");
});

test("sweepContainer(thread) pages forward from an existing cursor, same as the poll path", async () => {
  const state = {
    threadObjs: { 7: { id: 7, channel_id: 3 } },
    threadComments: { 7: msgRun(1, 45) },
  };
  const { queue, cursors, producer } = await build(state);
  await cursors.setCursor("threads", 7, 30); // not first sight

  await producer.sweepContainer({ kind: "thread", id: 7 });

  assert.equal(queue.itemsInState("queued").length, 15); // 31..45
  assert.equal(cursors.getCursor("threads", 7), 45);
  assert.equal(queue.has("thread-post:7"), false, "no opening post outside first sight");
});

test("sweepContainer(thread) survives a failed metadata fetch: comments still sweep, channelId is left unset", async () => {
  const logs = [];
  const state = {
    getThreadThrows: true,
    threadComments: { 7: msgRun(1, 2) },
  };
  const { queue, cursors, producer } = await build(state, { logs });
  await cursors.setCursor("threads", 7, 0);

  await producer.sweepContainer({ kind: "thread", id: 7 });

  assert.equal(queue.itemsInState("queued").length, 2);
  assert.equal(queue.itemsInState("queued")[0].channelId, undefined);
  assert.match(logs.join("\n"), /metadata fetch failed/);
});

test("sweepContainer then pollOnce does not double-enqueue or double-ack", async () => {
  const acked = [];
  const state = {
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: msgRun(1, 3) },
  };
  const { queue, cursors, producer } = await build(state, { fastAck: async (item) => { acked.push(item.id); } });
  await cursors.setCursor("conversations", 9, 0);

  await producer.sweepContainer({ kind: "conversation", id: 9 });
  await producer.pollOnce();

  assert.equal(queue.itemsInState("queued").length, 3);
  assert.deepEqual(acked, ["conv-msg:1001", "conv-msg:1002", "conv-msg:1003"]);
});

test("sweepContainer fast-acks newly enqueued items exactly like a poll sweep", async () => {
  const acked = [];
  const state = { convMsgs: { 9: msgRun(1, 2) } };
  const { cursors, producer } = await build(state, { fastAck: async (item) => { acked.push(item.id); } });
  await cursors.setCursor("conversations", 9, 0);
  await producer.sweepContainer({ kind: "conversation", id: 9 });
  assert.deepEqual(acked, ["conv-msg:1001", "conv-msg:1002"]);
});

test("sweepContainer rejects a container it cannot address instead of sweeping something arbitrary", async () => {
  const { producer } = await build({});
  await assert.rejects(() => producer.sweepContainer({ kind: "channel", id: 1 }), /unknown container kind/);
  await assert.rejects(() => producer.sweepContainer({ kind: "thread" }), /id is required/);
  await assert.rejects(() => producer.sweepContainer({ kind: "thread", id: "" }), /id is required/);
  await assert.rejects(() => producer.sweepContainer(), /id is required/);
  // A non-numeric id can only come from a bug or a forged hint that slipped the extractor;
  // it must never reach an API query.
  for (const bad of ["abc", "../../x", "1 OR 1=1", "1.5", "-3", "0", "9".repeat(30), Number.NaN]) {
    await assert.rejects(
      () => producer.sweepContainer({ kind: "thread", id: bad }),
      /must be a positive integer id/,
      `expected rejection for ${String(bad)}`,
    );
  }
});
