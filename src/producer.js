// Transport-only sweep: everything new gets enqueued by id; no policy here.
// Cursor = refetch bound only. Invariant: cursor advances only after the
// enqueue batch is durably persisted, so nothing below a cursor is un-enqueued.
import { newInboundItems, advanceCursor } from "./routing.js";

const ITEM_FETCH_LIMIT = 30;

export function createProducer({ client, queue, cursors, botUserId, freshSinceTs, now, log }) {
  const isBacklog = (firstSight, postedTs) => firstSight && !(typeof postedTs === "number" && postedTs >= freshSinceTs);

  function toItem({ raw, kind, peerId, conversationId, threadId, channelId, firstSight }) {
    return {
      id: kind === "conv" ? `conv-msg:${raw.id}` : kind === "thread" ? `thread-comment:${raw.id}` : `thread-post:${threadId}`,
      kind, peerId, conversationId, threadId, channelId,
      messageId: raw.id, objIndex: raw.obj_index ?? 0,
      senderId: raw.creator, senderName: raw.creator_name ?? String(raw.creator),
      content: raw.content ?? "", postedTs: raw.posted_ts ?? 0,
      firstSightBacklog: isBacklog(firstSight, raw.posted_ts),
      state: "queued", attempts: 0, nextAttemptAt: 0, enqueuedAt: now(),
    };
  }

  async function sweepConversation(c) {
    const convId = c.conversation_id;
    const firstSight = cursors.isFirstSight("conversations", convId);
    const cursor = firstSight ? -1 : cursors.getCursor("conversations", convId);
    const messages = await client.getConversationMessages(convId, { limit: ITEM_FETCH_LIMIT });
    const fresh = newInboundItems(messages, cursor, botUserId)
      .filter((m) => !queue.has(`conv-msg:${m.id}`))
      .map((raw) => toItem({ raw, kind: "conv", peerId: `conv:${convId}`, conversationId: convId, firstSight }));
    await queue.enqueueAll(fresh, now());
    await cursors.setCursor("conversations", convId, advanceCursor(cursor, messages));
  }

  async function sweepThread(t) {
    const threadId = t.thread_id;
    const firstSight = cursors.isFirstSight("threads", threadId);
    const cursor = firstSight ? -1 : cursors.getCursor("threads", threadId);
    const comments = await client.getThreadComments(threadId, { limit: ITEM_FETCH_LIMIT });
    const items = newInboundItems(comments, cursor, botUserId)
      .filter((cm) => !queue.has(`thread-comment:${cm.id}`))
      .map((raw) => toItem({ raw, kind: "thread", peerId: `thread:${threadId}`, threadId, channelId: t.channel_id, firstSight }));
    if (firstSight && !queue.has(`thread-post:${threadId}`)) {
      try {
        const post = await client.getThread(threadId);
        if (post && String(post.creator) !== String(botUserId)) {
          items.push(toItem({ raw: { ...post, id: post.id, obj_index: 0 }, kind: "thread-post", peerId: `thread:${threadId}`, threadId, channelId: t.channel_id, firstSight }));
        }
      } catch (err) {
        log(`thread post fetch failed ${threadId}: ${String(err)}`);
      }
    }
    await queue.enqueueAll(items, now());
    const nextCursor = advanceCursor(cursor, comments);
    await cursors.setCursor("threads", threadId, nextCursor);
    if (Number.isFinite(nextCursor)) {
      try { await client.markThreadRead(threadId, nextCursor); } catch (err) { log(`markThreadRead ${threadId} failed: ${String(err)}`); }
    }
  }

  return {
    async pollOnce() {
      const convs = await client.getUnreadConversations();
      for (const c of convs) {
        try { await sweepConversation(c); } catch (err) { log(`conv sweep ${c.conversation_id} failed: ${String(err)}`); }
      }
      const threads = (await client.getUnreadThreads()).filter((t) => t.direct_mention);
      for (const t of threads) {
        try { await sweepThread(t); } catch (err) { log(`thread sweep ${t.thread_id} failed: ${String(err)}`); }
      }
    },
  };
}
