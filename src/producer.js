// Transport-only sweep: everything new gets enqueued by id; no policy here.
// Cursor = refetch bound only. Invariant: cursor advances only after the
// enqueue batch is durably persisted, so nothing below a cursor is un-enqueued.
import { newInboundItems, advanceCursor } from "./routing.js";

const ITEM_FETCH_LIMIT = 30;
// Bound on how much history one poll will walk per container. A truncated poll is NOT
// lossy: the cursor only ever advances over batches we actually fetched and enqueued, so
// the next poll resumes exactly where this one stopped.
export const MAX_PAGES_PER_POLL = 10;

export function createProducer({ client, queue, cursors, botUserId, freshSinceTs, now, log, abortSignal }) {
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

  /**
   * Walk a container FORWARD from its cursor, one ITEM_FETCH_LIMIT-sized ascending page at
   * a time, enqueueing each page before advancing the cursor over it.
   *
   * Why not a single newest-N fetch: Twist's list endpoints default to `order_by=desc`, so
   * `{limit: 30}` returns the NEWEST 30 items. If more than 30 landed between two polls,
   * the older ones were never fetched — yet advanceCursor jumped the cursor past them,
   * silently losing them forever (live-observed: a thread sitting at cursor 810 with latest
   * obj_index 1691). Paging ascending from `cursor + 1` means the cursor can only ever move
   * across items we actually saw.
   *
   * Ordering per page: enqueueAll(page N) → setCursor(max of page N) → page N+1. A crash
   * anywhere in the walk loses nothing; at worst the next boot refetches one page.
   *
   * @param {(fromObjIndex:number) => Promise<any[]>} fetchPage
   * @param {(raws:any[], cursor:number) => any[]} toItems
   * @returns {Promise<number>} the cursor after the walk
   */
  async function pageForward({ label, cursor, fetchPage, toItems }) {
    let cur = cursor;
    for (let page = 0; page < MAX_PAGES_PER_POLL; page++) {
      const batch = (await fetchPage(cur + 1)) ?? [];
      await queue.enqueueAll(toItems(batch, cur), now());
      const next = advanceCursor(cur, batch);
      // advanceCursor never goes backwards, so `stalled` means the page carried nothing
      // above the cursor — a full page like that would otherwise loop forever.
      const stalled = next <= cur;
      if (!stalled) {
        await cursors.setCursor(...label, next);
        cur = next;
      }
      if (stalled && batch.length === ITEM_FETCH_LIMIT) {
        // A FULL page carrying nothing above the cursor is the signature of the API having
        // ignored order_by=asc (we asked to page forward and got the newest window back).
        // Breaking here is the safe move — but it silently stops draining a real backlog,
        // so it must never be quiet.
        log(`sweep ${label.join(":")} pagination STALLED at cursor ${cur}: a full page of ${ITEM_FETCH_LIMIT} carried nothing above the cursor — ordering regression (order_by=asc ignored)?`);
      }
      if (batch.length < ITEM_FETCH_LIMIT || stalled) break; // short page = caught up
      if (page === MAX_PAGES_PER_POLL - 1) {
        log(`sweep ${label.join(":")} hit the ${MAX_PAGES_PER_POLL}-page cap at cursor ${cur} — resuming next poll`);
      }
    }
    return cur;
  }

  async function sweepConversation(c) {
    const convId = c.conversation_id;
    const firstSight = cursors.isFirstSight("conversations", convId);
    const toItems = (raws, cursor) =>
      newInboundItems(raws, cursor, botUserId)
        .filter((m) => !queue.has(`conv-msg:${m.id}`))
        .map((raw) => toItem({ raw, kind: "conv", peerId: `conv:${convId}`, conversationId: convId, firstSight }));

    if (firstSight) {
      // First sight IS the backlog baseline: take the newest window (desc) and start there.
      const messages = (await client.getConversationMessages(convId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
      await queue.enqueueAll(toItems(messages, -1), now());
      await cursors.setCursor("conversations", convId, advanceCursor(-1, messages));
      return;
    }
    await pageForward({
      label: ["conversations", convId],
      cursor: cursors.getCursor("conversations", convId),
      fetchPage: (from) => client.getConversationMessages(convId, { limit: ITEM_FETCH_LIMIT, fromObjIndex: from, signal: abortSignal }),
      toItems,
    });
  }

  async function sweepThread(t) {
    const threadId = t.thread_id;
    const firstSight = cursors.isFirstSight("threads", threadId);
    const toItems = (raws, cursor) =>
      newInboundItems(raws, cursor, botUserId)
        .filter((cm) => !queue.has(`thread-comment:${cm.id}`))
        .map((raw) => toItem({ raw, kind: "thread", peerId: `thread:${threadId}`, threadId, channelId: t.channel_id, firstSight }));

    let nextCursor;
    if (firstSight) {
      const comments = (await client.getThreadComments(threadId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
      const items = toItems(comments, -1);
      if (!queue.has(`thread-post:${threadId}`)) {
        try {
          const post = await client.getThread(threadId, abortSignal);
          if (post && String(post.creator) !== String(botUserId)) {
            items.push(toItem({ raw: { ...post, id: post.id, obj_index: 0 }, kind: "thread-post", peerId: `thread:${threadId}`, threadId, channelId: t.channel_id, firstSight }));
          }
        } catch (err) {
          log(`thread post fetch failed ${threadId}: ${String(err)}`);
        }
      }
      await queue.enqueueAll(items, now());
      nextCursor = advanceCursor(-1, comments);
      await cursors.setCursor("threads", threadId, nextCursor);
    } else {
      nextCursor = await pageForward({
        label: ["threads", threadId],
        cursor: cursors.getCursor("threads", threadId),
        fetchPage: (from) => client.getThreadComments(threadId, { limit: ITEM_FETCH_LIMIT, fromObjIndex: from, signal: abortSignal }),
        toItems,
      });
    }
    if (Number.isFinite(nextCursor) && nextCursor >= 0) {
      try { await client.markThreadRead(threadId, nextCursor, abortSignal); } catch (err) { log(`markThreadRead ${threadId} failed: ${String(err)}`); }
    }
  }

  return {
    async pollOnce() {
      const convs = await client.getUnreadConversations(abortSignal);
      for (const c of convs) {
        try { await sweepConversation(c); } catch (err) { log(`conv sweep ${c.conversation_id} failed: ${String(err)}`); }
      }
      const threads = (await client.getUnreadThreads(abortSignal)).filter((t) => t.direct_mention);
      for (const t of threads) {
        try { await sweepThread(t); } catch (err) { log(`thread sweep ${t.thread_id} failed: ${String(err)}`); }
      }
    },
  };
}
