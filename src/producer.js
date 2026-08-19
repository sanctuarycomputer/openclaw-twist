// Transport-only sweep: everything new gets enqueued by id; no policy here.
// Cursor = refetch bound only. Invariant: cursor advances only after the
// enqueue batch is durably persisted, so nothing below a cursor is un-enqueued.
import { newInboundItems, advanceCursor } from "./routing.js";

const ITEM_FETCH_LIMIT = 30;
// Bound on how much history one poll will walk per container. A truncated poll is NOT
// lossy: the cursor only ever advances over batches we actually fetched and enqueued, so
// the next poll resumes exactly where this one stopped.
export const MAX_PAGES_PER_POLL = 10;

/**
 * @param {object} deps
 * @param {(item: object) => Promise<void>} [deps.fastAck]
 *   Best-effort "we've seen you" side effect, fired once per NEWLY-enqueued, non-backlog
 *   item right after its batch is durably persisted. The ⏳ a human waits for used to be
 *   added at CLAIM time, so a mention sitting behind N unprocessed items in the same
 *   thread got no visible acknowledgement for N consumer ticks. Acking at ingestion makes
 *   that latency independent of queue depth. WHICH items actually get a reaction is not
 *   decided here (this layer stays transport-only) — the injected implementation applies
 *   the mention/DM rules. Defaults to a no-op.
 */
export function createProducer({ client, queue, cursors, botUserId, freshSinceTs, now, log, abortSignal, fastAck = async () => {} }) {
  const isBacklog = (firstSight, postedTs) => firstSight && !(typeof postedTs === "number" && postedTs >= freshSinceTs);

  /**
   * Persist a batch, THEN ack it. Durability first, always: an ack is cosmetic, but a ⏳ on
   * a message that never made it into the queue is a promise we can't keep.
   *
   * "Newly enqueued" is knowable without changing enqueueAll's `added` count return (other
   * callers, and queue.test.js, rely on it): `queue.has` is exactly enqueueAll's dedup
   * predicate (live items OR tombstones), and nothing awaits between this filter and the
   * enqueue, so the filtered list IS the set enqueueAll adds. A re-seen item is filtered out
   * here and therefore never re-acked.
   *
   * Acks are awaited in order rather than fired off in parallel — a first-sight DM sweep can
   * carry a whole fetch window, and a burst of reaction calls is exactly what a rate limiter
   * punishes. Each one is contained: a rejection is logged and the sweep continues. Because
   * the loop is serial it also checks the abort signal between acks: on shutdown the items
   * are already durable, so the remaining (purely cosmetic) reactions are worth abandoning
   * rather than holding the process open for.
   */
  async function enqueueAcked(items) {
    const fresh = items.filter((it) => !queue.has(it.id));
    await queue.enqueueAll(fresh, now());
    for (const item of fresh) {
      if (abortSignal?.aborted) break;
      if (item.firstSightBacklog) continue; // never answered → never acknowledged
      try {
        await fastAck(item);
      } catch (err) {
        log(`fast-ack failed for ${item.id}: ${String(err)}`);
      }
    }
  }

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
   * Ordering per page: enqueueAcked(page N) → setCursor(max of page N) → page N+1. A crash
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
      await enqueueAcked(toItems(batch, cur));
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
      await enqueueAcked(toItems(messages, -1));
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
      await enqueueAcked(items);
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

  /**
   * Build the thread descriptor sweepThread expects from nothing but an id.
   *
   * `channel_id` matters: it becomes the queued item's channelId and therefore the
   * message's groupId, which is what `resolveRequireMention` keys per-channel policy off.
   * The poll path gets it from `threads/get_unread`; here it comes from an authenticated
   * `threads/getone`. It deliberately does NOT come from the webhook payload — that body is
   * unsigned, and letting a forged `channel_id` through would let an attacker pick which
   * channel's mention policy applies. A failed lookup degrades to the wildcard policy
   * (requireMention defaults to true), never to a more permissive one.
   */
  async function threadContainer(threadId) {
    try {
      const thread = await client.getThread(threadId, abortSignal);
      return { thread_id: threadId, channel_id: thread?.channel_id };
    } catch (err) {
      log(`sweepContainer: thread ${threadId} metadata fetch failed: ${String(err)}`);
      return { thread_id: threadId };
    }
  }

  return {
    /**
     * Sweep ONE container, using exactly the same machinery pollOnce uses for it — same
     * cursor, same dedup, same fast-ack, same mark-read. This is the targeted entry point
     * webhook hints funnel into: the hint says *where* to look, this re-fetches *what* is
     * actually there from the Twist API with our own token.
     *
     * It is not a shortcut around the queue: a container swept here is indistinguishable
     * afterwards from one swept by the poll loop, so a webhook that never arrives costs
     * nothing but latency.
     *
     * THE UNTRACKED-THREAD GATE. A targeted sweep must only ever ACCELERATE work the poll
     * would do anyway — it must never widen what gets ingested. For threads the two paths
     * are not symmetric: pollOnce filters unread threads to `direct_mention` before
     * sweeping, so a thread the bot was never mentioned in is never first-sighted. A hint,
     * by contrast, names whatever container the upstream integration is installed on. So a
     * hint for a thread with NO cursor (never tracked) would first-sight it and pull its
     * newest fetch window into the durable queue — items the poll path would never have
     * touched. The consumer would skip them terminally and correctly, but they are durable
     * rows with 30-day tombstones, so a workspace-wide integration would turn all channel
     * chatter into permanent queue growth.
     *
     * Untracked threads therefore do NOT get a targeted sweep. The hint degrades to
     * "run a full poll", which applies the `direct_mention` filter exactly as today: a
     * brand-new thread that DOES mention the bot is still picked up promptly (the hint
     * triggered the poll), and chatter-only threads cost nothing durable. The enhancement
     * stays purely accelerative.
     *
     * Conversations need no such gate: pollOnce sweeps every unread conversation without a
     * mention filter, so hint-sweeping an unknown conversation matches poll semantics
     * exactly — including the first-sight backlog rules.
     *
     * @param {{kind: "thread"|"conversation", id: string|number}} hint
     * @returns {Promise<{swept: boolean, reason?: "untracked-thread"}>} `swept:false` means
     *   the caller should schedule a full poll instead; it is not an error.
     */
    async sweepContainer({ kind, id } = {}) {
      if (id === undefined || id === null || id === "") throw new Error("sweepContainer: id is required");
      if (kind === "conversation") {
        await sweepConversation({ conversation_id: id });
        return { swept: true };
      }
      if (kind === "thread") {
        if (cursors.isFirstSight("threads", id)) return { swept: false, reason: "untracked-thread" };
        await sweepThread(await threadContainer(id));
        return { swept: true };
      }
      throw new Error(`sweepContainer: unknown container kind ${JSON.stringify(kind)}`);
    },

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
