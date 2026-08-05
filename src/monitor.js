// Poll-based monitor. Thin wiring layer over the durable ingestion pipeline:
//
//   producer.pollOnce()  → sweeps Twist unreads and ENQUEUES everything new (transport only)
//   consumer.tick()      → claims queued items, applies policy, runs the agent turn
//   queue.prune(now)     → tombstones long-finished items
//
// The old at-most-once path (cursor advanced before dispatch, fire-and-forget
// dispatch, no retry) lived here and lost messages six different ways. It is gone:
// the cursor is now only a refetch bound, and the queue file is the delivery record.
// This module owns nothing but the effects the consumer needs (network, config,
// SDK) — all decision logic lives in producer.js / consumer.js / routing.js.
import { createTwistClient, conversationParticipantCount } from "./twist-client.js";
import { resolveTwistAccount } from "./config.js";
import {
  classifyConversation,
  contentMentionsBot,
  routingPeer,
  buildTranscript,
  resolveOutboundTarget,
} from "./routing.js";
import { admissionVerdict, handleTwistInbound } from "./inbound.js";
import { postToTwist } from "./outbound.js";
import { createQueueStore } from "./queue.js";
import { createProducer } from "./producer.js";
import { createConsumer } from "./consumer.js";
import { getTwistRuntime } from "./runtime.js";

const ITEM_FETCH_LIMIT = 30;
// On first sight of a thread/conv, items posted within this window before boot still
// count as "fresh" (answerable) — covers an @mention sent while the bot was down for a
// deploy/restart/short outage. Older items are enqueued but marked firstSightBacklog and
// skipped by the consumer. Sized to span a realistic outage (not just a deploy): a
// 10-minute window silently swallowed thread mentions that landed during longer downtime.
const FIRST_SIGHT_GRACE_MS = 2 * 60 * 60 * 1000;
const TERMINAL_STATES = new Set(["done", "skipped", "failed"]);

/**
 * @param {object} p
 * @param {object} p.cursors  loaded cursor store (refetch bound only)
 * @param {string} p.queuePath  path to the durable queue file (sibling of the cursors file)
 * @returns {Promise<{stop: () => void}>}
 */
export async function monitorTwistProvider({ accountId, config, runtime, abortSignal, statusSink, cursors, queuePath }) {
  const core = getTwistRuntime();
  const cfg = config ?? core.config.current();
  const account = resolveTwistAccount(cfg);
  if (!account.configured) {
    throw new Error("twist: not configured (need token, workspaceId, botUserId in channels.twist)");
  }
  if (!queuePath) throw new Error("twist: queuePath is required (durable ingestion queue)");
  const client = createTwistClient({ token: account.token, workspaceId: account.workspaceId });
  const botUserId = account.botUserId;
  const log = (m) => runtime.log?.(`[twist] ${m}`);
  // Anything posted at/after this (seconds) is "fresh" on first sight; older is backlog.
  const freshSinceTs = Math.floor((Date.now() - FIRST_SIGHT_GRACE_MS) / 1000);

  let stopped = false;
  let timer = null;
  let cycle = 0;

  const queue = createQueueStore(queuePath);
  await queue.load();

  const producer = createProducer({ client, queue, cursors, botUserId, freshSinceTs, now: Date.now, log });

  // ---------------------------------------------------------------- helpers

  // Conversation kind never changes for a given conversation, so this cache is
  // process-lived (not per-cycle): classifyPeer is called per item, and refetching
  // the conversation for every queued message would multiply poll cost.
  // Only SUCCESSFUL lookups are cached — a transient failure must not poison the
  // classification forever — and a failed lookup THROWS so the consumer requeues the
  // item with backoff instead of guessing a kind and mis-routing (a wrong "dm" would
  // bypass the group mention requirement; a wrong "groupdm" would drop a real DM).
  const peerKindCache = new Map();
  async function participantKind(convId) {
    const key = String(convId);
    if (peerKindCache.has(key)) return peerKindCache.get(key);
    let conv;
    try {
      conv = await client.getConversation(convId, abortSignal);
    } catch (err) {
      log(`participant lookup failed for conv ${convId}: ${String(err)}`);
      throw err;
    }
    const kind = classifyConversation(conversationParticipantCount(conv));
    peerKindCache.set(key, kind);
    return kind;
  }

  // Fetch thread title + channel name + prior-comment transcript for agent context.
  async function fetchThreadContext(threadId, channelId, comments, triggerId) {
    let threadTitle;
    let channelName;
    try {
      threadTitle = (await client.getThread(threadId, abortSignal))?.title;
    } catch (err) {
      log(`thread meta failed ${threadId}: ${String(err)}`);
    }
    try {
      if (channelId != null) channelName = (await client.getChannel(channelId, abortSignal))?.name;
    } catch (err) {
      log(`channel meta failed ${channelId}: ${String(err)}`);
    }
    return { threadTitle, channelName, transcript: buildTranscript(comments, triggerId) };
  }

  // A queued sibling still awaiting (or mid-) delivery. Used to reproduce the old DM
  // behaviour: a 1:1 DM's transcript carries the OTHER messages from this batch (so a
  // link sent just before a question still reaches the agent) but NOT messages the
  // session already saw — those are terminal in the queue.
  const isPendingSibling = (id) => {
    const q = queue.get(id);
    return Boolean(q) && !TERMINAL_STATES.has(q.state);
  };

  async function buildContext(item, kind) {
    if (item.kind === "conv") {
      const messages = (await client.getConversationMessages(item.conversationId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
      // Group DMs are not per-peer sessions, so they get the full recent transcript.
      const source = kind === "groupdm" ? messages : messages.filter((m) => isPendingSibling(`conv-msg:${m.id}`));
      return { transcript: buildTranscript(source, item.messageId) };
    }
    const comments = (await client.getThreadComments(item.threadId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
    return await fetchThreadContext(item.threadId, item.channelId, comments, item.messageId);
  }

  /**
   * Queue item → the normalized message `handleTwistInbound` documents.
   * `kind` is "thread" for both thread comments and the thread's opening post.
   * NOTE: `message.peerId` is the SESSION routing key from routingPeer (dm:<id> /
   * conv:<id> / thread:<id>) — deliberately not `item.peerId`, which is the queue's
   * coarser per-peer concurrency key.
   */
  async function toNormalizedMessage(item, { withContext = false } = {}) {
    const kind = item.kind === "conv" ? await participantKind(item.conversationId) : "thread";
    const peer = routingPeer({ kind, conversationId: item.conversationId, threadId: item.threadId });
    const context = withContext ? await buildContext(item, kind) : {};
    return {
      messageId: String(item.messageId),
      kind,
      conversationId: item.conversationId,
      threadId: item.threadId,
      groupId: item.channelId,
      peerKind: peer.peerKind,
      peerId: peer.peerId,
      isGroup: peer.isGroup,
      senderId: item.senderId,
      senderName: item.senderName,
      text: item.content,
      timestamp: item.postedTs ? item.postedTs * 1000 : Date.now(),
      directMention: contentMentionsBot(item.content, botUserId),
      // Twist context for the agent:
      threadTitle: context.threadTitle,
      channelName: context.channelName,
      transcript: context.transcript,
    };
  }

  // ---------------------------------------------------- injected consumer effects

  const classifyPeer = (item) => participantKind(item.conversationId);

  const admission = async (item) =>
    await admissionVerdict({
      message: await toNormalizedMessage(item),
      account,
      cfg: core.config.current() ?? cfg,
    });

  const runTurn = async (item, { commandAuthorized }) => {
    const message = await toNormalizedMessage(item, { withContext: true });
    log(`dispatching ${message.kind} ${message.peerId} from ${message.senderName}`);
    await handleTwistInbound({
      message,
      account,
      cfg: core.config.current() ?? cfg,
      runtime,
      client,
      statusSink,
      verdict: { admit: true, admission: "dispatch", commandAuthorized },
    });
  };

  // Boot recovery: did the bot already answer this item before the crash? True when a
  // post of ours landed at/after the claim. Twist timestamps are seconds.
  const probe = async (item) => {
    const since = (item.claimedAt ?? 0) / 1000;
    const posts =
      (item.kind === "conv"
        ? await client.getConversationMessages(item.conversationId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })
        : await client.getThreadComments(item.threadId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
    return posts.some((p) => String(p.creator) === String(botUserId) && (p.posted_ts ?? 0) >= since);
  };

  // Twist's reactions API only addresses a thread COMMENT or a conversation MESSAGE
  // (see twist-client.addReaction). A "thread-post" item is the thread's opening post,
  // which is neither — reacting to it would mean sending the thread id as a comment id
  // and hitting the wrong (or a nonexistent) object, so we no-op instead, once loudly.
  let threadPostReactionLogged = false;
  function reactionTarget(item) {
    if (item.kind === "conv") return { messageId: Number(item.messageId) };
    if (item.kind === "thread") return { commentId: Number(item.messageId) };
    return null;
  }
  // Best-effort: a reaction is cosmetic and must never fail (and thus retry) a turn.
  async function react(item, verb, reaction) {
    const target = reactionTarget(item);
    if (!target) {
      if (!threadPostReactionLogged) {
        threadPostReactionLogged = true;
        log("thread-post items carry no reactable target (Twist reacts to comments/messages only) — skipping reactions for them");
      }
      return;
    }
    try {
      if (verb === "add") await client.addReaction({ ...target, reaction });
      else await client.removeReaction({ ...target, reaction });
    } catch (err) {
      log(`reaction ${verb} ${reaction} failed for ${item.id}: ${String(err)}`);
    }
  }

  const alert = async (text) => {
    try {
      const { kind, id } = resolveOutboundTarget(null, account.defaultTo);
      await postToTwist({ client, kind, id, text });
    } catch (err) {
      log(`alert delivery failed: ${String(err)}`);
    }
  };

  const replyInPlace = (item, text) =>
    postToTwist({
      client,
      kind: item.kind === "conv" ? "conv" : "thread",
      id: item.kind === "conv" ? item.conversationId : item.threadId,
      text,
    });

  const consumer = createConsumer({
    queue, botUserId, now: Date.now, log,
    classifyPeer, admission, runTurn, probe, react, alert, replyInPlace,
  });

  // ------------------------------------------------------------------ poll loop

  async function pollOnce() {
    cycle++;
    await producer.pollOnce();
    if (stopped) return;
    await consumer.tick();
    await queue.prune(Date.now());
    if (core.logging?.shouldLogVerbose?.()) {
      log(`poll #${cycle}: queue depth ${queue.nonTerminalCount()}, ${consumer.inFlightCount()} turn(s) in flight`);
    }
  }

  async function loop() {
    if (stopped) return;
    try {
      await pollOnce();
    } catch (err) {
      log(`poll cycle failed: ${String(err)}`);
    }
    if (stopped) return;
    timer = setTimeout(loop, account.pollIntervalMs);
  }

  abortSignal?.addEventListener?.("abort", () => {
    stopped = true;
    if (timer) clearTimeout(timer);
  });

  // Boot-only: items left "processing" by a crashed process are probed and either
  // marked done (the reply landed) or requeued. MUST complete before the first tick().
  await consumer.recoverOrphans();

  log(`polling workspace ${account.workspaceId} every ${account.pollIntervalMs}ms (bot ${botUserId})`);
  void loop();

  return {
    stop: () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    },
  };
}
