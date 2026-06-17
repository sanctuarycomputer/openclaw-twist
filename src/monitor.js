// Poll-based monitor. Replaces IRC's socket monitor: every pollIntervalMs it
// scans Twist unread conversations (DM always, group DM on mention) and unread
// threads (mention-only), then dispatches new items through handleTwistInbound.
// Returns a { stop } handle for runStoppablePassiveMonitor.
//
// Two important properties:
//  - Baseline-on-first-sight uses the ACTUAL latest obj_index (fetched), not
//    get_unread's stale read-marker, so pre-existing backlog is never answered.
//  - Dispatch is NON-BLOCKING (fire-and-forget with a per-peer in-flight guard
//    and timeout) so one slow agent turn never starves the rest of the poll.
import { createTwistClient, conversationParticipantCount } from "./twist-client.js";
import { resolveTwistAccount } from "./config.js";
import {
  classifyConversation,
  shouldRespondToConversation,
  newInboundItems,
  advanceCursor,
  contentMentionsBot,
  routingPeer,
  buildTranscript,
} from "./routing.js";
import { handleTwistInbound } from "./inbound.js";
import { getTwistRuntime } from "./runtime.js";

const ITEM_FETCH_LIMIT = 30;
const DISPATCH_TIMEOUT_MS = 180000;
const REACTION_PROCESSING = "⏳"; // shown while a turn is in progress
const REACTION_DONE = "✅"; // shown when the turn completes successfully
const REACTION_ERROR = "❌"; // shown if the turn errors

const toTimestamp = (item) => (item.posted_ts ? item.posted_ts * 1000 : Date.now());

function buildMessage({ kind, item, conversationId, threadId, groupId, directMention, context = {} }) {
  const peer = routingPeer({ kind, conversationId, threadId });
  return {
    messageId: String(item.id),
    kind,
    conversationId,
    threadId,
    groupId,
    peerKind: peer.peerKind,
    peerId: peer.peerId,
    isGroup: peer.isGroup,
    senderId: item.creator,
    senderName: item.creator_name,
    text: item.content,
    timestamp: toTimestamp(item),
    directMention,
    // Twist context for the agent:
    threadTitle: context.threadTitle,
    channelName: context.channelName,
    transcript: context.transcript,
  };
}

export function monitorTwistProvider({ accountId, config, runtime, abortSignal, statusSink, cursors }) {
  const core = getTwistRuntime();
  const cfg = config ?? core.config.current();
  const account = resolveTwistAccount(cfg);
  if (!account.configured) {
    throw new Error("twist: not configured (need token, workspaceId, botUserId in channels.twist)");
  }
  const client = createTwistClient({ token: account.token, workspaceId: account.workspaceId });
  const botUserId = account.botUserId;
  const log = (m) => runtime.log?.(`[twist] ${m}`);

  let stopped = false;
  let timer = null;
  let cycle = 0;
  const inFlight = new Set(); // peerId currently running an agent turn

  // Reaction target for the triggering message: comment_id for threads,
  // message_id for conversation (DM / group DM) messages. Best-effort.
  function reactionTarget(message) {
    const id = Number(message.messageId);
    return message.kind === "thread" ? { commentId: id } : { messageId: id };
  }
  async function safeReact(verb, message, reaction) {
    try {
      const target = { ...reactionTarget(message), reaction };
      if (verb === "add") await client.addReaction(target);
      else await client.removeReaction(target);
    } catch (err) {
      log(`reaction ${verb} ${reaction} failed for ${message.peerId}: ${String(err)}`);
    }
  }

  // Fire an agent turn without blocking the poll loop. Reaction lifecycle on the
  // triggering message: 🕰️ on pickup -> removed on success / replaced with ❌ on error.
  function fireDispatch(message) {
    if (inFlight.has(message.peerId)) {
      log(`skip ${message.peerId}: turn already in flight`);
      return;
    }
    inFlight.add(message.peerId);
    log(`dispatching ${message.kind} ${message.peerId} from ${message.senderName}`);
    const watchdog = setTimeout(() => log(`dispatch ${message.peerId} still running >${DISPATCH_TIMEOUT_MS}ms`), DISPATCH_TIMEOUT_MS);
    void (async () => {
      await safeReact("add", message, REACTION_PROCESSING);
      try {
        await handleTwistInbound({ message, account, cfg: core.config.current() ?? cfg, runtime, client, statusSink });
        await safeReact("remove", message, REACTION_PROCESSING);
        await safeReact("add", message, REACTION_DONE);
      } catch (err) {
        log(`dispatch ${message.peerId} failed: ${String(err)}`);
        await safeReact("remove", message, REACTION_PROCESSING);
        await safeReact("add", message, REACTION_ERROR);
      } finally {
        clearTimeout(watchdog);
        inFlight.delete(message.peerId);
      }
    })();
  }

  // Cache participant-derived kind within a cycle to avoid refetching.
  async function participantKind(convId, cache) {
    if (cache.has(convId)) return cache.get(convId);
    let kind = "dm";
    try {
      const conv = await client.getConversation(convId, abortSignal);
      kind = classifyConversation(conversationParticipantCount(conv));
    } catch (err) {
      log(`participant lookup failed for conv ${convId}: ${String(err)}`);
    }
    cache.set(convId, kind);
    return kind;
  }

  async function processConversation(c, cache) {
    const convId = c.conversation_id;
    const messages = await client.getConversationMessages(convId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal });
    if (cursors.isFirstSight("conversations", convId)) {
      await cursors.setCursor("conversations", convId, advanceCursor(c.obj_index ?? 0, messages)); // baseline to latest
      return;
    }
    const kind = await participantKind(convId, cache);
    const cursor = cursors.getCursor("conversations", convId);
    let fresh = newInboundItems(messages, cursor, botUserId);
    if (kind === "groupdm") fresh = fresh.filter((m) => contentMentionsBot(m.content, botUserId));
    await cursors.setCursor("conversations", convId, advanceCursor(cursor, messages)); // at-most-once
    if (fresh.length && shouldRespondToConversation({ kind, directMention: c.direct_mention })) {
      const trigger = fresh[fresh.length - 1];
      // Never drop the other new messages from this cycle: group DMs get the full
      // recent transcript; 1:1 DMs get the OTHER fresh messages in this batch
      // (so e.g. a link sent just before a question still reaches the agent).
      const context = {
        transcript: buildTranscript(kind === "groupdm" ? messages : fresh, trigger.id),
      };
      fireDispatch(buildMessage({ kind, item: trigger, conversationId: convId, directMention: Boolean(c.direct_mention), context }));
    }
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

  async function processThread(t) {
    const threadId = t.thread_id;
    const comments = await client.getThreadComments(threadId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal });
    if (cursors.isFirstSight("threads", threadId)) {
      await cursors.setCursor("threads", threadId, advanceCursor(t.obj_index ?? 0, comments)); // baseline to latest
      return;
    }
    const cursor = cursors.getCursor("threads", threadId);
    const fresh = newInboundItems(comments, cursor, botUserId).filter((c) => contentMentionsBot(c.content, botUserId));
    await cursors.setCursor("threads", threadId, advanceCursor(cursor, comments)); // at-most-once
    if (fresh.length) {
      const trigger = fresh[fresh.length - 1];
      const context = await fetchThreadContext(threadId, t.channel_id, comments, trigger.id);
      fireDispatch(buildMessage({ kind: "thread", item: trigger, threadId, groupId: t.channel_id, directMention: true, context }));
    }
  }

  async function pollOnce() {
    const convs = await client.getUnreadConversations(abortSignal);
    const threads = await client.getUnreadThreads(abortSignal);
    const mentionThreads = threads.filter((t) => t.direct_mention);
    cycle++;
    if (core.logging?.shouldLogVerbose?.()) {
      log(`poll #${cycle}: ${convs.length} unread convs, ${threads.length} unread threads (${mentionThreads.length} mention), ${inFlight.size} in flight`);
    }

    const cache = new Map();
    for (const c of convs) {
      if (stopped) return;
      try {
        await processConversation(c, cache);
      } catch (err) {
        log(`conversation ${c.conversation_id} failed: ${String(err)}`);
      }
    }
    for (const t of mentionThreads) {
      if (stopped) return;
      try {
        await processThread(t);
      } catch (err) {
        log(`thread ${t.thread_id} failed: ${String(err)}`);
      }
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

  log(`polling workspace ${account.workspaceId} every ${account.pollIntervalMs}ms (bot ${botUserId})`);
  void loop();

  return {
    stop: () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    },
  };
}
