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

/**
 * One queue store per queue file per PROCESS, and one boot recovery per store.
 *
 * OpenCLAW hot-reload (`channels.twist` → restart-channel:twist) calls stopChannel()
 * then startChannel() in the same process, and stop() does not drain in-flight settle()
 * work. Without this memo the restarted account would build a SECOND store over the same
 * file — two independent in-memory snapshots clobbering each other's writes — and re-run
 * recoverOrphans over an item a still-live turn is mid-way through, requeueing it for a
 * duplicate agent turn and a double reply.
 *
 * Sharing the store makes the restart safe. Producer and consumer ARE rebuilt per start
 * (they hold no durable state), which gives the new consumer an empty `inFlight` map while
 * the old consumer's settle() calls are still running — safe, because:
 *   - the old turn's item sits in state "processing" in the SHARED store, and
 *     selectClaimable only ever returns state === "queued", so the new consumer cannot
 *     claim it (per-peer inFlight tracking is not what protects it — the state does);
 *   - the old settle() finishes by transitioning that same shared store, so its verdict
 *     (done / retry / failed) is not lost;
 *   - recovery is skipped on restart, so the mid-flight item is never requeued while the
 *     process lives. Genuine orphans can only come from a process crash, which by
 *     definition drops this Map, so the next process does recover them.
 * The single-writer property still holds: `writeChain` inside the one shared store
 * serializes every persist.
 *
 * One hole that argument does NOT close: stop() can land while the OLD monitor is inside
 * consumer.tick(). Claim reservation is per-consumer and in-memory (inFlight), and the
 * "processing" state only lands after an awaited persist — so two consumers running claim
 * passes concurrently could both select the same still-"queued" item. `tickChain` closes
 * it by serializing claim passes across every monitor instance sharing this store, the
 * cross-instance twin of the consumer's own `ticking` guard. It chains tick() only — not
 * settle() — so turns still run concurrently up to MAX_GLOBAL_TURNS.
 */
const QUEUE_STORES = new Map(); // queuePath -> { store, ready, recovery, tickChain, log }

function acquireQueueStore(queuePath, log) {
  let entry = QUEUE_STORES.get(queuePath);
  if (entry) {
    entry.log = log; // a restart brings a fresh logger; the store keeps writing to the live one
    return entry;
  }
  entry = { log, store: null, ready: null, recovery: null, tickChain: Promise.resolve() };
  entry.store = createQueueStore(queuePath, { log: (m) => entry.log?.(m) });
  // Cache the load PROMISE (not its result) so two concurrent starts share one load; evict
  // on failure so a transient IO fault doesn't wedge the account until process restart.
  entry.ready = entry.store.load(Date.now()).catch((err) => {
    QUEUE_STORES.delete(queuePath);
    throw err;
  });
  QUEUE_STORES.set(queuePath, entry);
  return entry;
}

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

  const queueEntry = acquireQueueStore(queuePath, log);
  await queueEntry.ready;
  const queue = queueEntry.store;

  const producer = createProducer({ client, queue, cursors, botUserId, freshSinceTs, now: Date.now, log, abortSignal });

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

  // "Conversation so far" must be STRICTLY PRIOR to the trigger. selectClaimable claims the
  // OLDEST queued item, so same-peer siblings still in the queue are FUTURE messages — each
  // gets its own turn, in order, with its own prior context. Already-answered earlier items
  // are fine to include (they come from the same fresh fetch and are what the human sees);
  // the trigger itself is dropped by buildTranscript.
  const strictlyPrior = (items, item) => items.filter((it) => (it.obj_index ?? 0) < item.objIndex);

  async function buildContext(item) {
    if (item.kind === "conv") {
      const messages = (await client.getConversationMessages(item.conversationId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
      return { transcript: buildTranscript(strictlyPrior(messages, item), item.messageId) };
    }
    // A thread-post item has objIndex 0 (it IS the opening post), so it correctly gets no
    // prior comments.
    const comments = (await client.getThreadComments(item.threadId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
    return await fetchThreadContext(item.threadId, item.channelId, strictlyPrior(comments, item), item.messageId);
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
    const context = withContext ? await buildContext(item) : {};
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

  // Denials used to be logged by handleTwistInbound's drop path; now they short-circuit
  // before dispatch, so log them here — a silently skipped message is exactly the failure
  // mode this pipeline exists to make visible.
  const admission = async (item) => {
    const verdict = await admissionVerdict({
      message: await toNormalizedMessage(item),
      account,
      cfg: core.config.current() ?? cfg,
    });
    if (!verdict.admit) log(`skip ${item.id} (${item.peerId}): admission=${verdict.admission}`);
    return verdict;
  };

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

  // Claim passes are serialized across every monitor instance sharing this queue store
  // (see QUEUE_STORES): during an in-process restart the outgoing monitor can still be
  // inside a tick(), and two independent claim passes could double-claim one item.
  function tick() {
    const next = queueEntry.tickChain.then(() => consumer.tick());
    queueEntry.tickChain = next.catch(() => {}); // a failed tick must not poison the chain
    return next;
  }

  async function pollOnce() {
    cycle++;
    await producer.pollOnce();
    if (stopped) return;
    await tick();
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

  // Boot-only: items left "processing" by a CRASHED process are probed and either marked
  // done (the reply landed) or requeued. MUST complete before the first tick(), and must
  // run at most once per queue file per process — see QUEUE_STORES above for why an
  // in-process channel restart must NOT re-recover.
  if (!queueEntry.recovery) {
    queueEntry.recovery = consumer.recoverOrphans().catch((err) => {
      queueEntry.recovery = null; // let a later start retry recovery
      throw err;
    });
  }
  await queueEntry.recovery;

  log(`polling workspace ${account.workspaceId} every ${account.pollIntervalMs}ms (bot ${botUserId})`);
  void loop();

  return {
    stop: () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    },
  };
}
