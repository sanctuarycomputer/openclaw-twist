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
  turnDeliveryVerdict,
  fastAckDecision,
  replyRecipients,
} from "./routing.js";
import { admissionVerdict, handleTwistInbound } from "./inbound.js";
import { postToTwist } from "./outbound.js";
import { createQueueStore } from "./queue.js";
import { createProducer } from "./producer.js";
import { createConsumer, REPLAY_HORIZON_MS } from "./consumer.js";
import {
  MIN_WEBHOOK_TOKEN_LENGTH,
  createHintDebouncer,
  createWebhookHandler,
  extractContainerHint,
} from "./webhook.js";
import {
  CYCLE_DEADLINE_MS,
  DEADLINE_EXPIRED,
  composeCycleSignal,
  createHintFunnel,
  createHintSweeper,
  createPokeGate,
  settleWithinDeadline,
} from "./cycle.js";
import { createWebhookTrace } from "./webhook-trace.js";
import { registerTwistWebhookRoute } from "./webhook-route.js";
import { getTwistRuntime } from "./runtime.js";

const ITEM_FETCH_LIMIT = 30;
// On first sight of a thread/conv, items posted within this window before boot still
// count as "fresh" (answerable) — covers an @mention sent while the bot was down for a
// deploy/restart/short outage. Older items are enqueued but marked firstSightBacklog and
// skipped by the consumer. Sized to span a realistic outage (not just a deploy): a
// 10-minute window silently swallowed thread mentions that landed during longer downtime.
const FIRST_SIGHT_GRACE_MS = 2 * 60 * 60 * 1000;
// Trailing window for webhook hints. One human action can fan out into several deliveries;
// this collapses them into a single targeted sweep while keeping hint→sweep latency well
// inside the "⏳ within ~5s" product contract the fast-ack path is sized against.
const WEBHOOK_HINT_DEBOUNCE_MS = 2000;

/**
 * Per-process, per-queue-file ingestion state: one store, one in-flight map, one boot
 * recovery. Keyed by queue path because that is what identifies the durable queue.
 *
 * OpenCLAW hot-reload (`channels.twist` → restart-channel:twist) calls stopChannel() then
 * startChannel() in the SAME process, and stop() does not drain in-flight settle() work.
 * So an outgoing monitor's turns keep running against the queue while the incoming monitor
 * builds a fresh producer and consumer. Three things must therefore outlive a restart:
 *
 *   - `store` — STATE VISIBILITY + single-writer. Two stores over one file would keep two
 *     independent in-memory snapshots and clobber each other's writes; one store means the
 *     outgoing settle()'s verdict (done / retry / failed) lands where the incoming consumer
 *     reads it, and `writeChain` still serializes every persist.
 *   - `inFlight` — CONCURRENCY ACCOUNTING. This is the one the store cannot cover: a live
 *     turn's own item is `processing` (so selectClaimable skips it), but OTHER queued items
 *     on that same peer still look claimable to a consumer with an empty in-flight map.
 *     Sharing it keeps per-peer exclusion AND MAX_GLOBAL_TURNS process-wide, instead of
 *     letting a restart run two turns in one conversation and 2× the global cap.
 *   - `recovery` — recoverOrphans() must run ONCE per process. Re-running it on a restart
 *     would probe items a live turn is mid-way through and requeue them: duplicate turn,
 *     double reply. Genuine orphans come only from a process crash, which by definition
 *     drops this Map, so the next process does recover them.
 */
const QUEUE_STORES = new Map(); // queuePath -> { store, ready, recovery, inFlight, log }

function acquireQueueStore(queuePath, log) {
  let entry = QUEUE_STORES.get(queuePath);
  if (entry) {
    entry.log = log; // a restart brings a fresh logger; the store keeps writing to the live one
    return entry;
  }
  entry = { log, store: null, ready: null, recovery: null, inFlight: new Map() };
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
export async function monitorTwistProvider({ accountId, config, runtime, abortSignal, statusSink, cursors, queuePath, tracePath }) {
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
  // Cancels everything this monitor has in flight. The account's own abortSignal is not
  // enough: stop() is also called directly (channel hot reload) WITHOUT the host aborting
  // ctx.abortSignal, and in that case nothing would cancel an in-flight bypass sweep — it
  // would keep hitting Twist on behalf of a monitor that no longer exists. Cycles and
  // sweeps compose their deadlines from this, so both routes to shutdown cancel them.
  const lifecycle = new AbortController();
  abortSignal?.addEventListener?.("abort", () => lifecycle.abort(), { once: true });

  const queueEntry = acquireQueueStore(queuePath, log);
  await queueEntry.ready;
  const queue = queueEntry.store;

  // ---------------------------------------------------------------- helpers

  // Conversation kind never changes for a given conversation, so this cache is
  // process-lived (not per-cycle): classifyPeer is called per item, and refetching
  // the conversation for every queued message would multiply poll cost.
  // Only SUCCESSFUL lookups are cached — a transient failure must not poison the
  // classification forever — and a failed lookup THROWS so the consumer requeues the
  // item with backoff instead of guessing a kind and mis-routing (a wrong "dm" would
  // bypass the group mention requirement; a wrong "groupdm" would drop a real DM).
  const peerKindCache = new Map();
  async function participantKind(convId, signal = abortSignal) {
    const key = String(convId);
    if (peerKindCache.has(key)) return peerKindCache.get(key);
    let conv;
    try {
      conv = await client.getConversation(convId, signal);
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
      // Who Twist notified about this post; the reply mirrors it (see replyRecipients).
      // Both halves: a default-audience comment carries its audience in `groups`.
      recipients: item.recipients ?? null,
      groups: item.groups ?? null,
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
  // before dispatch. The logging lives in ONE place — the consumer's skip branch — which
  // reports admission denials alongside backlog/no-mention skips in the same format.
  const admission = async (item) =>
    await admissionVerdict({
      message: await toNormalizedMessage(item),
      account,
      cfg: core.config.current() ?? cfg,
    });

  const runTurn = async (item, { commandAuthorized }) => {
    const message = await toNormalizedMessage(item, { withContext: true });
    log(`dispatching ${message.kind} ${message.peerId} from ${message.senderName}`);
    // A turn whose ONLY output was openclaw's incomplete_turn placeholder produced no
    // answer. Delivery already dropped the placeholder (see handleTwistInbound), so the
    // human sees nothing — and if we returned normally the consumer would record the item
    // `done` and the request would be gone. Fail instead: the item goes back on the retry
    // ladder (30s/2m/10m/1h…) and, only if every attempt burns, dead-letters loudly with
    // the in-place apology and the ops alert. Observed live: a transient flake consumed a
    // user's thread mention, with the placeholder posted as if it were the reply.
    const deliveries = [];
    await handleTwistInbound({
      message,
      account,
      cfg: core.config.current() ?? cfg,
      runtime,
      client,
      statusSink,
      verdict: { admit: true, admission: "dispatch", commandAuthorized },
      onDelivery: (outcome) => deliveries.push(outcome),
    });
    const { retry } = turnDeliveryVerdict(deliveries);
    if (retry === "incomplete-turn") throw new Error("incomplete-turn fallback suppressed — retrying");
    if (retry === "delivery-failed") throw new Error("reply delivery to Twist failed — retrying");
  };

  // Boot recovery: did the bot already answer this item before the crash? True when a
  // post of ours landed at/after the claim. Twist timestamps are seconds.
  const probe = async (item) => {
    const since = Math.floor((item.claimedAt ?? 0) / 1000);
    const posts =
      (item.kind === "conv"
        ? await client.getConversationMessages(item.conversationId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })
        : await client.getThreadComments(item.threadId, { limit: ITEM_FETCH_LIMIT, signal: abortSignal })) ?? [];
    return posts.some((p) => String(p.creator) === String(botUserId) && (p.posted_ts ?? 0) >= since);
  };

  // Twist's reactions API addresses a conversation MESSAGE, a thread COMMENT, or a
  // thread's OPENING POST (reactions/add|remove take thread_id as a first-class target),
  // so every item kind we queue is reactable.
  function reactionTarget(item) {
    if (item.kind === "conv") return { messageId: Number(item.messageId) };
    if (item.kind === "thread") return { commentId: Number(item.messageId) };
    return { threadId: Number(item.threadId) }; // "thread-post": the opening post
  }
  // Best-effort: a reaction is cosmetic and must never fail (and thus retry) a turn.
  async function react(item, verb, reaction, signal = abortSignal) {
    const target = reactionTarget(item);
    try {
      if (verb === "add") await client.addReaction({ ...target, reaction }, signal);
      else await client.removeReaction({ ...target, reaction }, signal);
    } catch (err) {
      log(`reaction ${verb} ${reaction} failed for ${item.id}: ${String(err)}`);
    }
  }

  // ---------------------------------------------------- injected producer effects

  /**
   * Fast ack: the ⏳ that tells a human "seen" is added at INGESTION, not at claim.
   *
   * The claim-time ⏳ is only as fast as the consumer reaches the item, and the consumer
   * takes one item per peer per tick — so a mention landing in a thread with 15 unprocessed
   * comments in front of it went unacknowledged for 15 ticks (over a minute at the default
   * cadence). The product contract is ⏳ within ~5s of the mention, so it moves here.
   *
   * Only messages the bot would actually answer get one — an ack we don't honor is worse
   * than none: any container where the bot is @mentioned, plus EVERY message in a 1:1 DM
   * (DMs need no mention). A group DM message without a mention gets nothing.
   *
   * The replay horizon is re-checked HERE, not just at claim time. Paging forward from a
   * stale cursor after a long outage can enqueue hundreds of items the consumer will
   * immediately condemn as `stale` — in a 1:1 DM (where no mention is needed) that is a
   * reaction call each, run sequentially in front of consumer.tick(). Acking them would be
   * both a lie and a self-inflicted stall.
   *
   * The consumer still adds ⏳ when it claims (safeReact tolerates the duplicate — Twist
   * treats a repeated reaction from the same user as a no-op), and its normal remove-⏳ →
   * ✅ / ❌ lifecycle clears this reaction with no extra bookkeeping. The one outcome with
   * no reaction step of its own is a SKIP, so the consumer clears the ack there explicitly
   * (see `wasFastAcked`) — an acknowledged mention must never be left wearing ⏳ forever.
   */
  async function shouldFastAck(item, signal) {
    const decision = fastAckDecision(item, { botUserId, nowMs: Date.now(), replayHorizonMs: REPLAY_HORIZON_MS });
    if (decision !== "peer-kind") return decision === "ack";
    return (await participantKind(item.conversationId, signal)) === "dm";
  }
  // NOTE: this is reached only from the producer, for items an authenticated fetch actually
  // returned and that were durably enqueued. Acking straight off a webhook payload's message
  // id would be faster and is deliberately not done — the payload is unsigned, so that id is
  // unverified and a forged request could make the bot paint reactions on arbitrary
  // messages. See the contract at the top of webhook.js.
  // `signal` is the SWEEP's signal (threaded by producer.enqueueAcked), not the account's:
  // both Twist calls below must be cancellable by that sweep's deadline, or a hung reaction
  // pins the container guard and a fast-path slot for the life of the process.
  async function fastAck(item, signal = abortSignal) {
    if (!(await shouldFastAck(item, signal))) return;
    await react(item, "add", "⏳", signal); // react() is already best-effort; the producer contains throws anyway
  }

  const alert = async (text) => {
    try {
      const { kind, id } = resolveOutboundTarget(null, account.defaultTo);
      await postToTwist({ client, kind, id, text });
    } catch (err) {
      log(`alert delivery failed: ${String(err)}`);
    }
  };

  // The dead-letter apology answers the same message the reply would have, so it owes the
  // same audience: without this, an answer that fails notifies MORE people than one that
  // succeeds — the exact fan-out this mirrors away from.
  const replyInPlace = (item, text) =>
    postToTwist({
      client,
      kind: item.kind === "conv" ? "conv" : "thread",
      id: item.kind === "conv" ? item.conversationId : item.threadId,
      text,
      recipients: replyRecipients(item, botUserId) ?? undefined,
    });

  // Built PER CYCLE rather than once, so each cycle's Twist reads carry that cycle's
  // deadline signal (see runCycle). The factory is pure closure construction — no I/O, no
  // state of its own — so one allocation per poll is free; all durable state lives in the
  // queue and cursor stores it is handed. Declared here rather than at the top of the
  // function so `fastAck` (and the helpers it leans on) are already in scope.
  const makeProducer = (signal) =>
    createProducer({ client, queue, cursors, botUserId, freshSinceTs, now: Date.now, log, abortSignal: signal, fastAck });

  const consumer = createConsumer({
    queue, botUserId, now: Date.now, log,
    classifyPeer, admission, runTurn, probe, react, alert, replyInPlace,
    inFlight: queueEntry.inFlight, // shared across restarts — see QUEUE_STORES
  });

  // ------------------------------------------------------------------ poll loop
  //
  // THE SEAM webhook hints funnel into, and why it is this one.
  //
  // Before webhooks, mutual exclusion between cycles was implicit: `loop()` only re-armed
  // its timer AFTER its cycle resolved, so exactly one cycle could ever be in flight and
  // nothing needed a lock. A webhook-triggered sweep firing on its own would break that by
  // construction — two concurrent sweeps of the same container would double-fetch from the
  // same cursor, and both would see the same items as "not yet queued" (`queue.has` is read
  // before the awaited `enqueueAll`), so the loser's fast-ack would fire twice and the
  // cursor could be written backwards.
  //
  // Rather than bolt a lock onto a loop that never had one, hints funnel into
  // `pendingHints` and every cycle — scheduled or webhook-triggered — goes through
  // `runExclusive`, which chains cycles onto a single promise. That makes the old implicit
  // invariant explicit and total: at most one cycle body runs at a time, webhook and poll
  // sweeps included, and they take the queue/cursor stores in the same order they always
  // did. The alternative (a mutex around producer.sweep*) would have serialized the sweeps
  // but still let a webhook cycle's consumer.tick() interleave with the poll cycle's, which
  // is a different race for no benefit.
  //
  // The funnel is a bounded Map keyed by container, so a hint arriving while a cycle is
  // running is absorbed into the NEXT cycle rather than dropped or duplicated — and a
  // FLOOD of hints degrades to "just do a full poll" instead of queueing unbounded work.
  // See cycle.js for why every overflow path is safe.
  const funnel = createHintFunnel({ log });
  let cycleChain = Promise.resolve();

  async function runCycle({ full = false, tickOnly = false, signal }) {
    const drained = funnel.drain();
    let sweepAll = full || drained.sweepAll;
    // A queued webhook cycle whose hints an earlier cycle already drained has nothing to do.
    // `tickOnly` is the exception: a bypass sweep has already enqueued (and acked) its work
    // off-chain and just needs the claim pass to run.
    if (!sweepAll && drained.hints.length === 0 && !tickOnly) return;
    cycle++;
    // This cycle's producer carries this cycle's deadline, so a stalling Twist read is
    // actively cancelled rather than merely abandoned.
    const producer = makeProducer(signal);
    for (const hint of drained.hints) {
      if (stopped) return;
      try {
        const outcome = await producer.sweepContainer(hint);
        // An untracked container is not swept directly: for threads that would ingest what
        // the poll's direct_mention filter would never have touched, and for conversations
        // it would be an authenticated enumeration primitive. Either way the hint degrades
        // to a full poll, which still picks the container up if the poll would have.
        if (outcome?.swept === false) {
          sweepAll = true;
          if (core.logging?.shouldLogVerbose?.()) {
            log(`webhook hint ${hint.kind}:${hint.id} degraded to a full poll (${outcome.reason})`);
          }
        }
      } catch (err) {
        log(`webhook sweep ${hint.kind}:${hint.id} failed: ${String(err)}`);
      }
    }
    if (sweepAll) await producer.pollOnce();
    if (stopped) return;
    await consumer.tick();
    // Pruning walks the whole queue and rewrites the file; it is housekeeping, not
    // latency-critical, so it rides the scheduled poll rather than every claim poke.
    if (!tickOnly) await queue.prune(Date.now());
    if (core.logging?.shouldLogVerbose?.()) {
      log(`poll #${cycle}: queue depth ${queue.nonTerminalCount()}, ${consumer.inFlightCount()} turn(s) in flight`);
    }
  }

  /**
   * Run one cycle under a hard wall-clock deadline.
   *
   * The composed signal cancels the producer's in-flight reads, which is where a stalling
   * Twist API actually bites. The race is the backstop: it guarantees the exclusive chain
   * is RELEASED at the deadline even if something the signal doesn't reach is slow, so a
   * single bad cycle can never stop the bot answering anyone.
   *
   * Abandoning a cycle mid-flight is safe by construction: cursors advance only over
   * batches already durably enqueued, enqueue is idempotent by item id, and `tick()` has
   * its own reentrancy guard — so the next cycle simply re-sweeps.
   */
  async function runCycleWithDeadline(opts) {
    const { signal, deadline } = composeCycleSignal(lifecycle.signal, { deadlineMs: CYCLE_DEADLINE_MS });
    let done = false;
    let failure = null;
    // Always settles, so abandoning it below can never leave an unhandled rejection.
    const body = runCycle({ ...opts, signal }).then(
      () => { done = true; },
      (err) => { done = true; failure = err; },
    );
    if (deadline) {
      const blown = new Promise((resolve) => {
        if (deadline.aborted) resolve();
        else deadline.addEventListener("abort", () => resolve(), { once: true });
      });
      await Promise.race([body, blown]);
      if (!done) {
        // Loud on purpose: this means Twist stalled past two minutes, which is never normal
        // and is the difference between "slow" and "the bot has gone quiet".
        log(
          `poll cycle #${cycle} exceeded the ${CYCLE_DEADLINE_MS}ms deadline — abandoning it and releasing the poll chain; the next cycle re-sweeps (cursor + queue dedup make that safe)`,
        );
        return;
      }
    } else {
      await body;
    }
    if (failure) throw failure;
  }

  /** Run a cycle with global mutual exclusion; never rejects (failures are logged). */
  function runExclusive(opts = {}) {
    const run = cycleChain.then(async () => {
      // Before the `stopped` check: a poke that never runs must still free the gate, or a
      // restarted monitor could never queue another one.
      if (opts.tickOnly) pokeGate.starting();
      if (stopped) return;
      try {
        await runCycleWithDeadline(opts);
      } catch (err) {
        log(`poll cycle failed: ${String(err)}`);
      }
    });
    // Chain off the settled promise so one failed cycle cannot poison every later one.
    cycleChain = run.catch(() => {});
    return run;
  }

  // At most one queued claim pass at a time — see createPokeGate for why N sweeps must not
  // become N cycles.
  const pokeGate = createPokeGate(() => void runExclusive({ tickOnly: true }));

  async function loop() {
    if (stopped) return;
    await runExclusive({ full: true });
    if (stopped) return;
    timer = setTimeout(loop, account.pollIntervalMs);
  }

  // ------------------------------------------------------------- webhook ingress

  const trace = tracePath ? createWebhookTrace(tracePath, { log }) : null;

  /**
   * The FAST PATH. A targeted hint sweep runs the moment it is flushed, off the exclusive
   * chain — that queueing delay was the entire 3.0s-vs-6.6s ingest variance. See
   * createHintSweeper for the concurrency-safety argument (verified against producer.js,
   * queue.js and state.js).
   *
   * Everything that is NOT a targeted sweep of a tracked container stays chain-bound and
   * unchanged: the full-poll degrades, the unrecognizable-payload fallback, and claiming.
   */
  const sweeper = createHintSweeper({
    log,
    sweep: async (hint) => {
      // Each bypass sweep carries its own deadline, exactly like a chain cycle.
      const { signal, deadline } = composeCycleSignal(lifecycle.signal, { deadlineMs: CYCLE_DEADLINE_MS });
      const work = makeProducer(signal)
        .sweepContainer(hint)
        .then((outcome) => ({ outcome }), (err) => ({ err }));

      // The signal alone cannot be trusted to end this: it only reaches calls that thread
      // it, so one un-signalled path is enough to hang the sweep forever — and a hung sweep
      // holds this container's guard AND one of the few fast-path slots for the life of the
      // process, silently. The race guarantees we let go on time regardless.
      //
      // Releasing the guard while the abandoned work may still be running is safe, and this
      // is exactly why the concurrency argument had to be nailed down first: an orphan
      // overlapping a fresh sweep of the same container is the same interleaving the poll
      // loop already races, covered by the atomic filter+enqueue (no double-enqueue or
      // double-ack), monotonic cursors, and the whenPersisted durability edge.
      const settled = await settleWithinDeadline(work, deadline);
      if (settled === DEADLINE_EXPIRED) {
        log(
          `hint sweep ${hint.kind}:${hint.id} exceeded the ${CYCLE_DEADLINE_MS}ms deadline — abandoning it and releasing the container; falling back to a full poll`,
        );
        return { swept: false, reason: "deadline" };
      }
      if (settled.err) {
        if (deadline?.aborted) {
          log(`hint sweep ${hint.kind}:${hint.id} was cancelled by its ${CYCLE_DEADLINE_MS}ms deadline — falling back to a full poll`);
          return { swept: false, reason: "deadline" };
        }
        throw settled.err;
      }
      return settled.outcome;
    },
    // The ⏳ already went out inside the sweep (producer fastAck), so all that is left is the
    // claim pass — and that stays chain-serialized, because per-peer and global turn caps
    // depend on exactly one claim pass running at a time.
    onSwept: () => pokeGate.request(),
    onDegrade: (hint, reason) => {
      if (reason === "at-capacity") {
        // Too many bypass sweeps already running. Fall back to the pre-0.5.2 behaviour:
        // hand the hint to the funnel so a chain cycle sweeps it, still targeted, still
        // coalesced and capped. Only if the funnel itself overflows does it become a poll.
        funnel.push(hint);
      } else {
        // Untracked container, or a blown deadline — the poll is the strict superset.
        funnel.requestFullSweep();
      }
      void runExclusive();
    },
  });

  // Hints are debounced per container, then dispatched. The debouncer collapses an event
  // burst for ONE container (leading + at most one trailing); the sweeper bounds how many
  // containers sweep at once; the funnel catches whatever the sweeper declines.
  const hintDebouncer = createHintDebouncer({
    delayMs: WEBHOOK_HINT_DEBOUNCE_MS,
    log,
    onFlush: (hint) => {
      if (stopped) return;
      if (hint.kind === "all") {
        funnel.requestFullSweep();
        void runExclusive();
        return;
      }
      sweeper.dispatch(hint);
    },
  });

  let unregisterWebhook = null;

  function shutdown() {
    stopped = true;
    if (timer) clearTimeout(timer);
    hintDebouncer.cancelAll();
    lifecycle.abort(); // cancel in-flight bypass sweeps and the running cycle, if any
    try {
      unregisterWebhook?.();
    } catch (err) {
      log(`webhook deregistration failed: ${String(err)}`);
    }
    unregisterWebhook = null;
  }

  /**
   * Open the webhook route. Deliberately NOT called until boot orphan recovery has
   * settled: a hint that lands mid-recovery would schedule a cycle, and that cycle's
   * consumer.tick() would run against items recoverOrphans() is still probing — exactly
   * the double-dispatch the "recovery completes before the first tick" invariant exists
   * to prevent. Latency costs nothing here; correctness does.
   */
  function startWebhookIngress() {
    // `stopped` is re-checked HERE, not only at the top of the monitor: this runs after an
    // awaited boot recovery, and the account can be aborted during that await. Registering
    // a route for an account that is already shutting down would leak it — shutdown() has
    // already run and will not run again to deregister it.
    if (stopped) return;
    if (!account.webhookEnabled) {
      if (account.webhookDisabledReason === "token-too-short") {
        // An ERROR, not a debug line: the operator explicitly asked for webhook ingress and
        // is not getting it. Silence here looks exactly like "webhooks are broken".
        (runtime.error ?? log)(
          `[twist] webhook ingress REFUSED: channels.twist.webhookToken is shorter than ${MIN_WEBHOOK_TOKEN_LENGTH} characters. ` +
            "The token travels in the webhook URL and is the endpoint's only authentication — use a long random value " +
            "(e.g. `openssl rand -hex 32`). Continuing with poll only.",
        );
      }
      return;
    }
    const onEvent = createWebhookHandler({
      extract: extractContainerHint,
      log,
      // Scheduling only — this returns immediately so the HTTP 200 is not held behind a
      // Twist round-trip. "poll" rides the same debouncer under a reserved key so a flood
      // of unrecognizable deliveries collapses exactly like a flood of real ones.
      schedule: (decision) => {
        const hint = decision.action === "sweep" ? decision.hint : { kind: "all", id: "*" };
        // Best-effort breadcrumb: lets the ledger split Twist's delivery leg from ours.
        // Never awaited — a diagnostics write must not sit in front of the HTTP 200.
        void trace?.record({ t: Date.now(), k: `${hint.kind}:${hint.id}`, e: decision.eventTs });
        hintDebouncer.push(hint);
      },
    });
    try {
      unregisterWebhook = registerTwistWebhookRoute({
        accountId: account.accountId,
        path: account.webhookPath,
        secret: account.webhookToken,
        config: cfg,
        runtime,
        statusSink,
        onEvent,
      });
      log(`webhook ingress enabled at ${account.webhookPath} (hint-only; poll remains authoritative)`);
    } catch (err) {
      // Never fatal: the poll loop is the source of truth and works without this.
      log(`webhook ingress registration failed (continuing with poll only): ${String(err)}`);
      unregisterWebhook = null;
    }
  }

  abortSignal?.addEventListener?.("abort", shutdown);

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
  startWebhookIngress(); // only now — recovery has settled, so a hint can safely schedule a cycle
  void loop();

  return { stop: shutdown };
}
