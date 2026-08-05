// State-machine orchestration. All effects are injected; nothing here touches
// the network or the SDK directly, so the whole delivery guarantee is testable.
import { contentMentionsBot } from "./routing.js";

export const BACKOFF_MS = [30_000, 120_000, 600_000, 3_600_000, 3_600_000];
export const MAX_ATTEMPTS = 6;
export const MAX_GLOBAL_TURNS = 3;
export const HIGH_WATER = 50;
export const HUNG_TURN_ALERT_MS = 30 * 60_000;

// Permanent = the message/target is gone; retrying can never help. Generic 400s (bad
// request) are NOT included — they may be transient (rate-limit-shaped, malformed by a
// retry-able upstream hiccup, etc.) and should ride the backoff ladder like anything else.
export function isPermanentError(err) {
  const status = err?.status ?? err?.statusCode;
  if (status === 404 || status === 410) return true;
  return /not found|does not exist|deleted/i.test(String(err?.message ?? ""));
}

/**
 * @param {object} deps
 * @param {Map<string, {id:string, startedAt:number, hungAlerted:boolean}>} [deps.inFlight]
 *   peerId -> live turn. Injectable because per-peer exclusion and MAX_GLOBAL_TURNS must
 *   hold across CONSUMER INSTANCES, not just within one: an in-process channel restart
 *   builds a new consumer over the same queue file while the outgoing instance's settle()
 *   calls are still running. Sharing the store alone is not enough — a claimed item is
 *   `processing` in the store, but OTHER queued items on that same peer would still look
 *   claimable to a consumer whose own inFlight map is empty. Defaults to a private Map.
 */
export function createConsumer({ queue, botUserId, now, log, classifyPeer, admission, runTurn, probe, react, alert, replyInPlace, inFlight = new Map() }) {
  const settlePromises = new Set(); // test-only visibility into in-flight settle() work; see idle()
  let highWaterAlerted = false;
  let ticking = false; // reentrancy guard: tick() must never run two claim passes concurrently

  // Best-effort reaction: never let a reaction failure (network blip, sync throw from a
  // misbehaving injected `react`, etc.) block the transition/reply/alert that must follow it.
  async function safeReact(item, verb, emoji) {
    try {
      await react(item, verb, emoji);
    } catch (err) {
      log(`reaction ${verb}:${emoji} failed for ${item.id}: ${String(err)}`);
    }
  }

  // Same contract as safeReact, for the ops alert: a failing alert channel must never
  // escape settle() (tick() fires it and forgets).
  async function safeAlert(text) {
    try {
      await alert(text);
    } catch (err) {
      log(`alert delivery failed: ${String(err)}`);
    }
  }

  async function policyVerdict(item) {
    if (item.firstSightBacklog) return { skip: "backlog" };
    const kind = item.kind === "conv" ? await classifyPeer(item) : "thread";
    if (kind !== "dm" && !contentMentionsBot(item.content, botUserId)) return { skip: "no-mention" };
    const adm = await admission(item);
    if (!adm.admit) return { skip: `admission:${adm.admission}` };
    return { commandAuthorized: adm.commandAuthorized };
  }

  /**
   * Terminal failure: the item will never be tried again. Loud on purpose — ❌ reaction,
   * an in-place apology to the sender, and an ops alert. Shared by the retries-exhausted
   * branch of settle() and the poison-orphan branch of recoverOrphans().
   */
  async function deadLetter(item, errText, attempts) {
    await safeReact(item, "add", "❌");
    await queue.transition(item.id, { state: "failed", lastError: errText }, now());
    await replyInPlace(item, "I hit an error answering this and have exhausted retries — it's been flagged for review.").catch(() => {});
    await alert(`twist queue: item ${item.id} (${item.peerId}) failed after ${attempts} attempts: ${errText.slice(0, 300)}`).catch(() => {});
  }

  // Bookkeeping ON TOP OF an already-failed turn: classify the error and record the verdict
  // (gone / dead-letter / retry with backoff). May throw — settle() contains it.
  async function recordFailure(item, err) {
    await safeReact(item, "remove", "⏳");
    if (isPermanentError(err)) {
      await queue.transition(item.id, { state: "skipped", reason: "gone", lastError: String(err) }, now());
      return;
    }
    const attempts = queue.get(item.id)?.attempts ?? item.attempts;
    if (attempts >= MAX_ATTEMPTS) {
      await deadLetter(item, String(err), attempts);
      return;
    }
    const delay = BACKOFF_MS[Math.min(attempts - 1, BACKOFF_MS.length - 1)];
    log(`turn failed for ${item.id} (attempt ${attempts}), retrying in ${delay}ms: ${String(err)}`);
    await queue.transition(item.id, { state: "queued", nextAttemptAt: now() + delay, lastError: String(err) }, now());
  }

  async function settle(item) {
    try {
      try {
        const verdict = await policyVerdict(item);
        if (verdict.skip) {
          // Every silently-dropped message is a support ticket waiting to happen: log the
          // reason (backlog / no-mention / admission:<verdict>) for ALL skips, not just denials.
          log(`skip ${item.id} (${item.peerId}): ${verdict.skip}`);
          await queue.transition(item.id, { state: "skipped", reason: verdict.skip }, now());
          return;
        }
        await safeReact(item, "add", "⏳");
        await runTurn(item, { commandAuthorized: verdict.commandAuthorized });
      } catch (err) {
        // None of this may throw out of settle(): settle() is fired-and-forgotten by tick(),
        // so an escaping rejection would become an unhandled rejection and (Node >=20) kill
        // the process. If persisting the verdict fails (e.g. disk full), log and leave the
        // item "processing" — boot recovery (recoverOrphans) picks it up on restart.
        try {
          await recordFailure(item, err);
        } catch (bookkeepingErr) {
          log(`settle bookkeeping failed for ${item.id} (${item.peerId}), leaving in "processing" for boot recovery: ${String(bookkeepingErr)}`);
        }
        return;
      }
      // THE TURN SUCCEEDED — the reply is already on Twist. Everything from here is
      // record-keeping, and it is deliberately NOT covered by the retry path above: if
      // marking the item done fails (disk full, etc.), requeueing it would re-run the turn
      // and post a SECOND reply. Leave it "processing" instead; boot recovery probes Twist,
      // finds our reply, and settles it as done without re-dispatching.
      try {
        await safeReact(item, "remove", "⏳");
        await safeReact(item, "add", "✅");
        await queue.transition(item.id, { state: "done" }, now());
      } catch (err) {
        log(`turn for ${item.id} (${item.peerId}) succeeded but recording it failed — leaving in "processing" for boot recovery (NOT retrying: the reply already went out): ${String(err)}`);
        await safeAlert(`twist queue: item ${item.id} (${item.peerId}) answered but could not be recorded done: ${String(err).slice(0, 300)}`);
      }
    } finally {
      inFlight.delete(item.peerId);
    }
  }

  return {
    inFlightCount: () => inFlight.size,
    // Test-only: await all settle() work currently tracked. NOT for production use —
    // if a claimed item's runTurn never resolves (e.g. hung upstream call), this hangs
    // forever. tick()'s own claim loop never awaits this.
    async idle() { await Promise.allSettled([...settlePromises]); },
    // Boot-only: sweeps items stuck in "processing" from a previous process's crash. Must
    // never run while tick()/settle() has turns in flight — it would race the same items'
    // reactions and transitions against the live consumer loop.
    async recoverOrphans() {
      for (const item of queue.itemsInState("processing")) {
        const answered = await probe(item).catch((err) => { log(`probe failed for ${item.id}: ${String(err)}`); return false; });
        // The original claim added ⏳; that reaction cycle never completed normally, so
        // clear it here regardless of outcome (otherwise it leaks forever on the done
        // branch, or gets double-added when the requeued item is retried).
        await safeReact(item, "remove", "⏳");
        if (answered) {
          await safeReact(item, "add", "✅");
          await queue.transition(item.id, { state: "done" }, now());
          log(`boot recovery: ${item.id} -> done (reply found)`);
          continue;
        }
        // Poison message: this item has burned every attempt and the process died mid-turn
        // again. Requeueing it unconditionally would re-claim it on every boot forever — a
        // crash loop that also blocks its peer. Dead-letter it like any other exhausted item.
        if ((item.attempts ?? 0) >= MAX_ATTEMPTS) {
          log(`boot recovery: ${item.id} -> dead-lettered (poison: ${item.attempts} attempts, process died mid-turn)`);
          await deadLetter(item, `orphaned mid-turn after ${item.attempts} attempts (process died during the turn)`, item.attempts);
          continue;
        }
        await queue.transition(item.id, { state: "queued", nextAttemptAt: 0 }, now());
        log(`boot recovery: ${item.id} -> requeued`);
      }
    },
    async tick() {
      if (ticking) return; // a concurrent tick() call is a no-op, not a second claim pass
      ticking = true;
      try {
        if (!highWaterAlerted && queue.nonTerminalCount() > HIGH_WATER) {
          highWaterAlerted = true;
          await alert(`twist queue: depth ${queue.nonTerminalCount()} exceeds high-water ${HIGH_WATER} — consumer may be wedged`).catch(() => {});
        }
        for (const [peerId, f] of inFlight) {
          if (!f.hungAlerted && now() - f.startedAt > HUNG_TURN_ALERT_MS) {
            f.hungAlerted = true;
            await alert(`twist queue: turn for ${f.id} (${peerId}) running > ${HUNG_TURN_ALERT_MS / 60000}min`).catch(() => {});
          }
        }
        while (true) {
          const item = queue.selectClaimable(now(), new Set(inFlight.keys()), MAX_GLOBAL_TURNS - inFlight.size);
          if (!item) break;
          // Reserve the peer slot BEFORE the (awaited) claim persist, so a claim that's slow
          // to persist can't let another loop iteration double-claim the same peer.
          inFlight.set(item.peerId, { id: item.id, startedAt: now(), hungAlerted: false });
          let claimed;
          try {
            claimed = await queue.transition(item.id, { state: "processing", claimedAt: now(), attempts: item.attempts + 1 });
          } catch (err) {
            inFlight.delete(item.peerId);
            log(`claim persist failed for ${item.id}: ${String(err)}`);
            await alert(`twist queue: claim persist failed for ${item.id} — stranded until restart`).catch(() => {});
            break; // don't keep hammering a store that's failing to persist this tick
          }
          const p = settle(claimed)
            .catch((err) => log(`settle crashed unexpectedly for ${claimed.id}: ${String(err)}`)) // belt-and-suspenders: settle() shouldn't reject, but tick() never awaits it, so guard anyway
            .finally(() => settlePromises.delete(p));
          settlePromises.add(p);
        }
      } finally {
        ticking = false;
      }
    },
  };
}
