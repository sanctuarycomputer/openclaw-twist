// Poll-cycle machinery — PURE, no SDK, no network, no ambient clock. Extracted from
// monitor.js so the parts that bound a cycle's cost are unit-testable (monitor.js imports
// the openclaw runtime store and cannot be loaded in this plugin's test environment).
//
// Both pieces exist for the same reason: a webhook hint is an ATTACKER-INFLUENCED trigger.
// Even fully authenticated (a leaked URL token, a compromised relay, or just Twist itself
// having a bad day), a flood of deliveries must not be able to starve the poll loop. The
// funnel bounds how much work a flood can queue, and the deadline bounds how long any one
// cycle can hold the exclusive chain.

/**
 * Ceiling on distinct containers held for the next cycle, and on how many one cycle will
 * sweep. 32 is far above steady-state (a busy workspace produces single digits per poll
 * interval) and far below anything that could make a cycle expensive.
 */
export const MAX_PENDING_HINTS = 32;

/** Hard ceiling on how long one cycle may hold the exclusive chain. */
export const CYCLE_DEADLINE_MS = 120_000;

/**
 * Ceiling on hint sweeps running concurrently OUTSIDE the exclusive chain.
 *
 * Bypassing the chain is what makes a hint fast, but it also removes the chain's implicit
 * "one Twist read at a time" bound, so a flood naming N distinct containers would otherwise
 * spawn N concurrent authenticated sweeps. The poll path sweeps containers strictly
 * serially, so 4 in flight is already 4x the historical API pressure; past that, hints fall
 * back to the chain-bound path (still targeted, still coalesced and capped by the funnel),
 * which is both cheaper and self-limiting.
 */
export const MAX_CONCURRENT_HINT_SWEEPS = 4;

/**
 * Collects hints between cycles and hands each cycle a bounded batch.
 *
 * The safety property that makes every overflow path trivially correct: **a full poll is a
 * strict superset of any set of targeted sweeps.** Dropping hints is therefore never
 * lossy — it is only ever a trade of precision for a bounded amount of work. So both caps
 * degrade the same way: throw the hints away, ask for a full poll instead.
 *
 * @param {object} [deps]
 * @param {number} [deps.maxPending]   cap on retained distinct containers
 * @param {number} [deps.maxPerCycle]  cap on containers swept in one cycle (same bound by default)
 * @param {(msg:string)=>void} [deps.log]
 */
export function createHintFunnel({ maxPending = MAX_PENDING_HINTS, maxPerCycle = maxPending, log } = {}) {
  const pending = new Map(); // "kind:id" -> hint
  let fullSweep = false;

  return {
    /**
     * @returns {"queued"|"full-sweep"|"overflow"}
     */
    push(hint) {
      // The reserved "all" hint is how an unrecognizable webhook payload asks for the
      // ordinary poll. It carries no container, so it never occupies a slot.
      if (hint?.kind === "all") {
        fullSweep = true;
        return "full-sweep";
      }
      pending.set(`${hint.kind}:${hint.id}`, hint);
      if (pending.size > maxPending) {
        pending.clear();
        fullSweep = true;
        log?.(
          `webhook hint funnel exceeded ${maxPending} pending containers — dropping targeted hints and falling back to a full poll (a full poll covers all of them)`,
        );
        return "overflow";
      }
      return "queued";
    },

    /** Ask the next cycle for a full poll regardless of hints (used by the "all" fallback). */
    requestFullSweep() {
      fullSweep = true;
    },

    /**
     * Take everything queued for the next cycle and reset.
     *
     * The `maxPerCycle` slice is belt-and-braces: `push` already caps the map, so under
     * normal operation it never truncates. It stands as an independent bound on the cycle
     * body's cost, so no future change to how hints get into the map can make a cycle
     * unbounded without also tripping this.
     *
     * @returns {{hints: object[], sweepAll: boolean}}
     */
    drain() {
      const all = [...pending.values()];
      pending.clear();
      const hints = all.slice(0, maxPerCycle);
      const truncated = hints.length < all.length;
      if (truncated) {
        log?.(`webhook hint batch truncated to ${maxPerCycle} containers — falling back to a full poll`);
      }
      const sweepAll = fullSweep || truncated;
      fullSweep = false;
      return { hints, sweepAll };
    },

    size: () => pending.size,
    fullSweepPending: () => fullSweep,
  };
}

/**
 * Compose the account's abort signal with a per-cycle deadline.
 *
 * Twist's REST client sets no socket timeout, so a hung connection would otherwise pin the
 * exclusive cycle chain forever and stop the bot answering anyone. The composed signal is
 * handed to the producer, so a stalling sweep is actively cancelled rather than merely
 * abandoned.
 *
 * `deadline` is returned separately so the caller can tell "we were shut down" (no loud
 * log — that is normal) from "Twist stalled past the deadline" (loud log).
 *
 * `any` / `timeout` are injected for testing and so a host missing `AbortSignal.any`
 * (Node < 20.3) degrades to the account signal alone rather than throwing at startup.
 *
 * @returns {{signal: AbortSignal|undefined, deadline: AbortSignal|null}}
 */
export function composeCycleSignal(
  accountSignal,
  {
    deadlineMs = CYCLE_DEADLINE_MS,
    timeout = typeof AbortSignal !== "undefined" ? AbortSignal.timeout : undefined,
    any = typeof AbortSignal !== "undefined" ? AbortSignal.any : undefined,
  } = {},
) {
  if (typeof timeout !== "function") return { signal: accountSignal, deadline: null };
  const deadline = timeout(deadlineMs);
  if (!accountSignal) return { signal: deadline, deadline };
  if (typeof any !== "function") return { signal: accountSignal, deadline: null };
  return { signal: any([accountSignal, deadline]), deadline };
}

/**
 * Runs hint sweeps immediately, off the exclusive cycle chain, one at a time per container.
 *
 * WHY BYPASS THE CHAIN AT ALL. Measured live, a DM ingested at 3.0s or 6.6s depending on
 * nothing but luck: a hint that arrived while a poll cycle was running had to wait for that
 * whole cycle before its own sweep could start. The sweep itself is a couple of API calls;
 * the variance was pure queueing. Hint sweeps therefore run as soon as they are flushed.
 *
 * WHY THAT IS SAFE against a poll-path sweep of the SAME container (verified against
 * producer.js / queue.js / state.js, not assumed):
 *
 *   1. NO DOUBLE-ENQUEUE, NO DOUBLE-ACK. `enqueueAcked` computes `fresh` with a
 *      synchronous `.filter(queue.has)` and then calls `queue.enqueueAll`, whose entire
 *      mutation loop over `data.items` runs before its first `await` (`persist()`). Filter
 *      and mutation are therefore one run-to-completion block with no interleaving point,
 *      so of two concurrent sweeps exactly one can see a given item as new — and it is the
 *      same one that inserts it. `fresh` (which drives fastAck) is thus disjoint between
 *      them. `enqueueAll` also re-checks membership itself, so the insert is idempotent
 *      even if the filter were wrong.
 *   2. CURSORS ONLY ADVANCE. `setCursor` writes only when `objIndex > cur`, so a slower
 *      sweep can never drag a cursor backwards. `persist` snapshots synchronously at call
 *      time and chains on `writeChain` in call order, so the last write always carries the
 *      newest state.
 *   3. MARK-READ IS A SET, NOT AN INCREMENT. Concurrent `markThreadRead` calls are
 *      last-write-wins on Twist's own read marker; the worst case is a thread lingering one
 *      extra poll in the unread list. Our cursor, not Twist's marker, is the refetch bound.
 *
 * (1) requires both paths to share ONE QUEUE STORE instance, since its atomicity is
 * atomicity over a shared `data.items` object. That holds: the queue store is memoized per
 * queue path (QUEUE_STORES in monitor.js) precisely so it survives an in-process channel
 * restart. The CURSOR store is NOT memoized — channel.js builds a fresh one per start — so
 * a hot reload can briefly run two stores over one cursors.json. Within a single monitor
 * both paths share one, so (2) holds for the concurrency introduced here; across a reload
 * the two stores can clobber each other's snapshot and REGRESS a cursor. That is non-lossy
 * by design: a cursor is only a refetch bound, so the cost is a redundant refetch whose
 * items the (shared, memoized) queue then dedups away.
 *
 * Producer *instances* may differ freely — they hold no mutable state of their own, only
 * read-only refs — which is what lets each sweep carry its own deadline signal.
 *
 * NOTE that (1) is a statement about in-memory bookkeeping, not durability. The matching
 * durability edge — never letting a cursor commit ahead of the queue write it depends on —
 * is `queue.whenPersisted()`, awaited in producer.enqueueAcked; see the comment there.
 *
 * The per-container guard is not a correctness requirement, then — it is a waste guard: a
 * second hint for a container already being swept is dropped because the running sweep
 * fetches CURRENT state and will therefore already see whatever that second event was about.
 *
 * @param {object} deps
 * @param {(hint:object)=>Promise<{swept:boolean, reason?:string}>} deps.sweep
 * @param {(hint:object)=>void} [deps.onSwept]    ran after a successful sweep (poke the chain to claim)
 * @param {(hint:object, reason:string)=>void} [deps.onDegrade]  sweep declined; caller falls back
 * @param {number} [deps.maxConcurrent]
 * @param {(msg:string)=>void} [deps.log]
 */
export function createHintSweeper({
  maxConcurrent = MAX_CONCURRENT_HINT_SWEEPS,
  sweep,
  onSwept,
  onDegrade,
  log,
} = {}) {
  if (typeof sweep !== "function") throw new Error("createHintSweeper: sweep is required");
  const inFlight = new Map(); // containerKey -> promise

  return {
    /**
     * @returns {"started"|"in-flight"|"at-capacity"}
     */
    dispatch(hint) {
      const key = `${hint.kind}:${hint.id}`;
      // The running sweep re-fetches current state, so it already covers this event.
      if (inFlight.has(key)) return "in-flight";
      if (inFlight.size >= maxConcurrent) {
        onDegrade?.(hint, "at-capacity");
        return "at-capacity";
      }
      const run = (async () => {
        // Yield once so the `inFlight.set` below has run before any path can delete the
        // key. Without it a synchronously-throwing `sweep` would reach `finally` first and
        // leave a stale entry that blocks this container forever.
        await Promise.resolve();
        let outcome;
        try {
          outcome = await sweep(hint);
        } catch (err) {
          log?.(`webhook sweep ${key} failed: ${String(err)}`);
        } finally {
          inFlight.delete(key);
        }
        // Inside a try of its own: these are host-supplied callbacks, and a throwing logger
        // or scheduler must not surface as an unhandled rejection (nobody awaits `run`
        // outside tests, so that would be a process-level crash on an unrelated bug).
        try {
          if (!outcome) return;
          if (outcome.swept === false) onDegrade?.(hint, outcome.reason ?? "declined");
          else onSwept?.(hint);
        } catch (err) {
          log?.(`webhook sweep ${key} follow-up failed: ${String(err)}`);
        }
      })();
      inFlight.set(key, run);
      return "started";
    },
    size: () => inFlight.size,
    /** Test-only: await every sweep currently in flight. */
    idle: () => Promise.allSettled([...inFlight.values()]),
  };
}

/** Returned by {@link settleWithinDeadline} when the deadline won the race. */
export const DEADLINE_EXPIRED = Symbol("deadline-expired");

/**
 * Race already-settled-shaped work against a deadline signal.
 *
 * A signal on its own is not enough to bound anything: it only cancels work that actually
 * threads it all the way down, and any un-signalled call (a third-party retry sleep, a
 * socket with no timeout) sails straight past it. This is the backstop that guarantees the
 * CALLER is released on time regardless, so a hung request can never hold a lock, a queue
 * slot, or a container guard for the life of the process.
 *
 * `work` must never reject — wrap it first. That keeps the abandoned branch from becoming
 * an unhandled rejection once we stop looking at it.
 *
 * @param {Promise<T>} work
 * @param {AbortSignal|null} [deadline]
 * @returns {Promise<T|typeof DEADLINE_EXPIRED>}
 * @template T
 */
export async function settleWithinDeadline(work, deadline) {
  if (!deadline) return await work;
  const blown = new Promise((resolve) => {
    if (deadline.aborted) resolve(DEADLINE_EXPIRED);
    else deadline.addEventListener("abort", () => resolve(DEADLINE_EXPIRED), { once: true });
  });
  return await Promise.race([work, blown]);
}

/**
 * Collapses repeated "please run a cycle" requests into at most ONE queued run.
 *
 * Every completed bypass sweep wants the chain to do a claim pass, but N sweeps do not need
 * N cycles — one claim pass after the last of them sees exactly the same queue. Worse,
 * queueing one per sweep lengthens the chain that the overflow path (which falls back to
 * chain-bound sweeps) has to wait behind, so the fast path would starve the slow path it is
 * supposed to be relieving. One outstanding poke keeps the chain short.
 *
 * `starting()` is called when the queued cycle BEGINS, not when it ends: work that landed
 * while it was queued is already covered by it, but work landing after it starts needs a
 * fresh poke.
 *
 * @param {() => void} schedule
 */
export function createPokeGate(schedule) {
  if (typeof schedule !== "function") throw new Error("createPokeGate: schedule is required");
  let pending = false;
  return {
    /** @returns {"queued"|"already-queued"} */
    request() {
      if (pending) return "already-queued";
      pending = true;
      schedule();
      return "queued";
    },
    /** The queued cycle has begun; later requests must queue a new one. */
    starting() {
      pending = false;
    },
    pending: () => pending,
  };
}
