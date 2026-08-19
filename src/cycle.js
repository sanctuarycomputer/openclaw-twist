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
