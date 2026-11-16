// Loop breaker: stop a fast loop with a counterpart the bot-sender guard cannot see. A
// bot that is not flagged `bot: true` (another integration posting as a normal user, a
// person's auto-responder) answers every reply, and the bot answers every message, so the
// pair runs as fast as a turn takes. The Spend Guard catches that from cost, but it runs
// about once a day; Ada and the bot traded ~748k messages over 40 days (~$28k) before
// anything stopped them. This counts dispatched turns per peer and, past a rate no person
// sustains, mutes the peer in the shared mute file (mutes.js) and tells Hugh directly.
//
// State is in memory: a restart resets the windows, which costs at most one more window of
// turns. The mute itself persists, because it is in the file.
import { ALERT_HEADER } from "./consumer.js";
import { DEFAULT_NEVER_MUTE, isLive, peerTarget } from "./mutes.js";
import { appendUrgentWake, subjectOfPeer } from "./urgent-wakes.js";

export const LOOP_BREAKER_BY = "twist-loop-breaker";
export const DEFAULT_LOOP_BREAKER = Object.freeze({ enabled: true, maxTurns: 40, windowMinutes: 30, muteHours: 6 });
// Hugh does not read the ops thread; a Critical goes to his DM with the bot.
export const DEFAULT_CRITICAL_TO = "conv:1367817";

const positive = (v, dflt) => (Number.isFinite(Number(v)) && Number(v) > 0 ? Number(v) : dflt);

/**
 * `channels.twist` → the loop guard settings, defaults applied. Lives here rather than in
 * config.js because config.js imports the openclaw SDK and cannot load under `npm test`.
 */
export function resolveLoopGuardConfig(section = {}) {
  const lb = section.loopBreaker ?? {};
  return {
    neverMute: Array.isArray(section.neverMute) ? section.neverMute.map(String) : [...DEFAULT_NEVER_MUTE],
    criticalTo: typeof section.criticalTo === "string" && section.criticalTo.trim() ? section.criticalTo.trim() : DEFAULT_CRITICAL_TO,
    loopBreaker: {
      enabled: lb.enabled !== false,
      maxTurns: positive(lb.maxTurns, DEFAULT_LOOP_BREAKER.maxTurns),
      windowMinutes: positive(lb.windowMinutes, DEFAULT_LOOP_BREAKER.windowMinutes),
      muteHours: positive(lb.muteHours, DEFAULT_LOOP_BREAKER.muteHours),
    },
  };
}

const utc = (ms) => `${new Date(ms).toISOString().slice(0, 16).replace("T", " ")} UTC`;

/** Plain words for where the loop is, for a reader who has never seen a peer id. */
function where({ peerId, kind, senderName }) {
  const id = peerTarget(peerId)?.id ?? peerId;
  if (kind === "dm") return `my DM with ${senderName || "someone"} (conversation ${id})`;
  return kind === "groupdm" ? `a group DM (conversation ${id})` : `thread ${id}`;
}

/**
 * The Critical alert, one per trip. Says where, who, how fast, what was done and how to undo
 * it, in that order (Stacksbot reader contract: every alert says who acts and how).
 * @param {{peerId:string, kind?:string, senderName?:string, turns:number, minutes:number,
 *   outcome:"muted"|"memory-only"|"protected"|"held", untilMs?:number, why?:string, heldSince?:string}} t
 */
export function loopAlertText(t) {
  const id = peerTarget(t.peerId)?.id ?? t.peerId;
  // In a DM the other side is in the place name; elsewhere it is whoever posted last.
  const latest = t.kind === "dm" ? "" : ` The latest was from ${t.senderName || "someone"}.`;
  const head = `${ALERT_HEADER}\nCritical: ${where(t)} looks like a loop. I answered ${t.turns} messages there in ${t.minutes} minutes, faster than any person writes.${latest}`;
  const undo = `To undo it sooner, delete its entry ("${id}") in twist-muted.json, or run \`node scripts/twist-mute.mjs unmute ${id}\` in the stacksbot repo on the Render shell.`;
  if (t.outcome === "muted") return `${head} I've stopped answering it until ${utc(t.untilMs)}. ${undo}`;
  if (t.outcome === "memory-only") {
    return `${head} I've stopped answering it until ${utc(t.untilMs)} or the next restart, whichever comes first. I couldn't record it in twist-muted.json (${t.why}), so restarting the service on Render undoes it.`;
  }
  if (t.outcome === "held") {
    return `${head} I'm still answering it because someone unmuted it at ${t.heldSince}. If it is a loop, mute it again: set its entry's "until" to null in twist-muted.json.`;
  }
  return `${head} I'm still answering it because it is on the never-mute list (channels.twist.neverMute). If it is a loop, restart the service on Render.`;
}

/**
 * The Critical alert sender. Posts to `criticalTo` and returns the post's id, and whether that
 * target is a conversation (Hugh's DM), so the loop breaker's wake can tell Stacksbot's Urgent
 * runner which message to adopt. If that post fails (or the target can't be resolved) the text
 * still goes out through the ordinary alert path, the ops thread, and the result is null: no DM.
 * @param {object} deps
 * @param {(text: string) => Promise<{messageId?: string, kind?: string}>} deps.post
 * @param {(text: string) => Promise<void>} deps.fallback
 * @param {(line: string) => void} deps.log
 * @param {string} deps.to  the configured target, for the log line
 * @returns {(text: string) => Promise<{messageId: string|undefined, dm: boolean} | null>}
 */
export function createCriticalNotifier({ post, fallback, log, to }) {
  return async (text) => {
    try {
      const res = await post(text);
      return { messageId: res?.messageId, dm: res?.kind === "conv" };
    } catch (err) {
      log(`critical alert to ${to} failed, sending it to the ops thread instead: ${String(err)}`);
      await fallback(text);
      return null;
    }
  };
}

/**
 * @param {object} deps
 * @param {ReturnType<typeof import("./mutes.js").createMuteList>} deps.mutes
 * @param {(text: string) => Promise<{messageId?: string, dm?: boolean} | null | void>} deps.notify
 *   posts the Critical alert; must not throw
 * @param {{enabled:boolean, maxTurns:number, windowMinutes:number, muteHours:number}} deps.config
 * @param {object} [deps.env] where the Urgent wake goes ($OPENCLAW_STATE_DIR); process.env by default
 * @param {typeof appendUrgentWake} [deps.appendWake] injectable for tests
 */
export function createLoopBreaker({ mutes, notify, config = DEFAULT_LOOP_BREAKER, log, now = Date.now, env = process.env, appendWake = appendUrgentWake }) {
  const windowMs = config.windowMinutes * 60_000;
  const muteMs = config.muteHours * 3600_000;
  const turns = new Map(); // peerId -> dispatch times inside the window, oldest first
  const alertedAt = new Map(); // peerId -> last alert, for peers that trip but keep being answered

  /** Alert-only outcomes (protected, held) re-trip every window; one alert per peer per muteHours. */
  function alertDue(peerId) {
    if (now() - (alertedAt.get(peerId) ?? -Infinity) < muteMs) return false;
    alertedAt.set(peerId, now());
    return true;
  }

  /**
   * Did a person cut this breaker's own mute short? `unmute` sets `until` to its moment, so an
   * entry of ours that ended well before mutedAt + muteHours was ended by hand. That holds for
   * muteHours from the unmute: re-muting at once would make the unmute pointless.
   */
  function heldSince(peerId) {
    const e = mutes.rawEntry(peerId);
    if (!e || e.by !== LOOP_BREAKER_BY || isLive(e, now())) return null;
    const until = Date.parse(e.until);
    const cutShort = until < Date.parse(e.mutedAt) + muteMs - 60_000;
    return cutShort && now() - until < muteMs ? e.until : null;
  }

  /**
   * Send the Critical, then leave Stacksbot's Urgent runner a wake (signal 8) naming it, so the
   * runner adopts this DM as the incident's thread instead of sending its own. The lever is
   * what this trip did: mute-conversation only when it wrote the mute to the file (the runner's
   * undo removes that entry); otherwise propose, nothing to undo. Nothing in the wake path can
   * change the trip's outcome: a fault there is logged and dropped.
   */
  async function alertAndWake(text, facts, outcome, written = false) {
    const sent = await notify(text);
    try {
      const at = now(); // one clock read: the id, the time and the day file agree
      const subject = subjectOfPeer(facts.peerId);
      const target = peerTarget(facts.peerId);
      const did = written && target;
      const dmPosted = Boolean(sent?.dm && sent?.messageId);
      const ok = appendWake(
        {
          id: `8:${subject}:${at}`,
          at: new Date(at).toISOString(),
          source: "floor",
          signal: 8,
          subject,
          dm_posted: dmPosted,
          message_id: dmPosted ? sent.messageId : null,
          facts: {
            by: LOOP_BREAKER_BY,
            title: `Loop in ${where(facts)}`,
            what: `I answered ${facts.turns} messages there in ${facts.minutes} minutes.`,
            lever: did ? "mute-conversation" : "propose",
            lever_args: did ? (target.bucket === "threads" ? { thread: target.id } : { conversation: target.id }) : {},
            turns: facts.turns,
            minutes: facts.minutes,
            outcome,
          },
        },
        { env, nowMs: at },
      );
      if (!ok) log(`urgent wake not written for ${facts.peerId}`);
    } catch (err) {
      log(`urgent wake not written for ${facts.peerId}: ${String(err)}`);
    }
  }

  async function trip(item, kind, times) {
    const peerId = String(item.peerId);
    const facts = { peerId, kind, senderName: item.senderName, turns: times.length, minutes: Math.max(1, Math.ceil((now() - times[0]) / 60_000)) };
    turns.delete(peerId); // a trip starts a fresh window: the next one needs another maxTurns+1
    if (mutes.isProtected(peerId)) {
      log(`loop breaker: ${peerId} tripped (${facts.turns} turns in ${facts.minutes}min) but is in neverMute: alert only`);
      if (alertDue(peerId)) await alertAndWake(loopAlertText({ ...facts, outcome: "protected" }), facts, "protected");
      return true;
    }
    const held = heldSince(peerId);
    if (held) {
      log(`loop breaker: ${peerId} tripped (${facts.turns} turns in ${facts.minutes}min) but was unmuted by hand at ${held}: alert only`);
      if (alertDue(peerId)) await alertAndWake(loopAlertText({ ...facts, outcome: "held", heldSince: held }), facts, "held");
      return true;
    }
    const untilMs = now() + muteMs;
    const res = mutes.mute(peerId, {
      kind: kind ?? (item.kind === "conv" ? "dm" : "thread"),
      reason: `loop breaker: ${facts.turns} turns in ${facts.minutes} min (limit ${config.maxTurns} per ${config.windowMinutes} min); last sender ${item.senderName ?? item.senderId}`,
      mutedAt: new Date(now()).toISOString(),
      until: new Date(untilMs).toISOString(),
      by: LOOP_BREAKER_BY,
    });
    log(`loop breaker: muted ${peerId} until ${new Date(untilMs).toISOString()} (${facts.turns} turns in ${facts.minutes}min)${res.written ? "" : `; not written to the file: ${res.why}`}`);
    const alreadyMuted = !res.written && /^already muted/.test(res.why ?? "");
    const outcome = res.written || alreadyMuted ? "muted" : "memory-only";
    await alertAndWake(loopAlertText({ ...facts, outcome, untilMs, why: res.why }), facts, outcome, res.written === true);
    return false;
  }

  return {
    /**
     * Record one turn about to be dispatched for this item's peer. → true to dispatch, false
     * when this turn tripped the breaker and the peer is now muted. Never throws: a fault in
     * the guard logs and lets the turn through, so a bug here can't stop the bot answering.
     * @param {object} item queue item
     * @param {{kind?: string}} [opts] the classified peer kind ("dm" | "groupdm" | "thread")
     */
    async admit(item, { kind } = {}) {
      if (!config.enabled) return true;
      try {
        const peerId = String(item.peerId);
        const times = (turns.get(peerId) ?? []).filter((t) => now() - t < windowMs);
        times.push(now());
        turns.set(peerId, times);
        if (times.length <= config.maxTurns) return true;
        return await trip(item, kind, times);
      } catch (err) {
        log(`loop breaker failed for ${item?.id} (letting the turn through): ${String(err)}`);
        return true;
      }
    },
  };
}
