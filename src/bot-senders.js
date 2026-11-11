// Bot-sender guard: never answer another bot. Two bots that each reply to everything the
// other posts never stop — Twist's built-in "Ada Bot" auto-replies to every DM message,
// and from 2026-10-02 it and Stacksbot traded ~748k messages in one DM before anyone
// noticed. Self-post exclusion (isSelfAuthored) cannot catch this; only the sender's
// Twist `bot` flag can.

// Ada Bot, Twist's help/billing bot. Denied by id regardless of what the user lookup says,
// so the one bot we know loops is blocked even when that lookup is down.
export const ADA_BOT_USER_ID = 2601;
const HARD_DENIED = new Set([String(ADA_BOT_USER_ID)]);

export const USERS_TTL_MS = 6 * 3600_000; // bot flags almost never change
export const MISS_REFRESH_MS = 10 * 60_000; // an unknown sender may refresh early, this often at most
export const DROP_LOG_MS = 60_000; // one drop line per sender per minute; the rest are counted

/**
 * @param {object} deps
 * @param {{getWorkspaceUsers: (signal?: AbortSignal) => Promise<any>}} deps.client
 * @param {boolean} [deps.allowBots] `channels.twist.allowBots`: true answers bots again
 *   (the hard-denied ids stay denied).
 *
 * A failed lookup keeps the last good set and treats unknown senders as human: failing
 * closed would silence every person in the workspace whenever `workspaces/get_users` blips,
 * and the bot we know loops is denied by id anyway.
 */
export function createBotSenders({ client, log, now = Date.now, allowBots = false }) {
  let botIds = new Set();
  let knownIds = new Set();
  let fetchedAt = -Infinity; // last SUCCESSFUL lookup
  let attemptedAt = -Infinity; // last lookup of any outcome
  let inFlight = null;
  const drops = new Map(); // senderId -> {loggedAt, suppressed}

  async function refresh(signal) {
    attemptedAt = now();
    try {
      const res = await client.getWorkspaceUsers(signal);
      const users = Array.isArray(res) ? res : res?.users ?? [];
      botIds = new Set(users.filter((u) => u?.bot).map((u) => String(u.id)));
      knownIds = new Set(users.map((u) => String(u?.id)));
      fetchedAt = now();
    } catch (err) {
      // Retried after MISS_REFRESH_MS (see prime): not every poll while the endpoint is down,
      // and not 6h later either, so one aborted cycle can't hide every bot but Ada for hours.
      log(`bot-sender lookup failed (keeping ${botIds.size} known bot(s); ${ADA_BOT_USER_ID} still denied): ${String(err)}`);
    }
  }

  return {
    /** Make sure the bot set is fresh enough to judge these senders. Never throws. */
    async prime(senderIds = [], signal) {
      if (allowBots) return;
      const due = now() - fetchedAt >= USERS_TTL_MS || senderIds.some((id) => !knownIds.has(String(id)));
      if (!due || now() - attemptedAt < MISS_REFRESH_MS) return;
      inFlight ??= refresh(signal).finally(() => { inFlight = null; });
      await inFlight;
    },

    isBot(senderId) {
      const id = String(senderId);
      return HARD_DENIED.has(id) || (!allowBots && botIds.has(id));
    },

    /** One log line per sender per DROP_LOG_MS, carrying how many drops it stood for. */
    noteDropped(item) {
      const key = String(item.senderId);
      const d = drops.get(key) ?? { loggedAt: -Infinity, suppressed: 0 };
      drops.set(key, d);
      if (now() - d.loggedAt < DROP_LOG_MS) { d.suppressed++; return; }
      const more = d.suppressed ? ` (+${d.suppressed} more since the last line)` : "";
      log(`dropped ${item.id} from bot ${item.senderName ?? key} (${key}) in ${item.peerId}: bots are never answered${more}`);
      d.loggedAt = now();
      d.suppressed = 0;
    },
  };
}
