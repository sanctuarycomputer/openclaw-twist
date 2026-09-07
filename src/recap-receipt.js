// Receipt for a suppressed cron meta-recap.
//
// The outbound adapter drops a header-less final message that only recaps "already
// posted" (see isCronMetaRecap in routing.js). openclaw counts a cron delivery as
// delivered only when the channel returns at least one message id
// (run-delivery: `delivered = deliveryResults.length > 0`), so a bare suppression is
// recorded as deliveryStatus "not-delivered" even though the report is sitting in the
// thread. Live 2026-09-07: System Health Review 12:20 and Observe: GitHub Code 12:40
// both tool-posted to thread 7882650, both logged as silent successes for that reason.
//
// The truthful receipt for a suppressed recap is the bot's own post it recaps: look it
// up and hand THAT id back. No such post → the recap's claim is unverified → suppress
// without a receipt, so the ledger keeps saying not-delivered (which is then correct).
// This module has no openclaw-SDK imports so it is testable in the plugin's own suite.

/** How far back a bot post still counts as "the deliverable this recap refers to". */
export const RECENT_SELF_POST_WINDOW_MS = 60 * 60 * 1000;
// Tolerate a little clock skew between Twist and the gateway.
const FUTURE_SKEW_MS = 5 * 60 * 1000;

/**
 * Newest item authored by the bot within the window, as { messageId, postedMs }, or
 * null. Items are Twist comments / conversation messages ({ id, creator, posted_ts }).
 * Pure: no I/O, no clock reads unless nowMs is omitted.
 */
export function newestSelfPost(items, { botUserId, nowMs = Date.now(), windowMs = RECENT_SELF_POST_WINDOW_MS } = {}) {
  if (!Array.isArray(items) || botUserId == null || botUserId === "") return null;
  const bot = String(botUserId);
  let best = null;
  for (const it of items) {
    if (!it || it.id == null || String(it.creator) !== bot) continue;
    const postedMs = Number(it.posted_ts) * 1000;
    if (!Number.isFinite(postedMs)) continue;
    if (nowMs - postedMs > windowMs || postedMs - nowMs > FUTURE_SKEW_MS) continue;
    if (!best || postedMs > best.postedMs) best = { messageId: String(it.id), postedMs };
  }
  return best;
}

/**
 * Look up the bot's newest recent post in a thread ("thread") or conversation
 * ("conv"). Any API error resolves to null — a lookup failure must never turn into a
 * fabricated receipt, and never into a thrown delivery error either.
 */
export async function findRecentSelfPost(client, { kind, id, botUserId, nowMs, windowMs, limit = 10 } = {}) {
  if (!client || id == null) return null;
  try {
    const items =
      kind === "thread"
        ? await client.getThreadComments(id, { limit })
        : await client.getConversationMessages(id, { limit });
    return newestSelfPost(items, { botUserId, nowMs, windowMs });
  } catch {
    return null;
  }
}
