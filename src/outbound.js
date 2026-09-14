// Outbound delivery to Twist. Used both by the inbound dispatch reply path and
// by the channel's outbound adapter (the `message` tool). A target is encoded as
// "thread:<id>" or "conv:<id>" (optionally prefixed "twist:").
import { createTwistClient } from "./twist-client.js";
import { resolveTwistAccount } from "./config.js";
import { parseTarget, resolveOutboundTarget, channelDefaultRecipients, threadFallbackAudience, stripPreHeaderNarration, isBareCronFailureAlert, isCronMetaRecap } from "./routing.js";
import { findRecentSelfPost } from "./recap-receipt.js";

export { parseTarget };

// Cache: channelId → { recipients: Array|null, expiresAt: number }
const CHANNEL_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const _channelCache = new Map();

/** Channel default recipients for a channel id, cached (TTL 5 min); null when none configured. */
async function cachedChannelDefaults(client, channelId) {
  const cached = _channelCache.get(String(channelId));
  if (cached && Date.now() < cached.expiresAt) return cached.recipients;
  const channel = await client.getChannel(channelId);
  const recipients = channelDefaultRecipients(channel);
  _channelCache.set(String(channelId), { recipients, expiresAt: Date.now() + CHANNEL_CACHE_TTL_MS });
  return recipients;
}

/**
 * Who a post with nothing to mirror should notify, for a thread: the channel's configured
 * default recipients when it has them, else the thread's own audience (see
 * threadFallbackAudience) — never Twist's EVERYONE_IN_THREAD default. Returns null only
 * when even the thread cannot be read; the caller then omits the field and logs it.
 */
async function resolveThreadAudience(client, threadId, botUserId) {
  let thread;
  try {
    thread = await client.getThread(threadId);
  } catch (err) {
    console.warn(`[twist] thread ${threadId} unreadable while resolving the audience (${String(err).slice(0, 80)}) — posting with Twist's default`);
    return null;
  }
  if (thread?.channel_id) {
    try {
      const defaults = await cachedChannelDefaults(client, thread.channel_id);
      if (defaults) return { recipients: defaults, groups: [] };
    } catch {
      // channel unreadable — the thread's own audience still applies
    }
  }
  return threadFallbackAudience(thread, botUserId);
}

/**
 * Post text to a Twist thread or conversation. Returns { messageId } — or
 * { messageId: undefined, suppressed: true } for the redundant bare cron
 * failure alert (see isBareCronFailureAlert). Suppressions and strips are
 * logged loudly (console → gateway logs) so a wrongly-eaten message is
 * diagnosable, never a silent black hole.
 */
export async function postToTwist({ client, kind, id, text, audience, botUserId }) {
  if (isBareCronFailureAlert(text)) {
    console.warn(`[twist] suppressed bare cron failure alert to ${kind}:${id} (redundant with Job Failure Alert): ${String(text).slice(0, 160)}`);
    return { messageId: undefined, suppressed: true };
  }
  const body = stripPreHeaderNarration(text);
  if (body !== text) {
    console.warn(`[twist] stripped ${text.length - body.length} chars of pre-header narration from post to ${kind}:${id}`);
  }
  if (kind === "thread") {
    // `audience` mirrors the triggering post (see replyAudience); undefined means there is
    // nothing to mirror — a cron delivery or an agent-to-agent announce — so the channel's
    // configured defaults or the thread's own audience apply (resolveThreadAudience), never
    // Twist's EVERYONE_IN_THREAD default.
    const resolved = audience !== undefined ? audience : await resolveThreadAudience(client, id, botUserId);
    if (audience === undefined) {
      console.warn(`[twist] agent-initiated post to thread:${id} — audience ${resolved ? `recipients=${JSON.stringify(resolved.recipients)} groups=${JSON.stringify(resolved.groups)}` : "Twist default (thread unreadable)"}`);
    }
    const res = await client.addThreadComment(id, body, resolved ? { recipients: resolved.recipients, groups: resolved.groups } : {});
    return { messageId: res?.id != null ? String(res.id) : undefined };
  }
  const res = await client.addConversationMessage(id, body);
  return { messageId: res?.id != null ? String(res.id) : undefined };
}

/** Build a TwistClient from the current config. */
export function clientFromConfig(cfg) {
  const account = resolveTwistAccount(cfg);
  if (!account.configured) throw new Error("twist: not configured (token/workspaceId/botUserId)");
  return { client: createTwistClient({ token: account.token, workspaceId: account.workspaceId }), account };
}

/** Outbound adapter shape for createChatChannelPlugin (message tool path). */
export const twistOutbound = {
  base: {
    deliveryMode: "direct",
    chunkerMode: "markdown",
    // NOTE: inert in openclaw 2026.7.1-2 — no `chunker` fn is set and core doesn't
    // default one, so sendText receives the FULL text in one unit. Kept as intent
    // for when a chunker is wired; multi-part delivery today only happens via the
    // inbound block dispatcher (one postToTwist per block).
    textChunkLimit: 9000,
  },
  attachedResults: {
    channel: "twist",
    sendText: async ({ cfg, to, text }) => {
      const { client, account } = clientFromConfig(cfg);
      const { kind, id } = resolveOutboundTarget(to, account.defaultTo);
      // Cron announce deliveries arrive here (the inbound reply path calls postToTwist
      // directly, so an interactive answer is never subject to this check). A final
      // message that only recaps "already posted" duplicates a report the agent
      // tool-posted moments earlier — drop it, loudly. openclaw records the run as
      // delivered only if we return a message id, so hand back the id of the bot's own
      // recent post in the target: that IS the deliverable the recap refers to. When no
      // such post exists the claim is unverified and the run stays "not-delivered".
      if (isCronMetaRecap(text)) {
        const prior = await findRecentSelfPost(client, { kind, id, botUserId: account.botUserId });
        if (prior) {
          console.warn(`[twist] suppressed cron meta-recap to ${kind}:${id} (deliverable already tool-posted as ${prior.messageId}): ${String(text).slice(0, 160)}`);
          return { messageId: prior.messageId, suppressed: true };
        }
        console.warn(`[twist] suppressed cron meta-recap to ${kind}:${id} (no recent bot post found — claim unverified, run reported not-delivered): ${String(text).slice(0, 160)}`);
        return { messageId: undefined, suppressed: true };
      }
      return await postToTwist({ client, kind, id, text, botUserId: account.botUserId });
    },
  },
};
