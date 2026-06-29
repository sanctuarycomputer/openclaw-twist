// Outbound delivery to Twist. Used both by the inbound dispatch reply path and
// by the channel's outbound adapter (the `message` tool). A target is encoded as
// "thread:<id>" or "conv:<id>" (optionally prefixed "twist:").
import { createTwistClient } from "./twist-client.js";
import { resolveTwistAccount } from "./config.js";
import { parseTarget, resolveOutboundTarget, channelDefaultRecipients } from "./routing.js";

export { parseTarget };

// Cache: channelId → { recipients: Array|null, expiresAt: number }
const CHANNEL_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const _channelCache = new Map();

/**
 * Resolve the channel's default recipients for a given thread id.
 * Caches by channel id (TTL 5 min). Returns null on any error or when the
 * channel has no default recipients configured — falls back to Twist default.
 */
async function resolveThreadRecipients(client, threadId) {
  try {
    const thread = await client.getThread(threadId);
    const channelId = thread?.channel_id;
    if (!channelId) return null;

    const cached = _channelCache.get(String(channelId));
    if (cached && Date.now() < cached.expiresAt) return cached.recipients;

    const channel = await client.getChannel(channelId);
    const recipients = channelDefaultRecipients(channel);
    _channelCache.set(String(channelId), { recipients, expiresAt: Date.now() + CHANNEL_CACHE_TTL_MS });
    return recipients;
  } catch {
    return null; // tolerate API errors — fall back to Twist default
  }
}

/** Post text to a Twist thread or conversation. Returns { messageId }. */
export async function postToTwist({ client, kind, id, text, recipients }) {
  if (kind === "thread") {
    const resolved = recipients !== undefined ? recipients : await resolveThreadRecipients(client, id);
    const res = await client.addThreadComment(id, text, { recipients: resolved });
    return { messageId: res?.id != null ? String(res.id) : undefined };
  }
  const res = await client.addConversationMessage(id, text);
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
    textChunkLimit: 9000, // Twist messages are generous; keep well under any cap
  },
  attachedResults: {
    channel: "twist",
    sendText: async ({ cfg, to, text }) => {
      const { client, account } = clientFromConfig(cfg);
      const { kind, id } = resolveOutboundTarget(to, account.defaultTo);
      return await postToTwist({ client, kind, id, text });
    },
  },
};
