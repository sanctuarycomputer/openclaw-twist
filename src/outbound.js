// Outbound delivery to Twist. Used both by the inbound dispatch reply path and
// by the channel's outbound adapter (the `message` tool). A target is encoded as
// "thread:<id>" or "conv:<id>" (optionally prefixed "twist:").
import { createTwistClient } from "./twist-client.js";
import { resolveTwistAccount } from "./config.js";
import { parseTarget, resolveOutboundTarget } from "./routing.js";

export { parseTarget };

/** Post text to a Twist thread or conversation. Returns { messageId }. */
export async function postToTwist({ client, kind, id, text }) {
  if (kind === "thread") {
    const res = await client.addThreadComment(id, text);
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
