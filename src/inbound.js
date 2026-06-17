// Inbound handler: takes a normalized Twist message, runs OpenCLAW's ingress
// gate (DM policy / group mention), routes it to a session, dispatches an agent
// turn, and delivers the reply back to Twist. Mirrors the bundled IRC channel's
// handleIrcInbound, adapted for Twist's poll model.
import {
  createChannelIngressResolver,
  defineStableChannelIngressIdentity,
} from "openclaw/plugin-sdk/channel-ingress-runtime";
import { resolveInboundRouteEnvelopeBuilderWithRuntime } from "openclaw/plugin-sdk/inbound-envelope";
import { getTwistRuntime } from "./runtime.js";
import { resolveRequireMention } from "./config.js";
import { contentMentionsBot, cleanTwistMarkup } from "./routing.js";
import { postToTwist } from "./outbound.js";

const CHANNEL_ID = "twist";

// Render the surrounding Twist context (thread title, channel, prior comments)
// so the agent operates with full context, not just the bare mention.
function buildTwistContextBlock(message) {
  const lines = [];
  if (message.kind === "thread") {
    const where = message.channelName ? ` in #${message.channelName}` : "";
    lines.push(`[Twist thread: "${message.threadTitle ?? "(untitled)"}"${where} · thread_id ${message.threadId}]`);
  } else if (message.kind === "groupdm") {
    lines.push(`[Twist group conversation · conversation_id ${message.conversationId}]`);
  }
  const transcript = message.transcript ?? [];
  if (transcript.length) {
    lines.push("", "Conversation so far:");
    for (const t of transcript) lines.push(`${t.name}: ${cleanTwistMarkup(t.content)}`);
  }
  return lines.length ? lines.join("\n") : "";
}

const twistIngressIdentity = defineStableChannelIngressIdentity({
  key: "twist-id",
  normalizeEntry: (v) => {
    const s = String(v ?? "").trim();
    return s && s !== "*" ? s : null;
  },
  normalizeSubject: (v) => String(v ?? "").trim(),
  isWildcardEntry: (e) => String(e ?? "").trim() === "*",
  sensitivity: "pii",
});

/**
 * @param {object} p
 * @param {object} p.message normalized: {messageId,kind,conversationId,threadId,groupId,peerKind,peerId,isGroup,senderId,senderName,text,timestamp,directMention}
 * @param {object} p.account resolved Twist account
 * @param {object} p.cfg     live OpenCLAW config
 * @param {object} p.runtime logger-backed runtime
 * @param {object} p.client  TwistClient (for delivery)
 * @param {(u:object)=>void} [p.statusSink]
 */
export async function handleTwistInbound({ message, account, cfg, runtime, client, statusSink }) {
  const core = getTwistRuntime();
  const rawBody = cleanTwistMarkup((message.text ?? "").trim());
  if (!rawBody) return;
  statusSink?.({ lastInboundAt: message.timestamp });

  const dmPolicy = account.config.dmPolicy ?? "open";
  const groupPolicy = account.config.groupPolicy ?? "open";
  const allowTextCommands =
    core.channel?.commands?.shouldHandleTextCommands?.({ cfg, surface: CHANNEL_ID }) ?? false;
  const hasControlCommand = core.channel?.text?.hasControlCommand?.(rawBody, cfg) ?? false;

  const wasMentioned = Boolean(message.directMention) || contentMentionsBot(rawBody, account.botUserId);
  const requireMention = message.isGroup ? resolveRequireMention(account, message.groupId) : false;

  const access = await createChannelIngressResolver({
    channelId: CHANNEL_ID,
    accountId: account.accountId,
    identity: twistIngressIdentity,
    cfg,
    readStoreAllowFrom: async () => [],
  }).message({
    subject: { stableId: String(message.senderId) },
    conversation: { kind: message.isGroup ? "group" : "direct", id: message.peerId },
    route: [],
    mentionFacts: message.isGroup
      ? { canDetectMention: true, wasMentioned, hasAnyMention: wasMentioned }
      : undefined,
    dmPolicy,
    groupPolicy,
    policy: {
      groupAllowFromFallbackToAllowFrom: false,
      activation: { requireMention: message.isGroup && requireMention, allowTextCommands },
    },
    allowFrom: account.config.allowFrom,
    groupAllowFrom: account.config.groupAllowFrom,
    command: { allowTextCommands, hasControlCommand },
  });

  if (access.ingress.admission !== "dispatch") {
    runtime.log?.(
      `twist: drop ${message.kind} ${message.peerId} (admission=${access.ingress.admission})`,
    );
    return;
  }
  const commandAuthorized = access.commandAccess?.authorized ?? false;

  const { route, buildEnvelope } = resolveInboundRouteEnvelopeBuilderWithRuntime({
    cfg,
    channel: CHANNEL_ID,
    accountId: account.accountId,
    peer: { kind: message.peerKind, id: message.peerId },
    runtime: core.channel,
    sessionStore: cfg.session?.store,
  });

  const fromLabel = message.senderName || String(message.senderId);
  const contextBlock = buildTwistContextBlock(message);
  const bodyText = contextBlock ? `${contextBlock}\n\nNew message from ${fromLabel}:\n${rawBody}` : rawBody;
  const { storePath, body } = buildEnvelope({
    channel: "Twist",
    from: fromLabel,
    timestamp: message.timestamp,
    body: bodyText,
  });

  const ctxPayload = core.channel.reply.finalizeInboundContext({
    Body: body,
    RawBody: rawBody,
    CommandBody: rawBody,
    From: `twist:${message.peerId}`,
    To: `twist:${message.peerId}`,
    SessionKey: route.sessionKey,
    AccountId: route.accountId,
    ChatType: message.isGroup ? "group" : "direct",
    ConversationLabel: message.threadTitle || fromLabel,
    GroupSubject: message.isGroup ? (message.threadTitle || message.channelName || undefined) : undefined,
    SenderName: message.senderName || undefined,
    SenderId: String(message.senderId),
    Provider: CHANNEL_ID,
    Surface: CHANNEL_ID,
    WasMentioned: message.isGroup ? wasMentioned : undefined,
    MessageSid: message.messageId,
    Timestamp: message.timestamp,
    OriginatingChannel: CHANNEL_ID,
    OriginatingTo: message.peerId,
    CommandAuthorized: commandAuthorized,
  });

  await core.channel.inbound.dispatchReply({
    cfg,
    channel: CHANNEL_ID,
    accountId: account.accountId,
    agentId: route.agentId,
    routeSessionKey: route.sessionKey,
    storePath,
    ctxPayload,
    recordInboundSession: core.channel.session.recordInboundSession,
    dispatchReplyWithBufferedBlockDispatcher: core.channel.reply.dispatchReplyWithBufferedBlockDispatcher,
    delivery: {
      deliver: async (payload) => {
        const text = typeof payload === "string" ? payload : (payload?.text ?? "");
        if (!text.trim()) return;
        await postToTwist({
          client,
          kind: message.kind === "thread" ? "thread" : "conv",
          id: message.kind === "thread" ? message.threadId : message.conversationId,
          text,
        });
        statusSink?.({ lastOutboundAt: Date.now() });
      },
      onError: (err, info) => {
        runtime.error?.(`twist ${info?.kind ?? "reply"} delivery failed: ${String(err)}`);
      },
    },
    replyPipeline: {},
    replyOptions: {},
    record: {
      onRecordError: (err) => runtime.error?.(`twist: session meta update failed: ${String(err)}`),
    },
  });
}
