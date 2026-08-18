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
import { contentMentionsBot, cleanTwistMarkup, stripIncompleteTurnFallback } from "./routing.js";
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
 * Runs OpenCLAW's ingress gate (DM policy / group mention / command auth) for
 * a normalized Twist message, without side effects beyond the resolver's own
 * reads. Self-contained given ({message, account, cfg}) so it can be invoked
 * ahead of dispatch (e.g. to record a denial) or inline within
 * handleTwistInbound.
 *
 * @param {object} p
 * @param {object} p.message normalized: {messageId,kind,conversationId,threadId,groupId,peerKind,peerId,isGroup,senderId,senderName,text,timestamp,directMention}
 * @param {object} p.account resolved Twist account
 * @param {object} p.cfg     live OpenCLAW config
 * @returns {Promise<{admit: boolean, admission: string, commandAuthorized: boolean}>}
 */
export async function admissionVerdict({ message, account, cfg }) {
  const core = getTwistRuntime();
  const rawBody = cleanTwistMarkup((message.text ?? "").trim());

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

  return {
    admit: access.ingress.admission === "dispatch",
    admission: access.ingress.admission,
    commandAuthorized: access.commandAccess?.authorized ?? false,
  };
}

/**
 * @param {object} p
 * @param {object} p.message normalized: {messageId,kind,conversationId,threadId,groupId,peerKind,peerId,isGroup,senderId,senderName,text,timestamp,directMention}
 * @param {object} p.account resolved Twist account
 * @param {object} p.cfg     live OpenCLAW config
 * @param {object} p.runtime logger-backed runtime
 * @param {object} p.client  TwistClient (for delivery)
 * @param {(u:object)=>void} [p.statusSink]
 * @param {{admit: boolean, admission: string, commandAuthorized: boolean}} [p.verdict] pre-computed admission verdict; when omitted, computed internally
 * @param {(outcome:{delivered?: boolean, suppressed?: string, failed?: boolean})=>void} [p.onDelivery]
 *   Called once per non-empty payload the dispatcher produces, AFTER the deliver decision:
 *   `{delivered:true}` for a real post, `{suppressed:"incomplete-turn"}` for a payload that
 *   was ONLY openclaw's incomplete_turn placeholder (never posted — see
 *   stripIncompleteTurnFallback), `{failed:true}` when the post threw. The caller owns what
 *   that means for the turn: the queue consumer fails-and-retries a turn that delivered
 *   nothing, instead of recording the item answered. See turnDeliveryVerdict.
 */
export async function handleTwistInbound({ message, account, cfg, runtime, client, statusSink, verdict, onDelivery }) {
  const core = getTwistRuntime();
  const rawBody = cleanTwistMarkup((message.text ?? "").trim());
  if (!rawBody) return;
  statusSink?.({ lastInboundAt: message.timestamp });

  if (!verdict) verdict = await admissionVerdict({ message, account, cfg });

  if (!verdict.admit) {
    runtime.log?.(
      `twist: drop ${message.kind} ${message.peerId} (admission=${verdict.admission})`,
    );
    return;
  }
  const commandAuthorized = verdict.commandAuthorized;
  const wasMentioned = Boolean(message.directMention) || contentMentionsBot(rawBody, account.botUserId);

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
        // openclaw's incomplete_turn placeholder is NOT an answer — posting it burns the
        // user's request (the item records `done`) over what is usually a transient flake.
        const { body, hadFallback } = stripIncompleteTurnFallback(text);
        if (hadFallback && !body) {
          // Placeholder-only: nothing to say. Tell the caller, which fails the turn so the
          // consumer's retry ladder takes over instead of recording it answered.
          runtime.log?.(`twist: suppressed incomplete_turn fallback for ${message.peerId} (not delivered): ${text.slice(0, 120)}`);
          onDelivery?.({ suppressed: "incomplete-turn" });
          return;
        }
        if (hadFallback) {
          // Mixed payload (openclaw's `terminalToolPresentation + placeholder` shape): the
          // tool output really happened, so it ships — minus the placeholder. The turn counts
          // as delivered; retrying it would re-post that output.
          runtime.log?.(`twist: stripped an incomplete_turn placeholder from a mixed payload to ${message.peerId} (delivering the remaining ${body.length} chars, not retrying)`);
        }
        try {
          const res = await postToTwist({
            client,
            kind: message.kind === "thread" ? "thread" : "conv",
            id: message.kind === "thread" ? message.threadId : message.conversationId,
            text: body,
          });
          if (!res?.suppressed) statusSink?.({ lastOutboundAt: Date.now() });
          onDelivery?.(res?.suppressed ? { suppressed: "cron-alert" } : { delivered: true });
        } catch (err) {
          // The dispatcher swallows a rejecting deliver (it routes to onError and resolves),
          // so an unrecorded failure here means the turn returns clean, the item records
          // `done`, and the reply is simply GONE. Record it: the caller retries the turn when
          // nothing else in it landed.
          runtime.error?.(`twist reply delivery to ${message.peerId} failed: ${String(err)}`);
          onDelivery?.({ failed: true });
        }
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
