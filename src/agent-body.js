// What the AGENT reads for an inbound Twist message, as a pure module (no SDK imports)
// so the contract is unit-testable.
//
// openclaw builds the model prompt from `BodyForAgent`. Its inbound finalizer defaults
// that field to `CommandBody ?? RawBody` — the bare trigger text — whenever a channel
// does not set it; `Body` (the envelope) is NOT the prompt. Every bundled channel that
// wants history in front of the model therefore passes BOTH: `Body` for the envelope
// and `BodyForAgent` for the composed text, while `RawBody`/`CommandBody` stay raw so
// slash-commands still parse. This plugin only ever set `Body`, so the "Conversation so
// far" block it carefully built (openclaw-twist#6) never reached the model: live
// 2026-09-14 16:09Z, Hugh asked "If I merge that PR will it deploy?" three hours after a
// cron job had DM'd him the PR, and the DM session answered that no PR had come up —
// the transcript in front of it had the PR as its last line, but only inside `Body`.
import { cleanTwistMarkup } from "./routing.js";

/**
 * Render the surrounding Twist context (thread title, channel, prior messages) so the
 * agent operates with full context, not just the bare trigger. "" when there is none.
 * @param {{kind?:string, threadTitle?:string, channelName?:string, threadId?:any, conversationId?:any, transcript?:Array<{name?:string, content?:string}>}} message
 */
export function buildTwistContextBlock(message) {
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

/**
 * The text the agent should read: the context block (when any) followed by the new
 * message, or the raw message alone.
 */
export function buildAgentBody({ contextBlock, fromLabel, rawBody }) {
  return contextBlock ? `${contextBlock}\n\nNew message from ${fromLabel}:\n${rawBody}` : rawBody;
}

/**
 * The four body fields of the inbound context payload, with the contract spelled out:
 *   Body          — the channel envelope (what gets stored / shown as the message)
 *   BodyForAgent  — the composed text the MODEL reads (context + new message)
 *   RawBody       — the bare trigger text
 *   CommandBody   — the bare trigger text (slash-commands parse from here, never from context)
 * `buildEnvelope` is the runtime's envelope builder for this route; it receives the
 * composed text so the stored envelope and the prompt agree.
 */
export function buildInboundBodies({ message, rawBody, fromLabel, timestamp, buildEnvelope }) {
  const contextBlock = buildTwistContextBlock(message);
  const agentBody = buildAgentBody({ contextBlock, fromLabel, rawBody });
  const { storePath, body } = buildEnvelope({ channel: "Twist", from: fromLabel, timestamp, body: agentBody });
  return {
    storePath,
    bodies: { Body: body, BodyForAgent: agentBody, RawBody: rawBody, CommandBody: rawBody },
  };
}
