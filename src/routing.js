// Pure routing logic for the Twist channel. No SDK or network coupling so it can
// be unit-tested in isolation. These functions decide WHETHER and HOW Stacksbot
// should respond to a given Twist unread item, and how to advance read cursors.

/** Twist mention markup for a user id, e.g. `[Name](twist-mention://634870)`. */
export function mentionMarker(botUserId) {
  return `twist-mention://${botUserId}`;
}

/**
 * Parse an outbound target string into {kind,id}. Accepts (optionally prefixed
 * with "twist:"):
 *   - "thread:<id>"                          → a channel thread
 *   - "conv:<id>" / "conversation:<id>" / "dm:<id>" → a conversation (DM/group DM)
 *   - bare "<id>" (digits)                   → defaults to a conversation
 */
export function parseTarget(to) {
  const raw = String(to).trim().replace(/^twist:/, "");
  if (/^\d+$/.test(raw)) return { kind: "conv", id: raw }; // bare id → conversation
  const sep = raw.indexOf(":");
  const kindRaw = sep >= 0 ? raw.slice(0, sep) : "";
  const id = sep >= 0 ? raw.slice(sep + 1) : "";
  if (id) {
    if (kindRaw === "thread") return { kind: "thread", id };
    if (kindRaw === "conv" || kindRaw === "conversation" || kindRaw === "dm") return { kind: "conv", id };
  }
  throw new Error(`twist: invalid target "${to}" (expected thread:<id>, conv:<id>, conversation:<id>, dm:<id>, or a bare conversation id)`);
}

/**
 * Resolve the outbound target: an explicit `to` wins; otherwise fall back to the
 * channel's configured `defaultTo`. Throws if neither is present.
 * @returns {{kind:"thread"|"conv", id:string}}
 */
export function resolveOutboundTarget(to, defaultTo) {
  const raw = to == null ? "" : String(to).trim();
  if (raw) return parseTarget(raw);
  const fallback = defaultTo == null ? "" : String(defaultTo).trim();
  if (fallback) return parseTarget(fallback);
  throw new Error("twist: no delivery target and no channels.twist.defaultTo configured");
}

/** Rewrite Twist mention markup `[Name](twist-mention://id)` to readable `@Name`. */
export function cleanTwistMarkup(text) {
  if (!text) return "";
  return text.replace(/\[([^\]]+)\]\(twist-(?:group-)?mention:\/\/\d+\)/g, "@$1");
}

/**
 * Classify a conversation (DM) by participant count.
 * Twist conversations carry a `user_ids` array; 2 participants (bot + one human)
 * is a 1:1 DM, more is a group DM.
 * @returns {"dm"|"groupdm"}
 */
export function classifyConversation(participantCount) {
  return participantCount > 2 ? "groupdm" : "dm";
}

/**
 * Backstop mention check against raw content, independent of Twist's
 * `direct_mention` flag. Used to corroborate the flag and to catch mentions in
 * fetched message bodies.
 */
export function contentMentionsBot(content, botUserId) {
  if (!content) return false;
  return content.includes(mentionMarker(botUserId));
}

/**
 * Decide whether to respond to an unread CONVERSATION item.
 * - 1:1 DM  → respond to every new human message (option A: persistent session).
 * - group DM → respond only when Stacksbot is @mentioned.
 * @param {{ kind: "dm"|"groupdm", directMention: boolean }} p
 */
export function shouldRespondToConversation({ kind, directMention }) {
  if (kind === "dm") return true;
  return Boolean(directMention);
}

/**
 * Decide whether to respond to an unread channel THREAD item.
 * Threads are public; respond only when Stacksbot is @mentioned.
 * @param {{ directMention: boolean }} p
 */
export function shouldRespondToThread({ directMention }) {
  return Boolean(directMention);
}

/**
 * Build a transcript (chronological, excluding the trigger item) of recent items
 * so the agent gets surrounding context, not just the single trigger message.
 * @param {Array<{id:any,creator_name?:string,content?:string}>} items
 * @param {string|number} triggerId  id of the item being dispatched (excluded)
 */
export function buildTranscript(items, triggerId, limit = 15) {
  return (items ?? [])
    .filter((it) => String(it.id) !== String(triggerId))
    .slice(-limit)
    .map((it) => ({ name: it.creator_name, content: it.content }));
}

/** True when a message/comment was authored by Stacksbot itself (self-loop guard). */
export function isSelfAuthored(creatorId, botUserId) {
  return String(creatorId) === String(botUserId);
}

/**
 * Given fetched items (comments or conversation_messages) and the last-processed
 * cursor, return the new, non-self items in chronological order. Twist orders by
 * monotonically increasing `obj_index` per thread/conversation.
 * @param {Array<{obj_index:number, creator:any}>} items
 * @param {number} cursorObjIndex  last processed obj_index (-Infinity for none)
 * @param {string|number} botUserId
 */
export function newInboundItems(items, cursorObjIndex, botUserId) {
  return items
    .filter((it) => typeof it.obj_index === "number" && it.obj_index > cursorObjIndex)
    .filter((it) => !isSelfAuthored(it.creator, botUserId))
    .sort((a, b) => a.obj_index - b.obj_index);
}

/** Next cursor value after processing items (highest obj_index seen, incl. self). */
export function advanceCursor(prevCursor, items) {
  return items.reduce(
    (max, it) => (typeof it.obj_index === "number" && it.obj_index > max ? it.obj_index : max),
    prevCursor,
  );
}

/**
 * Build the OpenCLAW routing peer for the envelope/session-key builder.
 * Session-key shapes (per channel-routing docs):
 *   dm       → direct  : agent:<id>:twist:dm:<convId>      (persistent, option A)
 *   groupdm  → group   : agent:<id>:twist:group:conv:<id>  (respond on mention)
 *   thread   → group   : agent:<id>:twist:group:thread:<id> (separate per thread)
 * @param {{ kind:"dm"|"groupdm"|"thread", conversationId?:string|number, threadId?:string|number }} p
 * @returns {{ peerKind:"direct"|"group", peerId:string, isGroup:boolean }}
 */
export function routingPeer({ kind, conversationId, threadId }) {
  if (kind === "dm") {
    return { peerKind: "direct", peerId: `dm:${conversationId}`, isGroup: false };
  }
  if (kind === "groupdm") {
    return { peerKind: "group", peerId: `conv:${conversationId}`, isGroup: true };
  }
  return { peerKind: "group", peerId: `thread:${threadId}`, isGroup: true };
}
