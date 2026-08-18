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
 * Input order is NOT trusted: the bare comments/get / conversation_messages/get
 * calls return DESCENDING (newest first), so the items are sorted by obj_index
 * before windowing — otherwise `.slice(-limit)` keeps the OLDEST slice of the
 * fetch and the agent loses exactly the most recent context. Items lacking
 * obj_index all tie at 0, so for them the stable sort preserves input order —
 * chronology is only guaranteed when obj_index is present (Twist always sets it).
 * @param {Array<{id:any,obj_index?:number,creator_name?:string,content?:string}>} items
 * @param {string|number} triggerId  id of the item being dispatched (excluded)
 */
export function buildTranscript(items, triggerId, limit = 15) {
  return [...(items ?? [])]
    .sort((a, b) => (a.obj_index ?? 0) - (b.obj_index ?? 0))
    .filter((it) => String(it.id) !== String(triggerId))
    .slice(-limit)
    .map((it) => ({ name: it.creator_name, content: it.content }));
}

/**
 * Enforce the report-delivery contract: generated posts must OPEN with their
 * `# <Producer> … via [Stacksbot](<url>)` header. Models routinely leak run
 * narration ("I now have the data. Let me compose…") before that header, and
 * prompt discipline has not stopped it — so the outbound path drops anything
 * preceding the first report-header line. Messages with no such header (ordinary
 * conversational replies) pass through untouched, as does a header quoted inside
 * a fenced code block (fences close only on a matching ``` / ~~~ character, and
 * lines indented ≥4 spaces are content, not delimiters — per CommonMark).
 *
 * Known, ACCEPTED alteration: a conversational reply that intros a re-posted
 * report ("Here's the digest you asked about:\n\n# … via [Stacksbot](…)") loses
 * the intro line — the header-first contract is enforced even on re-posts. The
 * report body itself always survives, and the strip is logged by the caller.
 */
export function stripPreHeaderNarration(text) {
  if (!text) return text;
  const lines = text.split("\n");
  let fenceChar = null; // "`" or "~" while inside a fence, else null
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const fence = /^ {0,3}(`{3,}|~{3,})/.exec(line);
    if (fence) {
      const ch = fence[1][0];
      if (fenceChar === null) fenceChar = ch; // open
      else if (ch === fenceChar) fenceChar = null; // matching close only
      continue;
    }
    if (fenceChar === null && /^#\s.*via \[Stacksbot\]\(/.test(line)) {
      if (i === 0) return text;
      return lines.slice(i).join("\n");
    }
  }
  return text;
}

// A real gateway alert is one short paragraph. Anything longer or containing a
// blank line is treated as NOT the bare alert — e.g. an agent reply that OPENS by
// quoting the alert verbatim and then explains. Both bounds fail toward posting
// (worst case: a duplicate alert), never toward losing a message.
const BARE_ALERT_MAX_CHARS = 600;

/**
 * The gateway's bare per-occurrence cron failure alert
 * (`⚠️ Cron job "<name>" failed: <error>` — emitted by openclaw's
 * dispatchCronFailureDestinationNotifications). It is fully redundant: the
 * jobs-status reconciler posts a formatted, deduped `# Job Failure Alert` for the
 * same failure (with job name, link, and transition-based dedup), and repeat
 * failures are already surfaced via the Jobs DB status fields, the Logs ledger,
 * and the daily health review. The outbound path drops this shape — bounded to
 * short, single-paragraph messages so a reply merely QUOTING an alert (followed
 * by structured explanation) can never be swallowed.
 */
export function isBareCronFailureAlert(text) {
  const s = text ?? "";
  if (s.length > BARE_ALERT_MAX_CHARS || s.includes("\n\n")) return false;
  return /^⚠️ Cron job "[^"]+" failed: /.test(s);
}

/**
 * openclaw's `incomplete_turn` fallback — the placeholder it substitutes when an attempt
 * produced no usable assistant turn (empty response, reasoning-only, length-terminal,
 * provider error). Source: `resolveIncompleteTurnPayloadText` in openclaw 2026.6.11
 * (`dist/selection-*.js`), which emits one of two strings sharing this prefix:
 *
 *   "⚠️ Agent couldn't generate a response. Note: some tool actions may have already been
 *    executed — please verify before retrying."   (side effects possible)
 *   "⚠️ Agent couldn't generate a response. Please try again."
 *
 * This is diagnostic scaffolding, not an answer. Delivering it consumes the user's request:
 * the queue records the item `done` (a payload WAS produced), so a transient platform flake
 * silently costs a real message. Detecting it lets the channel suppress the post and fail
 * the turn instead, handing the item to the retry ladder that already exists for every other
 * transient failure. Matched on the PREFIX so both variants (and any future suffix) are
 * caught, and tolerant of either apostrophe in case the host's copy is ever typographic.
 */
export function isIncompleteTurnFallback(text) {
  return /^\s*⚠️ Agent couldn['’]t generate a response/.test(String(text ?? ""));
}

/**
 * Fold one turn's per-payload delivery outcomes into a verdict.
 *
 * The rule that matters: retry ONLY when the incomplete_turn placeholder was the turn's
 * sole output. A turn that answered and then trailed a placeholder block has already served
 * the human — retrying it would post the real answer a second time. A payload suppressed
 * for any OTHER reason (the redundant bare cron alert) counts as delivered: dropping it was
 * the intended outcome, not a failure.
 *
 * @param {Array<{delivered?: boolean, suppressed?: string}>} outcomes
 * @returns {{delivered: boolean, retryAsIncompleteTurn: boolean}}
 */
export function turnDeliveryVerdict(outcomes) {
  let delivered = false;
  let sawIncompleteTurn = false;
  for (const outcome of outcomes ?? []) {
    if (outcome?.suppressed === "incomplete-turn") sawIncompleteTurn = true;
    else delivered = true;
  }
  return { delivered, retryAsIncompleteTurn: sawIncompleteTurn && !delivered };
}

/** Channel default recipients to notify, or null to use Twist's default. */
export function channelDefaultRecipients(channel) {
  if (channel && channel.use_default_recipients && Array.isArray(channel.default_recipients) && channel.default_recipients.length) {
    return channel.default_recipients;
  }
  return null;
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
 * Cursor to seed on the FIRST sight of a thread/conversation. A thread is only polled
 * once it has a direct @mention, so naively baselining to the latest obj_index swallows
 * that very first mention (it gets treated as backlog and is never answered). Instead,
 * baseline only over items that predate the poller coming online — everything posted at
 * or after `freshSinceTs` stays above the returned cursor and is processed normally, so
 * the first mention in a brand-new thread IS answered. Older backlog (and items with no
 * usable posted_ts, treated as backlog for safety) is still baselined away, so a fresh
 * boot never replies to a pile of historical mentions.
 *
 * `fallbackObjIndex` (the conversation's read-marker index) applies ONLY when no items
 * were fetched: it points at the latest item, so folding it into the baseline when items
 * ARE present swallows a fresh message that shares that index — a brand-new conversation's
 * first message sits at obj_index 0 and must beat the cursor, so the baseline over fetched
 * items starts below 0.
 */
export function firstSightCursor(items, fallbackObjIndex, freshSinceTs) {
  if (!items || items.length === 0) return fallbackObjIndex;
  const backlog = items.filter(
    (it) => !(typeof it.posted_ts === "number" && it.posted_ts >= freshSinceTs),
  );
  return advanceCursor(-1, backlog);
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
