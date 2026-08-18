import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseTarget,
  resolveOutboundTarget,
  cleanTwistMarkup,
  buildTranscript,
  stripPreHeaderNarration,
  isBareCronFailureAlert,
  classifyConversation,
  shouldRespondToConversation,
  shouldRespondToThread,
  isSelfAuthored,
  contentMentionsBot,
  newInboundItems,
  advanceCursor,
  firstSightCursor,
  routingPeer,
  channelDefaultRecipients,
} from "../src/routing.js";

const BOT = 634870; // Stacksbot

test("classifyConversation: 2 participants is a 1:1 DM, more is a group", () => {
  assert.equal(classifyConversation(2), "dm");
  assert.equal(classifyConversation(3), "groupdm");
  assert.equal(classifyConversation(8), "groupdm");
});

test("1:1 DM responds regardless of mention; group DM only on mention", () => {
  assert.equal(shouldRespondToConversation({ kind: "dm", directMention: false }), true);
  assert.equal(shouldRespondToConversation({ kind: "groupdm", directMention: false }), false);
  assert.equal(shouldRespondToConversation({ kind: "groupdm", directMention: true }), true);
});

test("thread responds only when directly mentioned", () => {
  assert.equal(shouldRespondToThread({ directMention: false }), false);
  assert.equal(shouldRespondToThread({ directMention: true }), true);
});

test("self-loop guard: Stacksbot's own posts are ignored", () => {
  assert.equal(isSelfAuthored(634870, BOT), true);
  assert.equal(isSelfAuthored("634870", BOT), true);
  assert.equal(isSelfAuthored(427360, BOT), false);
});

test("parseTarget parses thread/conv targets and aliases, rejects bad ones", () => {
  assert.deepEqual(parseTarget("thread:123"), { kind: "thread", id: "123" });
  assert.deepEqual(parseTarget("conv:456"), { kind: "conv", id: "456" });
  assert.deepEqual(parseTarget("twist:thread:789"), { kind: "thread", id: "789" });
  // aliases for conversations
  assert.deepEqual(parseTarget("conversation:1367817"), { kind: "conv", id: "1367817" });
  assert.deepEqual(parseTarget("twist:conversation:1367817"), { kind: "conv", id: "1367817" });
  assert.deepEqual(parseTarget("dm:42"), { kind: "conv", id: "42" });
  // bare id defaults to conversation
  assert.deepEqual(parseTarget("1367817"), { kind: "conv", id: "1367817" });
  assert.deepEqual(parseTarget("twist:1367817"), { kind: "conv", id: "1367817" });
  assert.throws(() => parseTarget("user:1"));
  assert.throws(() => parseTarget("thread:"));
});

test("buildTranscript excludes the trigger and keeps the other items in order", () => {
  const items = [
    { id: 1, creator_name: "Hugh", content: "https://github.com/org/repo" },
    { id: 2, creator_name: "Hugh", content: "need creds, how?" },
  ];
  // trigger = the latest (id 2) → transcript carries the earlier link message
  assert.deepEqual(buildTranscript(items, 2), [{ name: "Hugh", content: "https://github.com/org/repo" }]);
  // single message (only the trigger) → empty transcript
  assert.deepEqual(buildTranscript([{ id: 2, creator_name: "Hugh", content: "x" }], 2), []);
  // respects the limit (last N excluding trigger)
  const many = Array.from({ length: 20 }, (_, i) => ({ id: i, creator_name: "U", content: String(i) }));
  assert.equal(buildTranscript(many, 19, 5).length, 5);
  assert.deepEqual(buildTranscript(undefined, 1), []);
});

test("stripPreHeaderNarration drops leaked run narration before a report header", () => {
  const header = "# Observations Digest Job via [Stacksbot](https://app.notion.com/p/390131fea2c7811c9557cda134dccd30)";
  const body = `${header}\n\n*last 24h*\n\n- item one`;

  // Leaked preamble before the header is dropped; report survives intact.
  const leaked = `I now have the observations data. Let me compose the digest.\n\nBased on the query results, I have 20 observations.\n\n${body}`;
  assert.equal(stripPreHeaderNarration(leaked), body);

  // Header already first → untouched.
  assert.equal(stripPreHeaderNarration(body), body);

  // No report header at all (ordinary conversational reply) → untouched.
  const chat = "Sounds good — I'll get that vetting run started now.";
  assert.equal(stripPreHeaderNarration(chat), chat);

  // Header inside a fenced code block does not count as a report header.
  const quoted = "Here's what the report format looks like:\n```\n# Recall Skill via [Stacksbot](https://x)\n```\ndone";
  assert.equal(stripPreHeaderNarration(quoted), quoted);

  // Only whitespace before the header → normalized to header-first.
  assert.equal(stripPreHeaderNarration(`\n\n${body}`), body);

  // Empty/nullish input passes through.
  assert.equal(stripPreHeaderNarration(""), "");
});

test("stripPreHeaderNarration fence tracking: mismatched closers and indented pseudo-fences", () => {
  const header = "# Recall Skill via [Stacksbot](https://x)";

  // A ~~~ line inside a backtick fence does NOT close it (CommonMark: closers must
  // match the opening character) — the header stays quoted, nothing is stripped.
  const tildeInside = "look at this format:\n```\n~~~\n" + header + "\n```\ndone";
  assert.equal(stripPreHeaderNarration(tildeInside), tildeInside);

  // An indented (≥4 spaces) ``` line is content, not a fence delimiter — it must
  // not close the surrounding fence and expose the quoted header.
  const indentedInside = "before\n```\n    ```\n" + header + "\n```\nafter";
  assert.equal(stripPreHeaderNarration(indentedInside), indentedInside);

  // A properly closed fence followed by a real header outside it still strips.
  const closedThenHeader = "narration\n```\nquoted stuff\n```\n" + header + "\n\nbody";
  assert.equal(stripPreHeaderNarration(closedThenHeader), `${header}\n\nbody`);
});

test("isBareCronFailureAlert: quoted-alert replies and oversized messages are NOT suppressed", () => {
  // An agent reply that OPENS by quoting the alert verbatim, then explains —
  // structured (blank line) content must never be swallowed.
  const quotedReply =
    '⚠️ Cron job "notion:3ac1" failed: exec error — here is what happened and how I fixed it:\n\n1. The filter file was malformed\n2. Re-ran with the corrected body';
  assert.equal(isBareCronFailureAlert(quotedReply), false);

  // Oversized "alert" (way past any real gateway alert) fails open → posts.
  const huge = '⚠️ Cron job "x" failed: ' + "e".repeat(700);
  assert.equal(isBareCronFailureAlert(huge), false);

  // A genuine bare alert with a short multiline error (no blank line) still matches.
  assert.equal(isBareCronFailureAlert('⚠️ Cron job "x" failed: line one\nline two'), true);
});

test("isBareCronFailureAlert matches the gateway's bare cron alert and nothing else", () => {
  // The redundant bare alert observed in the thread (duplicates the reconciler's
  // formatted Job Failure Alert ~13 min later).
  assert.equal(
    isBareCronFailureAlert('⚠️ Cron job "notion:3ac131fea2c7812f92e1fdc1f7b5de02" failed: ⚠️ 🛠️ Exec failed: `ntn api …`'),
    true,
  );
  assert.equal(isBareCronFailureAlert('⚠️ Cron job "stacksbot-resync" failed: boom'), true);
  // The reconciler's formatted alert opens with the report header — never matched.
  assert.equal(
    isBareCronFailureAlert('# Job Failure Alert via [Stacksbot](https://x)\n\n⚠️ Job **"Observe: Twist"** failed — …'),
    false,
  );
  // Auto-disable notices and conversational mentions pass through.
  assert.equal(isBareCronFailureAlert('⚠️ Cron job "x" has been auto-disabled after 5 consecutive schedule errors.'), false);
  assert.equal(isBareCronFailureAlert('fyi: ⚠️ Cron job "x" failed: y'), false);
  assert.equal(isBareCronFailureAlert(""), false);
});

test("buildTranscript keeps the NEWEST items in chronological order given a descending fetch", () => {
  // comments/get and conversation_messages/get return DESCENDING (newest first)
  // when called without from_obj_index — exactly how buildContext fetches. The
  // transcript must still carry the newest window, oldest→newest.
  const descending = Array.from({ length: 30 }, (_, i) => ({
    id: 30 - i,
    obj_index: 30 - i,
    creator_name: "U",
    content: `msg ${30 - i}`,
  }));
  // trigger = the newest (id 30); the message posted just before it (id 29) MUST survive
  const transcript = buildTranscript(descending, 30, 15);
  assert.equal(transcript.length, 15);
  assert.deepEqual(
    transcript.map((t) => t.content),
    Array.from({ length: 15 }, (_, i) => `msg ${15 + i}`), // 15..29, chronological
  );
});

test("cleanTwistMarkup rewrites mention markup to @Name", () => {
  assert.equal(
    cleanTwistMarkup("hey [Stacksbot](twist-mention://634870) vet [Acme](twist-group-mention://9)"),
    "hey @Stacksbot vet @Acme",
  );
  assert.equal(cleanTwistMarkup("plain text"), "plain text");
  assert.equal(cleanTwistMarkup(""), "");
});

test("content mention backstop matches twist-mention markup", () => {
  assert.equal(contentMentionsBot("hey [Stacksbot](twist-mention://634870) help", BOT), true);
  assert.equal(contentMentionsBot("hey [Hugh](twist-mention://427360)", BOT), false);
  assert.equal(contentMentionsBot("", BOT), false);
});

test("newInboundItems returns only newer, non-self items in order", () => {
  const items = [
    { obj_index: 9, creator: 427360, content: "c" },
    { obj_index: 7, creator: 427360, content: "a" },
    { obj_index: 8, creator: 634870, content: "bot reply" }, // self
    { obj_index: 6, creator: 427360, content: "old" }, // <= cursor
  ];
  const fresh = newInboundItems(items, 6, BOT);
  assert.deepEqual(fresh.map((i) => i.obj_index), [7, 9]);
});

test("advanceCursor takes the highest obj_index seen (including self posts)", () => {
  const items = [
    { obj_index: 7, creator: 427360 },
    { obj_index: 8, creator: 634870 },
  ];
  assert.equal(advanceCursor(6, items), 8);
  assert.equal(advanceCursor(10, items), 10); // never goes backwards
  assert.equal(advanceCursor(-Infinity, []), -Infinity);
});

test("firstSightCursor: pre-boot backlog is baselined, post-boot mention stays fresh", () => {
  const items = [
    { obj_index: 5, creator: 427360, posted_ts: 900, content: "old chatter" },
    { obj_index: 6, creator: 427360, posted_ts: 950, content: "more old chatter" },
    { obj_index: 7, creator: 427360, posted_ts: 1100, content: "[Stacksbot](twist-mention://634870) u there?" },
  ];
  // boot/cutoff at 1000 → baseline to the last pre-boot item (6); the live mention (7) stays fresh
  const cursor = firstSightCursor(items, 0, 1000);
  assert.equal(cursor, 6);
  assert.deepEqual(newInboundItems(items, cursor, BOT).map((i) => i.obj_index), [7]);
});

test("firstSightCursor: all items pre-boot → baseline to latest, nothing fresh (backlog never answered)", () => {
  const items = [
    { obj_index: 5, creator: 427360, posted_ts: 900 },
    { obj_index: 6, creator: 427360, posted_ts: 950 },
  ];
  const cursor = firstSightCursor(items, 0, 1000);
  assert.equal(cursor, 6);
  assert.deepEqual(newInboundItems(items, cursor, BOT), []);
});

test("firstSightCursor: items with no usable posted_ts are treated as backlog", () => {
  const items = [
    { obj_index: 5, creator: 427360 }, // no timestamp → backlog (safe default)
    { obj_index: 6, creator: 427360, posted_ts: 1100 }, // post-boot → fresh
  ];
  const cursor = firstSightCursor(items, 0, 1000);
  assert.equal(cursor, 5);
  assert.deepEqual(newInboundItems(items, cursor, BOT).map((i) => i.obj_index), [6]);
});

test("firstSightCursor: empty/undefined items fall back to the read-marker index", () => {
  assert.equal(firstSightCursor([], 4, 1000), 4);
  assert.equal(firstSightCursor(undefined, 4, 1000), 4);
});

test("firstSightCursor: brand-new conversation — fresh first message at obj_index 0 is answered", () => {
  // Regression (2026-08-05): a just-created group DM's only message sits at obj_index 0,
  // which equals the conversation's read-marker. Folding that marker into the baseline
  // swallowed the very mention that created the conversation.
  const items = [
    { obj_index: 0, creator: 427360, posted_ts: 1100, content: "[Stacksbot](twist-mention://634870) financial state?" },
  ];
  const cursor = firstSightCursor(items, 0, 1000);
  assert.equal(cursor, -1);
  assert.deepEqual(newInboundItems(items, cursor, BOT).map((i) => i.obj_index), [0]);
});

test("firstSightCursor: read-marker above backlog does not swallow fresh items when items are present", () => {
  const items = [
    { obj_index: 5, creator: 427360, posted_ts: 900 }, // backlog
    { obj_index: 6, creator: 427360, posted_ts: 1100, content: "[Stacksbot](twist-mention://634870) hi" }, // fresh
  ];
  // read-marker points at the latest item (6); baseline must still stop at the backlog (5)
  const cursor = firstSightCursor(items, 6, 1000);
  assert.equal(cursor, 5);
  assert.deepEqual(newInboundItems(items, cursor, BOT).map((i) => i.obj_index), [6]);
});

test("resolveOutboundTarget: explicit target wins, else falls back to defaultTo, else throws", () => {
  assert.deepEqual(resolveOutboundTarget("thread:5", "conv:9"), { kind: "thread", id: "5" });
  assert.deepEqual(resolveOutboundTarget("", "thread:7882650"), { kind: "thread", id: "7882650" });
  assert.deepEqual(resolveOutboundTarget(undefined, "conv:9"), { kind: "conv", id: "9" });
  assert.throws(() => resolveOutboundTarget("", ""));
  assert.throws(() => resolveOutboundTarget(undefined, undefined));
});

test("channelDefaultRecipients: honors use_default_recipients + non-empty list", () => {
  assert.deepEqual(channelDefaultRecipients({ use_default_recipients: true, default_recipients: [427360] }), [427360]);
  assert.equal(channelDefaultRecipients({ use_default_recipients: false, default_recipients: [427360] }), null);
  assert.equal(channelDefaultRecipients({ use_default_recipients: true, default_recipients: [] }), null);
  assert.equal(channelDefaultRecipients({}), null);
  assert.equal(channelDefaultRecipients(null), null);
});

test("routingPeer produces the documented session-key peer shapes", () => {
  assert.deepEqual(routingPeer({ kind: "dm", conversationId: 1367817 }), {
    peerKind: "direct",
    peerId: "dm:1367817",
    isGroup: false,
  });
  assert.deepEqual(routingPeer({ kind: "groupdm", conversationId: 555 }), {
    peerKind: "group",
    peerId: "conv:555",
    isGroup: true,
  });
  assert.deepEqual(routingPeer({ kind: "thread", threadId: 3424981 }), {
    peerKind: "group",
    peerId: "thread:3424981",
    isGroup: true,
  });
});
