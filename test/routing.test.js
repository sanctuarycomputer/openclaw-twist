import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseTarget,
  resolveOutboundTarget,
  cleanTwistMarkup,
  buildTranscript,
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
