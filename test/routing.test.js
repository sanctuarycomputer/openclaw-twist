import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseTarget,
  cleanTwistMarkup,
  classifyConversation,
  shouldRespondToConversation,
  shouldRespondToThread,
  isSelfAuthored,
  contentMentionsBot,
  newInboundItems,
  advanceCursor,
  routingPeer,
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
