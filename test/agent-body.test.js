import { test } from "node:test";
import assert from "node:assert/strict";
import { buildTwistContextBlock, buildAgentBody, buildInboundBodies } from "../src/agent-body.js";

const TRANSCRIPT = [
  { name: "Hugh Francis", content: "Woop" },
  { name: "Stacksbot", content: "🚨 Critical, needs you today: PR #1066 is sitting open." },
];

test("buildTwistContextBlock: a 1:1 DM gets the transcript with no header", () => {
  const block = buildTwistContextBlock({ kind: "dm", transcript: TRANSCRIPT });
  assert.equal(block, "\nConversation so far:\nHugh Francis: Woop\nStacksbot: 🚨 Critical, needs you today: PR #1066 is sitting open.");
});

test("buildTwistContextBlock: thread and group headers, and nothing at all without a transcript", () => {
  assert.match(
    buildTwistContextBlock({ kind: "thread", threadTitle: "Standup", channelName: "ops", threadId: 7882650, transcript: TRANSCRIPT }),
    /^\[Twist thread: "Standup" in #ops · thread_id 7882650\]\n\nConversation so far:/,
  );
  assert.match(buildTwistContextBlock({ kind: "groupdm", conversationId: 42, transcript: TRANSCRIPT }), /^\[Twist group conversation · conversation_id 42\]/);
  assert.equal(buildTwistContextBlock({ kind: "dm", transcript: [] }), "");
  assert.equal(buildTwistContextBlock({ kind: "dm" }), "");
});

test("buildAgentBody: context first, then the new message; raw alone when there is no context", () => {
  assert.equal(buildAgentBody({ contextBlock: "CTX", fromLabel: "Hugh Francis", rawBody: "If I merge that PR will it deploy?" }), "CTX\n\nNew message from Hugh Francis:\nIf I merge that PR will it deploy?");
  assert.equal(buildAgentBody({ contextBlock: "", fromLabel: "Hugh Francis", rawBody: "hi" }), "hi");
});

test("buildInboundBodies: the MODEL's body carries the transcript; command/raw bodies stay bare (the 2026-09-14 DM regression)", () => {
  const calls = [];
  const buildEnvelope = (p) => {
    calls.push(p);
    return { storePath: "/tmp/store", body: `[Twist ${p.from}] ${p.body}` };
  };
  const { storePath, bodies } = buildInboundBodies({
    message: { kind: "dm", transcript: TRANSCRIPT },
    rawBody: "If I merge that PR will it deploy?",
    fromLabel: "Hugh Francis",
    timestamp: 1789000000000,
    buildEnvelope,
  });
  assert.equal(storePath, "/tmp/store");
  assert.match(bodies.BodyForAgent, /Conversation so far:[\s\S]*PR #1066[\s\S]*New message from Hugh Francis:\nIf I merge that PR will it deploy\?$/);
  assert.equal(bodies.RawBody, "If I merge that PR will it deploy?");
  assert.equal(bodies.CommandBody, "If I merge that PR will it deploy?");
  assert.equal(bodies.Body, `[Twist Hugh Francis] ${bodies.BodyForAgent}`);
  assert.deepEqual(calls, [{ channel: "Twist", from: "Hugh Francis", timestamp: 1789000000000, body: bodies.BodyForAgent }]);
});

test("buildInboundBodies: without context every body is the raw message", () => {
  const { bodies } = buildInboundBodies({
    message: { kind: "dm", transcript: [] },
    rawBody: "yo",
    fromLabel: "Hugh Francis",
    timestamp: 1,
    buildEnvelope: (p) => ({ storePath: "s", body: p.body }),
  });
  assert.deepEqual(bodies, { Body: "yo", BodyForAgent: "yo", RawBody: "yo", CommandBody: "yo" });
});
