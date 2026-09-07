import { test } from "node:test";
import assert from "node:assert/strict";
import { newestSelfPost, findRecentSelfPost, RECENT_SELF_POST_WINDOW_MS } from "../src/recap-receipt.js";

const BOT = 634870;
const NOW = Date.parse("2026-09-07T12:24:03Z");
const secs = (iso) => Math.floor(Date.parse(iso) / 1000);

test("newestSelfPost: picks the bot's newest post inside the window, ignores other authors", () => {
  const items = [
    { id: 300, creator: 111, posted_ts: secs("2026-09-07T12:24:02Z") }, // a human, newest
    { id: 200, creator: BOT, posted_ts: secs("2026-09-07T12:24:01Z") }, // the report
    { id: 100, creator: BOT, posted_ts: secs("2026-09-07T11:50:00Z") }, // older bot post
  ];
  assert.deepEqual(newestSelfPost(items, { botUserId: BOT, nowMs: NOW }), {
    messageId: "200",
    postedMs: Date.parse("2026-09-07T12:24:01Z"),
  });
});

test("newestSelfPost: creator ids compare as strings (env-sourced botUserId is a string)", () => {
  const items = [{ id: 7, creator: 634870, posted_ts: secs("2026-09-07T12:20:00Z") }];
  assert.equal(newestSelfPost(items, { botUserId: "634870", nowMs: NOW })?.messageId, "7");
});

test("newestSelfPost: a bot post older than the window is not the deliverable", () => {
  const old = { id: 5, creator: BOT, posted_ts: secs("2026-09-07T10:00:00Z") }; // 2h24m ago
  assert.equal(newestSelfPost([old], { botUserId: BOT, nowMs: NOW }), null);
  assert.equal(newestSelfPost([old], { botUserId: BOT, nowMs: NOW, windowMs: 3 * 60 * 60 * 1000 })?.messageId, "5");
  assert.equal(RECENT_SELF_POST_WINDOW_MS, 60 * 60 * 1000);
});

test("newestSelfPost: null without a bot id, without items, or with malformed items", () => {
  const items = [{ id: 1, creator: BOT, posted_ts: secs("2026-09-07T12:24:00Z") }];
  assert.equal(newestSelfPost(items, { botUserId: undefined, nowMs: NOW }), null);
  assert.equal(newestSelfPost(items, { botUserId: "", nowMs: NOW }), null);
  assert.equal(newestSelfPost(undefined, { botUserId: BOT, nowMs: NOW }), null);
  assert.equal(newestSelfPost([null, { creator: BOT }, { id: 2, creator: BOT, posted_ts: "x" }], { botUserId: BOT, nowMs: NOW }), null);
});

test("findRecentSelfPost: reads thread comments for a thread, conversation messages for a conv", async () => {
  const calls = [];
  const client = {
    getThreadComments: async (id, opts) => {
      calls.push(["thread", id, opts]);
      return [{ id: 9, creator: BOT, posted_ts: secs("2026-09-07T12:24:01Z") }];
    },
    getConversationMessages: async (id, opts) => {
      calls.push(["conv", id, opts]);
      return [{ id: 8, creator: BOT, posted_ts: secs("2026-09-07T12:24:01Z") }];
    },
  };
  assert.equal((await findRecentSelfPost(client, { kind: "thread", id: "7882650", botUserId: BOT, nowMs: NOW }))?.messageId, "9");
  assert.equal((await findRecentSelfPost(client, { kind: "conv", id: "42", botUserId: BOT, nowMs: NOW }))?.messageId, "8");
  assert.deepEqual(calls, [
    ["thread", "7882650", { limit: 10 }],
    ["conv", "42", { limit: 10 }],
  ]);
});

test("findRecentSelfPost: an API error or missing client resolves to null, never throws", async () => {
  const client = {
    getThreadComments: async () => {
      throw new Error("HTTP 500");
    },
  };
  assert.equal(await findRecentSelfPost(client, { kind: "thread", id: "1", botUserId: BOT, nowMs: NOW }), null);
  assert.equal(await findRecentSelfPost(undefined, { kind: "thread", id: "1", botUserId: BOT, nowMs: NOW }), null);
});
