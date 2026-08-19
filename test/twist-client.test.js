import test from "node:test";
import assert from "node:assert/strict";
import { createTwistClient } from "../src/twist-client.js";

const TOKEN = "oauth2:test-token";
const WORKSPACE = 133876;

const res = (status, body = "{}", headers = {}) => ({
  status,
  ok: status >= 200 && status < 300,
  headers: { get: (k) => headers[k] },
  text: async () => body,
});

// The 429/5xx retry ladder used to sleep with a plain setTimeout, so an abort signal could
// not interrupt it: a caller's deadline would not fire until up to ~7s of dead sleep had
// elapsed per attempt, and meanwhile its container guard and fast-path slot stayed pinned.
test("client: a retry sleep is interrupted by an abort signal", async () => {
  const controller = new AbortController();
  let calls = 0;
  const client = createTwistClient({
    token: TOKEN,
    workspaceId: WORKSPACE,
    fetchImpl: async () => {
      calls++;
      // Abort mid-backoff, exactly as a deadline would.
      setTimeout(() => controller.abort(new Error("deadline")), 5);
      return res(429, "", { "retry-after": "30" }); // 30s of backoff to sit in
    },
  });

  const started = Date.now();
  await assert.rejects(() => client.getUnreadThreads(controller.signal), /deadline/);
  const elapsed = Date.now() - started;

  assert.ok(elapsed < 1000, `should abandon the backoff promptly, took ${elapsed}ms`);
  assert.equal(calls, 1, "and must not go round the ladder again after aborting");
});

test("client: an already-aborted signal fails the retry sleep without waiting", async () => {
  const controller = new AbortController();
  controller.abort(new Error("already gone"));
  const client = createTwistClient({
    token: TOKEN,
    workspaceId: WORKSPACE,
    fetchImpl: async () => res(503),
  });

  const started = Date.now();
  await assert.rejects(() => client.getUnreadThreads(controller.signal), /already gone/);
  assert.ok(Date.now() - started < 500);
});

test("client: without a signal the retry ladder still works", async () => {
  let calls = 0;
  const client = createTwistClient({
    token: TOKEN,
    workspaceId: WORKSPACE,
    fetchImpl: async () => {
      calls++;
      return calls === 1 ? res(503) : res(200, '[{"thread_id":7}]');
    },
  });

  assert.deepEqual(await client.getUnreadThreads(), [{ thread_id: 7 }]);
  assert.equal(calls, 2, "one retry, then success");
});

test("client: reactions forward the caller's signal to the request", async () => {
  const controller = new AbortController();
  const seen = [];
  const client = createTwistClient({
    token: TOKEN,
    workspaceId: WORKSPACE,
    fetchImpl: async (_url, opts) => {
      seen.push(opts.signal);
      return res(200);
    },
  });

  await client.addReaction({ commentId: 88, reaction: "⏳" }, controller.signal);
  await client.removeReaction({ messageId: 5, reaction: "⏳" }, controller.signal);

  assert.deepEqual(seen, [controller.signal, controller.signal]);
});
