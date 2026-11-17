import test from "node:test";
import assert from "node:assert/strict";
import { createOpsAlert } from "../src/ops-alert.js";
import { ALERT_HEADER } from "../src/consumer.js";

const NOW = Date.parse("2031-01-01T00:00:00.000Z"); // far from any real clock, so a now() → Date.now() slip fails
const BOT = 4242;
const TEXT = `${ALERT_HEADER}\nNeeds Hugh: the bot has stopped keeping up with Twist: 51 messages are waiting.`;
const TARGET = { kind: "thread", id: 777 };
const hoursAgo = (h) => (NOW - h * 3600_000) / 1000;
const comment = (over = {}) => ({ creator: BOT, posted_ts: hoursAgo(1), content: TEXT, ...over });

/** A factory over recording fakes. `readRecent` / `post` / `target` can be overridden. */
function harness({ comments = [], target, readRecent, post } = {}) {
  const calls = { target: 0, readRecent: [], post: [], log: [] };
  const alert = createOpsAlert({
    target: () => {
      calls.target += 1;
      return target ? target() : { ...TARGET };
    },
    readRecent: (kind, id) => {
      calls.readRecent.push({ kind, id });
      return readRecent ? readRecent(kind, id) : Promise.resolve(comments);
    },
    post: (arg) => {
      calls.post.push(arg);
      return post ? post(arg) : Promise.resolve({ id: 1 });
    },
    botUserId: BOT,
    log: (s) => calls.log.push(s),
    now: () => NOW,
  });
  return { alert, calls };
}

test("a repeat within 6h is held back: no post, the exact log line, false", async () => {
  const { alert, calls } = harness({ comments: [comment()] });
  assert.equal(await alert(TEXT), false);
  assert.deepEqual(calls.post, []);
  assert.deepEqual(calls.readRecent, [{ kind: "thread", id: 777 }]);
  assert.deepEqual(calls.log, [`alert not repeated (already posted within 6h): ${TEXT.split("\n")[1]}`.slice(0, 200)]);
});

test("a held-back repeat's log line is cut to 200 characters", async () => {
  const long = `${ALERT_HEADER}\nNeeds Hugh: ${"x".repeat(300)}`;
  const { alert, calls } = harness({ comments: [comment({ content: long })] });
  assert.equal(await alert(long), false);
  assert.deepEqual(calls.post, []);
  assert.equal(calls.log.length, 1);
  assert.equal(calls.log[0].length, 200);
  assert.equal(calls.log[0], `alert not repeated (already posted within 6h): ${long.split("\n")[1]}`.slice(0, 200));
});

for (const [name, c] of [
  ["7 h old", comment({ posted_ts: hoursAgo(7) })],
  ["from another creator", comment({ creator: BOT + 1 })],
]) {
  test(`the same comment ${name} does not hold it back: posted once to target(), true`, async () => {
    const { alert, calls } = harness({ comments: [c] });
    assert.equal(await alert(TEXT), true);
    assert.deepEqual(calls.post, [{ kind: "thread", id: 777, text: TEXT }]);
    assert.deepEqual(calls.log, []);
  });
}

test("a post that rejects logs the delivery failure and still resolves true", async () => {
  const err = new Error("twist 503");
  const { alert, calls } = harness({ post: () => Promise.reject(err) });
  assert.equal(await alert(TEXT), true);
  assert.equal(calls.post.length, 1);
  assert.deepEqual(calls.log, [`alert delivery failed: ${String(err)}`]);
});

test("a rejected comment read never blocks the alert: posted, true", async () => {
  const { alert, calls } = harness({ readRecent: () => Promise.reject(new Error("read failed")) });
  assert.equal(await alert(TEXT), true);
  assert.deepEqual(calls.post, [{ kind: "thread", id: 777, text: TEXT }]);
  assert.deepEqual(calls.log, []);
});

test("a comment read that throws synchronously posts nothing, logs, false", async () => {
  const err = new Error("no client");
  const { alert, calls } = harness({
    readRecent: () => {
      throw err;
    },
  });
  assert.equal(await alert(TEXT), false);
  assert.deepEqual(calls.post, []);
  assert.deepEqual(calls.log, [`alert delivery failed: ${String(err)}`]);
});

test("a text without the alert header is posted without reading comments", async () => {
  const { alert, calls } = harness({ comments: [comment({ content: "plain" })] });
  assert.equal(await alert("plain"), true);
  assert.deepEqual(calls.readRecent, []);
  assert.deepEqual(calls.post, [{ kind: "thread", id: 777, text: "plain" }]);
});

test("target() throwing posts nothing, logs, false", async () => {
  const err = new Error("no defaultTo");
  const { alert, calls } = harness({
    target: () => {
      throw err;
    },
  });
  assert.equal(await alert(TEXT), false);
  assert.deepEqual(calls.readRecent, []);
  assert.deepEqual(calls.post, []);
  assert.deepEqual(calls.log, [`alert delivery failed: ${String(err)}`]);
});

test("a conv target reads and posts to that conversation", async () => {
  const { alert, calls } = harness({ target: () => ({ kind: "conv", id: 9 }) });
  assert.equal(await alert(TEXT), true);
  assert.deepEqual(calls.readRecent, [{ kind: "conv", id: 9 }]);
  assert.deepEqual(calls.post, [{ kind: "conv", id: 9, text: TEXT }]);
});
