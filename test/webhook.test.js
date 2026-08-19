import test from "node:test";
import assert from "node:assert/strict";
import {
  createHintDebouncer,
  createWebhookHandler,
  extractContainerHint,
  extractRequestToken,
  hintKey,
} from "../src/webhook.js";

// ---------------------------------------------------------------- fake timers

/** Deterministic timer harness: nothing here sleeps, tests advance the clock by hand. */
function fakeTimers() {
  let now = 0;
  let seq = 0;
  const scheduled = new Map(); // handle -> {at, fn}
  return {
    setTimer: (fn, ms) => {
      const handle = ++seq;
      scheduled.set(handle, { at: now + ms, fn });
      return handle;
    },
    clearTimer: (handle) => scheduled.delete(handle),
    /** Advance the clock, firing everything due, in due order. */
    advance(ms) {
      now += ms;
      for (;;) {
        const due = [...scheduled.entries()].filter(([, t]) => t.at <= now).sort((a, b) => a[1].at - b[1].at);
        if (due.length === 0) return;
        for (const [handle, t] of due) {
          scheduled.delete(handle);
          t.fn();
        }
      }
    },
    pendingCount: () => scheduled.size,
  };
}

// ------------------------------------------------------- extractContainerHint

test("extractContainerHint: comment_added yields the THREAD id, never the comment id", () => {
  // `id` here is the comment — reading it as a thread id would sweep the wrong container.
  const hint = extractContainerHint({ event_type: "comment_added", id: 999, thread_id: 7882650 });
  assert.deepEqual(hint, { kind: "thread", id: "7882650" });
});

test("extractContainerHint: thread_added yields the thread's own id", () => {
  assert.deepEqual(extractContainerHint({ event_type: "thread_added", id: 4242, channel_id: 77 }), {
    kind: "thread",
    id: "4242",
  });
});

test("extractContainerHint: thread_updated and comment_updated resolve the same way", () => {
  assert.deepEqual(extractContainerHint({ event_type: "thread_updated", id: 51 }), { kind: "thread", id: "51" });
  assert.deepEqual(extractContainerHint({ event_type: "comment_updated", thread_id: 52, id: 8 }), {
    kind: "thread",
    id: "52",
  });
});

test("extractContainerHint: message_added yields the CONVERSATION id", () => {
  assert.deepEqual(extractContainerHint({ event_type: "message_added", id: 12, conversation_id: 34567 }), {
    kind: "conversation",
    id: "34567",
  });
});

test("extractContainerHint: conversation_added yields the conversation's own id", () => {
  assert.deepEqual(extractContainerHint({ event_type: "conversation_added", id: 34568 }), {
    kind: "conversation",
    id: "34568",
  });
});

test("extractContainerHint: a comment_added carrying only an id (no thread_id) is NOT guessed", () => {
  // Better a full-poll fallback than a sweep of whatever container happens to share that id.
  assert.equal(extractContainerHint({ event_type: "comment_added", id: 999 }), null);
});

test("extractContainerHint: string ids are accepted and normalized", () => {
  assert.deepEqual(extractContainerHint({ event_type: "comment_added", thread_id: " 7882650 " }), {
    kind: "thread",
    id: "7882650",
  });
});

test("extractContainerHint: an unknown event name still resolves via the generic field rule", () => {
  assert.deepEqual(extractContainerHint({ event_type: "comment_reacted", thread_id: 61 }), {
    kind: "thread",
    id: "61",
  });
  assert.deepEqual(extractContainerHint({ event_type: "some_future_event", conversation_id: 62 }), {
    kind: "conversation",
    id: "62",
  });
});

test("extractContainerHint: a payload with no event name at all still resolves", () => {
  assert.deepEqual(extractContainerHint({ thread_id: 63 }), { kind: "thread", id: "63" });
});

test("extractContainerHint: event_name / event aliases are accepted", () => {
  assert.deepEqual(extractContainerHint({ event_name: "message_added", conversation_id: 71 }), {
    kind: "conversation",
    id: "71",
  });
  assert.deepEqual(extractContainerHint({ event: "message_added", conversation_id: 72 }), {
    kind: "conversation",
    id: "72",
  });
});

test("extractContainerHint: a nested `data` envelope is unwrapped", () => {
  assert.deepEqual(extractContainerHint({ event_type: "comment_added", data: { thread_id: 81, id: 5 } }), {
    kind: "thread",
    id: "81",
  });
});

test("extractContainerHint: thread wins over conversation when a payload carries both", () => {
  assert.deepEqual(extractContainerHint({ thread_id: 91, conversation_id: 92 }), { kind: "thread", id: "91" });
});

test("extractContainerHint: missing fields yield null (→ full-poll fallback)", () => {
  assert.equal(extractContainerHint({ event_type: "comment_added" }), null);
  assert.equal(extractContainerHint({}), null);
});

test("extractContainerHint: garbage bodies yield null instead of throwing", () => {
  for (const body of [null, undefined, "", "not json", 42, true, [], [{ thread_id: 1 }], () => {}]) {
    assert.equal(extractContainerHint(body), null, `expected null for ${JSON.stringify(body) ?? String(body)}`);
  }
});

test("extractContainerHint: garbage id VALUES are rejected, not interpolated", () => {
  // These would otherwise reach an authenticated API query as a path/query fragment.
  const garbage = [
    "../../admin",
    "1 OR 1=1",
    "7882650; DROP",
    "abc",
    "",
    "  ",
    "0",
    "-5",
    "1.5",
    "1e9",
    Number.NaN,
    Infinity,
    -1,
    0,
    1.5,
    { id: 1 },
    [1],
    true,
    null,
  ];
  for (const value of garbage) {
    assert.equal(
      extractContainerHint({ event_type: "comment_added", thread_id: value }),
      null,
      `expected null for thread_id=${String(value)}`,
    );
  }
});

test("extractContainerHint: an absurdly long numeric string is rejected", () => {
  assert.equal(extractContainerHint({ thread_id: "1".repeat(64) }), null);
});

test("hintKey: distinguishes kinds sharing an id", () => {
  assert.notEqual(hintKey({ kind: "thread", id: "1" }), hintKey({ kind: "conversation", id: "1" }));
});

// -------------------------------------------------------- createHintDebouncer

test("debouncer: a burst for one container flushes exactly once, at the end of the window", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h), ...timers });

  assert.equal(d.push({ kind: "thread", id: "1" }), "armed");
  assert.equal(d.push({ kind: "thread", id: "1" }), "coalesced");
  assert.equal(d.push({ kind: "thread", id: "1" }), "coalesced");

  timers.advance(1999);
  assert.deepEqual(flushed, [], "must not flush before the window closes");
  timers.advance(1);
  assert.deepEqual(flushed, [{ kind: "thread", id: "1" }], "one sweep for the whole burst");
});

test("debouncer: separate containers keep independent windows", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(hintKey(h)), ...timers });

  d.push({ kind: "thread", id: "1" });
  timers.advance(1000);
  d.push({ kind: "conversation", id: "1" }); // same id, different kind → its own window
  d.push({ kind: "thread", id: "2" });

  timers.advance(1000); // thread:1's window closes; the other two are mid-window
  assert.deepEqual(flushed, ["thread:1"]);
  timers.advance(1000);
  assert.deepEqual(flushed, ["thread:1", "conversation:1", "thread:2"]);
});

test("debouncer: the window is NOT extended by later hints (a busy container cannot starve)", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h), ...timers });

  d.push({ kind: "thread", id: "1" });
  timers.advance(1500);
  d.push({ kind: "thread", id: "1" }); // a reset-style debounce would push the flush to 3500
  timers.advance(500);
  assert.equal(flushed.length, 1, "flush lands 2000ms after the FIRST hint");
});

test("debouncer: a new burst after a flush arms a fresh window", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h), ...timers });

  d.push({ kind: "thread", id: "1" });
  timers.advance(2000);
  assert.equal(d.push({ kind: "thread", id: "1" }), "armed");
  timers.advance(2000);
  assert.equal(flushed.length, 2);
});

test("debouncer: the LAST hint for a container is the one flushed", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 10, onFlush: (h) => flushed.push(h), ...timers });
  d.push({ kind: "thread", id: "1", seq: 1 });
  d.push({ kind: "thread", id: "1", seq: 2 });
  timers.advance(10);
  assert.equal(flushed[0].seq, 2);
});

test("debouncer: cancelAll drops armed windows without flushing (shutdown)", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h), ...timers });
  d.push({ kind: "thread", id: "1" });
  d.push({ kind: "conversation", id: "2" });
  assert.equal(d.size(), 2);

  d.cancelAll();
  assert.equal(d.size(), 0);
  assert.equal(timers.pendingCount(), 0, "timers are cleared, not left to fire after stop()");
  timers.advance(10_000);
  assert.deepEqual(flushed, []);
});

test("debouncer: a throwing onFlush is contained and does not wedge later windows", () => {
  const timers = fakeTimers();
  const logs = [];
  const seen = [];
  const d = createHintDebouncer({
    delayMs: 10,
    log: (m) => logs.push(m),
    onFlush: (h) => {
      seen.push(hintKey(h));
      if (h.id === "1") throw new Error("boom");
    },
    ...timers,
  });
  d.push({ kind: "thread", id: "1" });
  d.push({ kind: "thread", id: "2" });
  timers.advance(10);

  assert.deepEqual(seen, ["thread:1", "thread:2"]);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /boom/);
});

test("debouncer: a rejecting async onFlush is contained", async () => {
  const timers = fakeTimers();
  const logs = [];
  const d = createHintDebouncer({
    delayMs: 10,
    log: (m) => logs.push(m),
    onFlush: async () => {
      throw new Error("async boom");
    },
    ...timers,
  });
  d.push({ kind: "thread", id: "1" });
  timers.advance(10);
  await new Promise((r) => setImmediate(r));
  assert.equal(logs.length, 1);
  assert.match(logs[0], /async boom/);
});

test("debouncer: onFlush is required", () => {
  assert.throws(() => createHintDebouncer({ delayMs: 10 }), /onFlush is required/);
});

// -------------------------------------------------------- extractRequestToken

test("extractRequestToken: reads ?token= from the URL (the shape Twist can actually send)", () => {
  assert.equal(extractRequestToken("/twist/events?token=s3cret"), "s3cret");
  assert.equal(extractRequestToken("/twist/events?foo=1&token=s3cret&bar=2"), "s3cret");
});

test("extractRequestToken: falls back to headers when no query token is present", () => {
  assert.equal(extractRequestToken("/twist/events", { "x-twist-webhook-token": "hdr" }), "hdr");
  assert.equal(extractRequestToken("/twist/events", { authorization: "Bearer bear" }), "bear");
  assert.equal(extractRequestToken("/twist/events", { authorization: "bearer bear" }), "bear");
});

test("extractRequestToken: query wins over headers", () => {
  assert.equal(extractRequestToken("/e?token=q", { "x-twist-webhook-token": "h" }), "q");
});

test("extractRequestToken: absent/blank/garbage inputs yield the empty string, not undefined", () => {
  assert.equal(extractRequestToken("/twist/events"), "");
  assert.equal(extractRequestToken("/twist/events?token="), "");
  assert.equal(extractRequestToken("/twist/events?token=%20%20"), "");
  assert.equal(extractRequestToken(undefined), "");
  assert.equal(extractRequestToken("/e", { authorization: "Basic abc" }), "");
});

// -------------------------------------------------------- createWebhookHandler

const handlerHarness = (opts = {}) => {
  const scheduled = [];
  const logs = [];
  const handler = createWebhookHandler({
    extract: extractContainerHint,
    schedule: (d) => scheduled.push(d),
    log: (m) => logs.push(m),
    ...opts,
  });
  return { handler, scheduled, logs };
};

test("handler: a valid hint schedules exactly one targeted sweep", () => {
  const { handler, scheduled } = handlerHarness();
  const decision = handler({ event_type: "comment_added", thread_id: 7882650, id: 3 });

  assert.deepEqual(decision, { action: "sweep", hint: { kind: "thread", id: "7882650" } });
  assert.equal(scheduled.length, 1);
  assert.deepEqual(scheduled[0], decision);
});

test("handler: a message_added schedules a conversation sweep", () => {
  const { handler, scheduled } = handlerHarness();
  assert.deepEqual(handler({ event_type: "message_added", conversation_id: 34567 }), {
    action: "sweep",
    hint: { kind: "conversation", id: "34567" },
  });
  assert.equal(scheduled.length, 1);
});

test("handler: a bad-shaped body falls back to a full poll (still scheduled, never an error)", () => {
  for (const body of [{}, null, "garbage", [], { event_type: "comment_added" }, { thread_id: "../x" }]) {
    const { handler, scheduled } = handlerHarness();
    assert.deepEqual(handler(body), { action: "poll" }, `body=${JSON.stringify(body) ?? String(body)}`);
    assert.deepEqual(scheduled, [{ action: "poll" }]);
  }
});

test("handler: a throwing extractor degrades to the full-poll fallback and logs", () => {
  const { handler, scheduled, logs } = handlerHarness({
    extract: () => {
      throw new Error("extractor bug");
    },
  });
  assert.deepEqual(handler({ thread_id: 1 }), { action: "poll" });
  assert.deepEqual(scheduled, [{ action: "poll" }]);
  assert.match(logs.join("\n"), /extractor bug/);
});

test("handler: a throwing schedule is contained — the decision still returns so the route can 200", () => {
  const logs = [];
  const handler = createWebhookHandler({
    extract: extractContainerHint,
    log: (m) => logs.push(m),
    schedule: () => {
      throw new Error("queue full");
    },
  });
  assert.deepEqual(handler({ thread_id: 5 }), { action: "sweep", hint: { kind: "thread", id: "5" } });
  assert.match(logs.join("\n"), /queue full/);
});

test("handler: with verifyToken configured, a bad token schedules NOTHING", () => {
  const { handler, scheduled, logs } = handlerHarness({ verifyToken: (t) => t === "good" });

  assert.deepEqual(handler({ thread_id: 5 }, { token: "bad" }), { action: "unauthorized" });
  assert.deepEqual(handler({ thread_id: 5 }), { action: "unauthorized" }, "absent token is not a pass");
  assert.deepEqual(scheduled, [], "an unauthenticated delivery must never reach the sweep queue");
  assert.equal(logs.length, 2);

  assert.deepEqual(handler({ thread_id: 5 }, { token: "good" }), {
    action: "sweep",
    hint: { kind: "thread", id: "5" },
  });
  assert.equal(scheduled.length, 1);
});

test("handler: without verifyToken the token is ignored (the HTTP layer already authenticated)", () => {
  const { handler, scheduled } = handlerHarness();
  handler({ thread_id: 5 }, { token: "whatever" });
  assert.equal(scheduled.length, 1);
});

test("handler: schedule is required", () => {
  assert.throws(() => createWebhookHandler({ extract: extractContainerHint }), /schedule is required/);
});

// --------------------------------------------- debouncer + handler, end to end

test("handler → debouncer: an event burst on one thread produces ONE sweep", () => {
  const timers = fakeTimers();
  const sweeps = [];
  const debouncer = createHintDebouncer({ delayMs: 2000, onFlush: (h) => sweeps.push(h), ...timers });
  const handler = createWebhookHandler({
    extract: extractContainerHint,
    schedule: (d) => debouncer.push(d.action === "sweep" ? d.hint : { kind: "all", id: "*" }),
  });

  handler({ event_type: "comment_added", thread_id: 7882650, id: 1 });
  handler({ event_type: "comment_added", thread_id: 7882650, id: 2 });
  handler({ event_type: "thread_updated", id: 7882650 });
  handler({ event_type: "message_added", conversation_id: 34567 });
  handler({ garbage: true }); // → the "all" fallback
  handler("also garbage"); // → coalesces with the previous fallback

  timers.advance(2000);
  assert.deepEqual(sweeps, [
    { kind: "thread", id: "7882650" },
    { kind: "conversation", id: "34567" },
    { kind: "all", id: "*" },
  ]);
});
