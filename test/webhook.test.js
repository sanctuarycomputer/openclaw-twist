import test from "node:test";
import assert from "node:assert/strict";
import {
  MIN_WEBHOOK_TOKEN_LENGTH,
  createHintDebouncer,
  createWebhookHandler,
  extractContainerHint,
  extractRequestToken,
  hintKey,
  resolveWebhookIngressConfig,
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

// sweepContainer coerces the id back to a Number, so the accepted width must round-trip
// without precision loss. 15 digits is the widest that always does; real ids are ~7.
test("extractContainerHint: accepted ids survive the Number() round-trip sweepContainer does", () => {
  const widest = "9".repeat(15);
  assert.deepEqual(extractContainerHint({ thread_id: widest }), { kind: "thread", id: widest });
  assert.equal(String(Number(widest)), widest, "and it round-trips exactly");

  assert.equal(extractContainerHint({ thread_id: "9".repeat(16) }), null, "16 digits is refused");
});

test("hintKey: distinguishes kinds sharing an id", () => {
  assert.notEqual(hintKey({ kind: "thread", id: "1" }), hintKey({ kind: "conversation", id: "1" }));
});

// -------------------------------------------------------- createHintDebouncer

// The debouncer is LEADING-EDGE: the first hint for an idle container flushes at once, and
// only the hints behind it wait for the coalescing window. Measured live, the old flat
// trailing debounce added ~2s to the ⏳ a human waits for, with nothing to coalesce yet.

test("debouncer: a lone hint flushes immediately — nothing waits on the window", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h), ...timers });

  assert.equal(d.push({ kind: "thread", id: "1" }), "leading");
  assert.deepEqual(flushed, [], "still asynchronous — push never flushes inline");

  timers.advance(0);
  assert.deepEqual(flushed, [{ kind: "thread", id: "1" }], "the leading flush lands on the next tick, not 2s later");

  timers.advance(5000);
  assert.equal(flushed.length, 1, "and no trailing flush, because nothing arrived behind it");
});

test("debouncer: a burst of 5 in one window produces exactly two flushes (leading + one trailing)", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h.seq), ...timers });

  assert.equal(d.push({ kind: "thread", id: "1", seq: 1 }), "leading");
  for (const seq of [2, 3, 4, 5]) {
    assert.equal(d.push({ kind: "thread", id: "1", seq }), "coalesced");
  }

  timers.advance(0);
  assert.deepEqual(flushed, [1], "the first hint went straight through");
  timers.advance(1999);
  assert.deepEqual(flushed, [1], "the rest wait for the window to close");
  timers.advance(1);
  assert.deepEqual(flushed, [1, 5], "one trailing flush, carrying the newest hint of the burst");
});

test("debouncer: the window resets after a trailing flush — the next hint leads again", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h.seq), ...timers });

  d.push({ kind: "thread", id: "1", seq: 1 });
  d.push({ kind: "thread", id: "1", seq: 2 });
  timers.advance(2000);
  assert.deepEqual(flushed, [1, 2]);
  assert.equal(d.size(), 0, "the container is idle again");

  assert.equal(d.push({ kind: "thread", id: "1", seq: 3 }), "leading");
  timers.advance(0);
  assert.deepEqual(flushed, [1, 2, 3], "a hint after the window is a fresh leading flush, not a wait");
});

test("debouncer: a quiet window closes with no trailing flush, and the next hint still leads", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h.seq), ...timers });

  d.push({ kind: "thread", id: "1", seq: 1 });
  timers.advance(2000);
  assert.deepEqual(flushed, [1], "exactly one flush for one hint");

  assert.equal(d.push({ kind: "thread", id: "1", seq: 2 }), "leading");
  timers.advance(0);
  assert.deepEqual(flushed, [1, 2]);
});

test("debouncer: containers are independent — each gets its own leading flush and window", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(hintKey(h)), ...timers });

  d.push({ kind: "thread", id: "1" });
  timers.advance(0);
  assert.deepEqual(flushed, ["thread:1"]);

  timers.advance(1000); // t=1000; thread:1's window is open until t=2000
  d.push({ kind: "conversation", id: "1" }); // same id, different kind → its own window
  d.push({ kind: "thread", id: "2" });
  assert.equal(d.push({ kind: "thread", id: "1" }), "coalesced", "joins the window already open for it");

  timers.advance(0);
  assert.deepEqual(flushed, ["thread:1", "conversation:1", "thread:2"], "the two new containers lead immediately");

  timers.advance(1000); // t=2000: thread:1's window closes, carrying its trailing hint
  assert.deepEqual(flushed, ["thread:1", "conversation:1", "thread:2", "thread:1"]);

  timers.advance(1000); // t=3000: the other two windows close — nothing arrived behind them
  assert.equal(flushed.length, 4, "a quiet window never emits");
});

test("debouncer: the trailing window is NOT extended by later hints (a busy container cannot starve)", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 2000, onFlush: (h) => flushed.push(h.seq), ...timers });

  d.push({ kind: "thread", id: "1", seq: 1 });
  timers.advance(0);
  assert.deepEqual(flushed, [1]);

  timers.advance(1500);
  d.push({ kind: "thread", id: "1", seq: 2 }); // reset semantics would push the trailing flush to t=3500
  timers.advance(500); // t=2000

  assert.deepEqual(flushed, [1, 2], "the trailing flush is anchored to the LEADING hint, and never moves");
});

test("debouncer: a hammered container still flushes at a steady cadence (no starvation)", () => {
  const timers = fakeTimers();
  const flushed = [];
  const d = createHintDebouncer({ delayMs: 1000, onFlush: (h) => flushed.push(h.seq), ...timers });

  // An event every 100ms for 5s — the shape that starves a reset-on-every-hint debounce.
  for (let seq = 0; seq < 50; seq++) {
    d.push({ kind: "thread", id: "1", seq });
    timers.advance(100);
  }
  assert.ok(flushed.length >= 5, `expected steady flushes, got ${flushed.length}`);
  assert.equal(flushed[0], 0, "and the very first event was not delayed at all");
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
  assert.equal(timers.pendingCount(), 0, "both the leading and window timers are cleared, not left to fire after stop()");
  timers.advance(10_000);
  assert.deepEqual(flushed, [], "a leading flush that had not yet fired must not fire after shutdown");
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

test("handler → debouncer: a burst collapses to one leading + one trailing sweep per container", () => {
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

  // Leading edge: each of the three distinct containers sweeps at once...
  timers.advance(0);
  assert.deepEqual(sweeps, [
    { kind: "thread", id: "7882650" },
    { kind: "conversation", id: "34567" },
    { kind: "all", id: "*" },
  ]);

  // ...and the hints that piled up behind them collapse into one trailing sweep each. The
  // conversation had no second event, so it emits nothing further.
  timers.advance(2000);
  assert.deepEqual(sweeps.slice(3), [
    { kind: "thread", id: "7882650" },
    { kind: "all", id: "*" },
  ]);
});

// ---------------------------------------------- resolveWebhookIngressConfig

test("ingress config: both halves present and a long token → enabled", () => {
  const token = "a".repeat(MIN_WEBHOOK_TOKEN_LENGTH);
  assert.deepEqual(resolveWebhookIngressConfig({ path: "/twist/events", token }), {
    enabled: true,
    path: "/twist/events",
    token,
  });
});

test("ingress config: a short token is REFUSED, not silently accepted", () => {
  // The token rides in the URL and is the endpoint's only authentication, so a weak one is
  // a real hazard — refusing costs only latency, since poll is the source of truth.
  for (const token of ["short", "hunter2", "a".repeat(MIN_WEBHOOK_TOKEN_LENGTH - 1)]) {
    assert.deepEqual(
      resolveWebhookIngressConfig({ path: "/twist/events", token }),
      { enabled: false, reason: "token-too-short", path: "/twist/events" },
      `expected refusal for a ${token.length}-char token`,
    );
  }
});

test("ingress config: exactly the minimum length is accepted (boundary)", () => {
  assert.equal(resolveWebhookIngressConfig({ path: "/e", token: "b".repeat(MIN_WEBHOOK_TOKEN_LENGTH) }).enabled, true);
  assert.equal(resolveWebhookIngressConfig({ path: "/e", token: "b".repeat(MIN_WEBHOOK_TOKEN_LENGTH - 1) }).enabled, false);
});

test("ingress config: either half missing → unconfigured (never an open route)", () => {
  const long = "c".repeat(40);
  for (const input of [
    { path: "/twist/events" },
    { path: "/twist/events", token: "" },
    { path: "/twist/events", token: "   " },
    { token: long },
    { path: "", token: long },
    { path: "   ", token: long },
    {},
    undefined,
  ]) {
    const got = resolveWebhookIngressConfig(input);
    assert.equal(got.enabled, false, `expected disabled for ${JSON.stringify(input)}`);
    assert.equal(got.reason, "unconfigured");
  }
});

test("ingress config: a path with no token never falls back to an unauthenticated route", () => {
  const got = resolveWebhookIngressConfig({ path: "/twist/events", token: undefined });
  assert.equal(got.enabled, false);
  assert.equal(got.token, undefined);
});

test("ingress config: surrounding whitespace is trimmed off both values", () => {
  const token = `  ${"d".repeat(30)}  `;
  const got = resolveWebhookIngressConfig({ path: "  /twist/events  ", token });
  assert.deepEqual(got, { enabled: true, path: "/twist/events", token: token.trim() });
});

test("ingress config: non-string inputs are refused rather than coerced", () => {
  for (const input of [
    { path: 42, token: "e".repeat(30) },
    { path: "/e", token: 999999999999999999999999 },
    { path: ["/e"], token: "e".repeat(30) },
    { path: "/e", token: { value: "e".repeat(30) } },
  ]) {
    assert.equal(resolveWebhookIngressConfig(input).enabled, false, `expected disabled for ${JSON.stringify(input)}`);
  }
});
