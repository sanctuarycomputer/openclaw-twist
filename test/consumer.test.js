import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createConsumer, BACKOFF_MS, MAX_ATTEMPTS, MAX_GLOBAL_TURNS, HIGH_WATER, HUNG_TURN_ALERT_MS, REPLAY_HORIZON_MS, isPermanentError, syncSkipReason } from "../src/consumer.js";

const BOT = 634870;
const T0 = 1_785_900_000_000;
const SEC = T0 / 1000; // "now" in Twist's seconds — items are dated relative to this
const MENTION = `[Bot](twist-mention://${BOT}) hello`;

function baseItem(id, over = {}) {
  return {
    id, kind: "conv", peerId: "conv:1", conversationId: 1, messageId: 500,
    objIndex: 0, senderId: 427360, senderName: "Hugh", content: MENTION, postedTs: 1785900000,
    firstSightBacklog: false, state: "queued", attempts: 0, nextAttemptAt: 0, enqueuedAt: T0, ...over,
  };
}
async function harness({ items = [], clock = { t: T0 }, runTurn, probe, classifyPeer, admission, queue: sharedQueue, inFlight, calls: sharedCalls } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistc-"));
  const queue = sharedQueue ?? createQueueStore(join(dir, "queue.json"));
  if (!sharedQueue) await queue.load();
  await queue.enqueueAll(items, clock.t);
  const calls = sharedCalls ?? { turns: [], reacts: [], alerts: [], replies: [] };
  const consumer = createConsumer({
    queue, botUserId: BOT, now: () => clock.t, log: () => {},
    ...(inFlight ? { inFlight } : {}),
    classifyPeer: classifyPeer ?? (async () => "groupdm"),
    admission: admission ?? (async () => ({ admit: true, admission: "dispatch", commandAuthorized: false })),
    runTurn: runTurn ?? (async (item) => { calls.turns.push(item.id); }),
    probe: probe ?? (async () => false),
    react: async (item, verb, emoji) => { calls.reacts.push([item.id, verb, emoji]); },
    alert: async (text) => { calls.alerts.push(text); },
    replyInPlace: async (item, text) => { calls.replies.push([item.id, text]); },
  });
  return { queue, consumer, calls, clock };
}
async function drain(consumer) { await consumer.tick(); await consumer.idle(); await consumer.tick(); await consumer.idle(); } // claim pass + settle pass
// Flush pending microtasks without waiting on any promise to settle. Needed where a
// claimed item's settle() is deliberately left in-flight (blocked on an external gate) —
// consumer.idle() would hang in that case since it awaits ALL tracked settle promises.
const flushMicrotasks = () => new Promise((resolve) => setImmediate(resolve));

test("happy path: mention dispatched, reactions cycled, item done", async () => {
  const { queue, consumer, calls } = await harness({ items: [baseItem("conv-msg:500")] });
  await drain(consumer);
  assert.deepEqual(calls.turns, ["conv-msg:500"]);
  assert.equal(queue.get("conv-msg:500").state, "done");
  assert.deepEqual(calls.reacts.map((r) => `${r[1]}:${r[2]}`), ["add:⏳", "remove:⏳", "add:✅"]);
});

test("policy skips are terminal and reasoned: no-mention / backlog / admission", async () => {
  const { queue, consumer, calls } = await harness({
    items: [
      baseItem("conv-msg:1", { content: "no mention here" }),
      baseItem("conv-msg:2", { peerId: "conv:2", firstSightBacklog: true }),
      baseItem("conv-msg:3", { peerId: "conv:3" }),
    ],
    admission: async (item) => ({ admit: item.id !== "conv-msg:3", admission: item.id === "conv-msg:3" ? "deny" : "dispatch", commandAuthorized: false }),
  });
  await drain(consumer); await drain(consumer);
  assert.equal(queue.get("conv-msg:1").reason, "no-mention");
  assert.equal(queue.get("conv-msg:2").reason, "backlog");
  assert.equal(queue.get("conv-msg:3").reason, "admission:deny");
  for (const id of ["conv-msg:1", "conv-msg:2", "conv-msg:3"]) assert.equal(queue.get(id).state, "skipped");
  assert.equal(calls.turns.length, 0); // none of these should ever have reached runTurn
});

// Forward pagination drains a whole cursor gap, so a long outage can enqueue day-old
// mentions. Answering those publicly on the way back up would be worse than not answering:
// anything older than the replay horizon when it is CLAIMED is recorded skipped:stale.
test("replay horizon: items older than 24h at claim time are skipped, fresher ones dispatch", async () => {
  const { queue, consumer, calls } = await harness({
    items: [
      baseItem("conv-msg:old", { peerId: "conv:old", postedTs: SEC - 25 * 3600 }),
      baseItem("conv-msg:recent", { peerId: "conv:recent", postedTs: SEC - 23 * 3600 }),
    ],
  });
  await drain(consumer); await drain(consumer);
  assert.equal(queue.get("conv-msg:old").state, "skipped");
  assert.equal(queue.get("conv-msg:old").reason, "stale");
  assert.deepEqual(calls.turns, ["conv-msg:recent"]); // the 25h item never reached runTurn
  assert.equal(queue.get("conv-msg:recent").state, "done");
  assert.equal(REPLAY_HORIZON_MS, 24 * 3600 * 1000);
});

test("dms bypass the mention requirement", async () => {
  const { queue, consumer, calls } = await harness({
    items: [baseItem("conv-msg:9", { content: "plain question" })],
    classifyPeer: async () => "dm",
  });
  await drain(consumer);
  assert.deepEqual(calls.turns, ["conv-msg:9"]);
  assert.equal(queue.get("conv-msg:9").state, "done");
});

test("transient failure rides the backoff ladder then dead-letters loudly", async () => {
  const clock = { t: T0 };
  const { queue, consumer, calls } = await harness({
    clock, runTurn: async () => { throw new Error("model exploded"); },
    items: [baseItem("conv-msg:500")],
  });
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    await drain(consumer);
    const it = queue.get("conv-msg:500");
    if (attempt < MAX_ATTEMPTS) {
      assert.equal(it.state, "queued", `attempt ${attempt}`);
      assert.equal(it.nextAttemptAt, clock.t + BACKOFF_MS[attempt - 1]);
      clock.t = it.nextAttemptAt;
    } else {
      assert.equal(it.state, "failed");
    }
  }
  assert.equal(calls.replies.length, 1);            // in-place apology
  assert.equal(calls.alerts.length, 1);             // ops alert
  assert.ok(calls.reacts.some((r) => r[2] === "❌"));
});

// A suppressed incomplete_turn placeholder makes runTurn throw so the item retries instead
// of being recorded answered. That failure lands while the ⏳ is on the message, and the
// next attempt can be an hour out — clearing it in the meantime tells the human "nothing is
// happening here" and then flickers it back. It stays until the item is genuinely terminal.
test("retryable failure keeps the ⏳ on the message; only a terminal outcome clears it", async () => {
  const clock = { t: T0 };
  const { queue, consumer, calls } = await harness({
    clock,
    runTurn: async () => { throw new Error("incomplete-turn fallback suppressed — retrying"); },
    items: [baseItem("conv-msg:500")],
  });
  await drain(consumer);
  assert.equal(queue.get("conv-msg:500").state, "queued");
  assert.equal(queue.get("conv-msg:500").nextAttemptAt, T0 + BACKOFF_MS[0]);
  assert.deepEqual(calls.reacts.map((r) => `${r[1]}:${r[2]}`), ["add:⏳"]); // nothing removed

  for (let attempt = 2; attempt <= MAX_ATTEMPTS; attempt++) {
    clock.t = queue.get("conv-msg:500").nextAttemptAt;
    await drain(consumer);
  }
  assert.equal(queue.get("conv-msg:500").state, "failed");
  assert.equal(calls.reacts.filter((r) => r[1] === "remove" && r[2] === "⏳").length, 1); // exactly once, at the end
  assert.ok(calls.reacts.some((r) => r[2] === "❌"));
  assert.equal(calls.replies.length, 1); // the sender is told, rather than left with a stale ⏳
});

test("permanent errors classify to skipped:gone without retries", async () => {
  const err = Object.assign(new Error("Message not found"), { status: 404 });
  const { queue, consumer } = await harness({ runTurn: async () => { throw err; }, items: [baseItem("conv-msg:500")] });
  await drain(consumer);
  assert.equal(queue.get("conv-msg:500").state, "skipped");
  assert.equal(queue.get("conv-msg:500").reason, "gone");
  assert.equal(isPermanentError(err), true);
  // Ruling: generic 400s are NOT permanent — they ride the retry ladder like anything else.
  assert.equal(isPermanentError(Object.assign(new Error("bad request"), { status: 400 })), false);
});

test("per-peer serialization: second item in same peer waits, then runs", async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const { queue, consumer, calls } = await harness({
    runTurn: async (item) => { calls.turns.push(item.id); if (item.id === "conv-msg:1") await gate; },
    items: [baseItem("conv-msg:1", { postedTs: SEC - 200 }), baseItem("conv-msg:2", { postedTs: SEC - 100 })],
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1", "conv-msg:1"].slice(0, 1)); // only first claimed
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, 1); // still waiting on same peer
  release(); await Promise.resolve(); await drain(consumer);
  assert.equal(calls.turns.length, 2);
  assert.equal(queue.get("conv-msg:2").state, "done");
});

test("boot recovery: orphan with reply-after-claim is done; silent orphan requeues", async () => {
  const { queue, consumer } = await harness({
    items: [baseItem("conv-msg:1", { peerId: "conv:1" }), baseItem("conv-msg:2", { peerId: "conv:2" })],
    probe: async (item) => item.id === "conv-msg:1",
  });
  await queue.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: 1 });
  await queue.transition("conv-msg:2", { state: "processing", claimedAt: T0, attempts: 2 });
  await consumer.recoverOrphans();
  assert.equal(queue.get("conv-msg:1").state, "done");
  assert.equal(queue.get("conv-msg:2").state, "queued");
  assert.equal(queue.get("conv-msg:2").attempts, 2); // attempts preserved
});

// The reply is already on Twist by the time we mark the item done. If THAT write fails and
// the generic failure path requeues the item, the turn re-runs and the human gets a second
// answer. Recording success is therefore not retryable — the item is left "processing" for
// boot recovery, which probes Twist, sees our reply, and settles it without re-dispatching.
test("recorded-success failure does not re-dispatch the turn (no duplicate reply)", async () => {
  const { queue, consumer, calls } = await harness({ items: [baseItem("conv-msg:500")] });
  const realTransition = queue.transition.bind(queue);
  let failed = false;
  queue.transition = async (id, patch, now) => {
    if (patch.state === "done" && !failed) { failed = true; throw new Error("ENOSPC recording done"); }
    return realTransition(id, patch, now);
  };
  await drain(consumer);
  assert.deepEqual(calls.turns, ["conv-msg:500"]);   // ran exactly once
  assert.equal(queue.get("conv-msg:500").state, "processing"); // NOT requeued
  assert.equal(calls.alerts.length, 1);
  assert.match(calls.alerts[0], /could not be recorded done/);

  await drain(consumer); // a later tick must not pick it up either
  assert.deepEqual(calls.turns, ["conv-msg:500"]);
});

// An item whose turn kills the process is poison: recoverOrphans requeuing it
// unconditionally means it re-claims (and re-crashes) on every single boot, forever.
test("boot recovery dead-letters a poison orphan instead of requeueing it forever", async () => {
  const { queue, consumer, calls } = await harness({
    items: [baseItem("conv-msg:1", { attempts: MAX_ATTEMPTS })],
    probe: async () => false,
  });
  await queue.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: MAX_ATTEMPTS });
  await consumer.recoverOrphans();
  assert.equal(queue.get("conv-msg:1").state, "failed"); // not "queued"
  assert.equal(calls.alerts.length, 1);
  assert.match(calls.alerts[0], /failed after 6 attempts/);
  assert.equal(calls.replies.length, 1);                 // in-place apology
  assert.ok(calls.reacts.some((r) => r[2] === "❌"));
});

test("recoverOrphans: a rejecting probe requeues the item rather than throwing", async () => {
  const { queue, consumer } = await harness({
    items: [baseItem("conv-msg:1")],
    probe: async () => { throw new Error("probe blew up"); },
  });
  await queue.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: 1 });
  await assert.doesNotReject(() => consumer.recoverOrphans());
  assert.equal(queue.get("conv-msg:1").state, "queued");
});

test("MAX_GLOBAL_TURNS caps concurrent claims across distinct peers", async () => {
  const gates = [];
  const items = [1, 2, 3, 4].map((n) => baseItem(`g-${n}`, { peerId: `conv:g-${n}` }));
  const { consumer, calls } = await harness({
    items,
    runTurn: async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); },
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS); // only 3 of the 4 distinct-peer items started
  assert.equal(consumer.inFlightCount(), MAX_GLOBAL_TURNS);
  gates.forEach((g) => g());
  await consumer.idle();
});

// An in-process channel restart (stopChannel + startChannel, no drain) leaves the outgoing
// consumer's turns running while a NEW consumer is built over the same queue file. The
// shared store alone does not protect the invariants — a live turn's own item is
// "processing" (so it can't be re-claimed), but OTHER queued items on that peer would still
// look claimable to a consumer whose in-flight map is empty. The shared map is what makes
// per-peer exclusion and the global cap span instances.
test("restart overlap: a second consumer sharing queue+inFlight respects per-peer exclusion", async () => {
  const gates = [];
  const inFlight = new Map();
  const calls = { turns: [], reacts: [], alerts: [], replies: [] };
  const runTurn = async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); };
  // Instance A ("outgoing"): claims peer P's oldest item; its turn never settles.
  const a = await harness({
    inFlight, calls, runTurn,
    items: [baseItem("conv-msg:1", { peerId: "conv:P", postedTs: SEC - 200 }), baseItem("conv-msg:2", { peerId: "conv:P", postedTs: SEC - 100 })],
  });
  await a.consumer.tick();
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1"]);

  // Instance B ("incoming"): same queue store, same in-flight map, fresh consumer.
  const b = await harness({ queue: a.queue, inFlight, calls, runTurn });
  await b.consumer.tick();
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1"]); // conv-msg:2 NOT claimed — peer P is busy
  assert.equal(b.consumer.inFlightCount(), 1);   // B sees A's turn

  gates.forEach((g) => g());
  await a.consumer.idle();
  await b.consumer.tick(); // peer freed: B now runs the second item
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["conv-msg:1", "conv-msg:2"]);
  gates.forEach((g) => g()); // release the second turn's gate too
  await b.consumer.idle();
  assert.equal(b.queue.get("conv-msg:2").state, "done");
});

test("restart overlap: MAX_GLOBAL_TURNS spans consumer instances via the shared inFlight map", async () => {
  const gates = [];
  const inFlight = new Map();
  const calls = { turns: [], reacts: [], alerts: [], replies: [] };
  const runTurn = async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); };
  const a = await harness({
    inFlight, calls, runTurn,
    items: Array.from({ length: MAX_GLOBAL_TURNS }, (_, i) => baseItem(`g-${i}`, { peerId: `conv:g-${i}` })),
  });
  await a.consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS);

  // Fresh consumer, distinct peers still queued — but every global slot is already taken.
  const b = await harness({
    queue: a.queue, inFlight, calls, runTurn,
    items: [baseItem("g-extra", { peerId: "conv:g-extra" })],
  });
  await b.consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS); // no 2×cap during the overlap
  assert.equal(b.queue.get("g-extra").state, "queued");

  gates.forEach((g) => g());
  await a.consumer.idle(); await b.consumer.idle();
});

test("HIGH_WATER alert fires once when queue depth exceeds the threshold, not again next tick", async () => {
  const gates = [];
  const items = Array.from({ length: HIGH_WATER + 1 }, (_, i) => baseItem(`hw-${i}`, { peerId: `conv:hw-${i}` }));
  const { consumer, calls } = await harness({
    items,
    runTurn: async () => new Promise((resolve) => gates.push(resolve)), // never settles: items stay non-terminal
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("high-water")).length, 1);
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("high-water")).length, 1); // no re-alert
  gates.forEach((g) => g());
  await consumer.idle();
});

// ------------------------------------------------------------------- skip-drain

// A thread item, for the drain tests: the flavor decides which sync-cheap check condemns it.
const histItem = (id, i, over = {}) =>
  baseItem(id, { kind: "thread", peerId: "thread:7", threadId: 7, messageId: 900 + i, objIndex: i, postedTs: SEC - 900 + i, ...over });

test("syncSkipReason decides exactly the checks that need no peer lookup", () => {
  const at = (over) => syncSkipReason(histItem("x", 0, over), T0, BOT);
  assert.equal(at({ firstSightBacklog: true }), "backlog");
  assert.equal(at({ postedTs: SEC - 25 * 3600 }), "stale");
  assert.equal(at({ content: "no mention here" }), "no-mention");
  assert.equal(at({}), null);                                            // a fresh thread mention
  assert.equal(at({ kind: "conv", content: "no mention here" }), null);  // dm-vs-groupdm is async
});

// One claim per peer per tick is the right guard for TURNS, but it used to apply to items
// that were never going to get one: a cold thread's history walked past at one item per
// 5s tick, and the @mention sitting behind it waited every one of those ticks for even a ⏳.
test("skip-drain: a whole unanswerable history clears in ONE tick and the mention is claimed in the same pass", async () => {
  const history = Array.from({ length: 15 }, (_, i) => {
    const flavor = i % 3;
    return histItem(`hist-${i}`, i,
      flavor === 0 ? { content: "just chatter" }              // no-mention
      : flavor === 1 ? { firstSightBacklog: true }            // backlog
      : { postedTs: SEC - 25 * 3600 + i });                   // stale
  });
  const mention = histItem("hist-mention", 99, { postedTs: SEC - 10 });
  const { queue, consumer, calls } = await harness({ items: [...history, mention] });

  await consumer.tick();
  await flushMicrotasks();

  const reasons = history.map((h) => queue.get(h.id));
  assert.equal(reasons.every((r) => r.state === "skipped"), true);
  assert.deepEqual(reasons.map((r) => r.reason).filter((r, i, a) => a.indexOf(r) === i).sort(), ["backlog", "no-mention", "stale"]);
  assert.equal(reasons.every((r) => r.attempts === 0), true); // never claimed: no attempt burned
  assert.deepEqual(calls.turns, ["hist-mention"]);            // claimed in the SAME tick
  await consumer.idle();
  assert.equal(queue.get("hist-mention").state, "done");
});

test("inline skips cost neither a peer slot nor turn budget: other peers still fill the global cap", async () => {
  const gates = [];
  const history = Array.from({ length: 15 }, (_, i) => histItem(`hist-${i}`, i, { content: "just chatter" }));
  const mentions = ["A", "B", "C", "D"].map((p, i) =>
    baseItem(`m-${p}`, { kind: "thread", peerId: `thread:${p}`, threadId: p, messageId: 700 + i, objIndex: 99, postedTs: SEC - 100 + i }));
  const { queue, consumer, calls } = await harness({
    items: [...history, ...mentions],
    runTurn: async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); },
  });

  await consumer.tick();
  await flushMicrotasks();

  assert.equal(history.every((h) => queue.get(h.id).state === "skipped"), true);
  assert.deepEqual(calls.turns, ["m-A", "m-B", "m-C"]);  // 15 skips later, all 3 slots still available
  assert.equal(queue.get("m-D").state, "queued");        // and the global cap still bites
  gates.forEach((g) => g());
  await consumer.idle();
});

// dm-vs-groupdm decides whether a mention is even required, and it is an awaited lookup —
// so conversation items must keep going the long way round.
test("conversation items still take the async path (classifyPeer is consulted)", async () => {
  const seen = [];
  const { queue, consumer, calls } = await harness({
    items: [baseItem("conv-msg:1", { content: "plain, no mention" })],
    classifyPeer: async (item) => { seen.push(item.id); return "groupdm"; },
  });
  await drain(consumer);
  assert.deepEqual(seen, ["conv-msg:1"]); // NOT decided inline
  assert.equal(queue.get("conv-msg:1").state, "skipped");
  assert.equal(queue.get("conv-msg:1").reason, "no-mention");
  assert.equal(calls.turns.length, 0);
});

// The drain must not inherit the claim loop's gates: `selectClaimable` returns NOTHING once
// the global turn budget is spent, which is exactly the load this was built for.
test("skip-drain runs with every turn slot occupied: 10 items settle, no turn started", async () => {
  const gates = [];
  const busy = Array.from({ length: MAX_GLOBAL_TURNS }, (_, i) => baseItem(`busy-${i}`, { peerId: `conv:busy-${i}` }));
  const { queue, consumer, calls } = await harness({
    items: busy,
    runTurn: async (item) => { calls.turns.push(item.id); await new Promise((resolve) => gates.push(resolve)); },
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS); // every slot taken, nothing free

  const skippable = Array.from({ length: 10 }, (_, i) => histItem(`late-${i}`, i, { content: "just chatter" }));
  await queue.enqueueAll(skippable, T0);
  await consumer.tick();
  await flushMicrotasks();

  assert.equal(skippable.every((s) => queue.get(s.id).state === "skipped"), true);
  assert.equal(calls.turns.length, MAX_GLOBAL_TURNS); // still no new turn — a skip is not a turn
  gates.forEach((g) => g());
  await consumer.idle();
});

// ...nor the per-peer gate: a claimed mention makes its peer busy, and the siblings queued
// behind it are precisely the history we want gone.
test("skip-drain clears siblings on a peer that is already busy with a claimed turn", async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const { queue, consumer, calls } = await harness({
    items: [histItem("hist-mention", 50)],
    runTurn: async (item) => { calls.turns.push(item.id); await gate; },
  });
  await consumer.tick();
  await flushMicrotasks();
  assert.deepEqual(calls.turns, ["hist-mention"]); // peer thread:7 is now busy

  const siblings = Array.from({ length: 5 }, (_, i) => histItem(`sib-${i}`, 60 + i, { content: "just chatter" }));
  await queue.enqueueAll(siblings, T0);
  await consumer.tick();
  await flushMicrotasks();

  assert.equal(siblings.every((s) => queue.get(s.id).state === "skipped"), true);
  assert.equal(queue.get("hist-mention").state, "processing"); // the live turn is untouched
  release();
  await consumer.idle();
});

// The ingestion ⏳ (added by the producer's fast-ack) has no owner on the skip path — the
// remove-⏳ → ✅/❌ cycle only runs for items that reach a turn. Without this, a mention that
// is acked and then DENIED wears "seen, working on it" forever.
test("a skip clears the ingestion ⏳ — but only for items that could have been acked", async () => {
  const { queue, consumer, calls } = await harness({
    items: [
      baseItem("conv-msg:denied"),                                                    // mention → acked, then denied
      histItem("thread-comment:stale", 1, { postedTs: SEC - 25 * 3600, enqueuedAt: T0 - 25 * 3600 * 1000 }), // mention → acked when fresh, now stale
      baseItem("conv-msg:quiet", { peerId: "conv:quiet", content: "no mention" }),     // never ack-worthy
      histItem("thread-comment:backlog", 2, { firstSightBacklog: true }),              // mention, but backlog is never acked
      // A plain 1:1 DM message: enqueued fresh and fast-acked with NO mention (DMs need
      // none), still queued 25h later, now skipped as stale. No sync-skip path classifies the
      // peer, so `kind` is unknown — treating an unclassified conv item as possibly-acked is
      // the only thing that clears this ⏳.
      baseItem("conv-msg:dm-stale", { peerId: "conv:dm", content: "any news?", postedTs: SEC - 25 * 3600, enqueuedAt: T0 - 25 * 3600 * 1000 }),
      // ...whereas one that was ALREADY stale when it arrived was never acked: no futile
      // reaction call, however many of them an outage drains.
      baseItem("conv-msg:dm-born-stale", { peerId: "conv:born", content: "any news?", postedTs: SEC - 25 * 3600, enqueuedAt: T0 }),
    ],
    admission: async () => ({ admit: false, admission: "deny", commandAuthorized: false }),
  });
  await drain(consumer);
  const cleared = calls.reacts.filter((r) => r[1] === "remove" && r[2] === "⏳").map((r) => r[0]).sort();
  assert.deepEqual(cleared, ["conv-msg:denied", "conv-msg:dm-stale", "thread-comment:stale"]);
  assert.equal(calls.reacts.some((r) => ["conv-msg:quiet", "thread-comment:backlog", "conv-msg:dm-born-stale"].includes(r[0])), false);
  assert.equal(queue.get("conv-msg:denied").reason, "admission:deny");
});

test("hung-turn alert fires once after HUNG_TURN_ALERT_MS, not again next tick", async () => {
  const clock = { t: T0 };
  let release;
  const gate = new Promise((r) => { release = r; });
  const { consumer, calls } = await harness({
    clock, items: [baseItem("conv-msg:1")],
    runTurn: async () => { await gate; },
  });
  await consumer.tick();
  await flushMicrotasks();
  clock.t += HUNG_TURN_ALERT_MS + 1;
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("running >")).length, 1);
  await consumer.tick();
  await flushMicrotasks();
  assert.equal(calls.alerts.filter((a) => a.includes("running >")).length, 1); // no re-alert
  release();
  await consumer.idle();
});
