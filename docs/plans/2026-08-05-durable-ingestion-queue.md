# Durable Ingestion Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the at-most-once cursor ingestion in openclaw-twist with a persistent idempotent job queue so every fetched Twist message reaches a terminal outcome (done / skipped-with-reason / failed-loudly), across crashes and errors.

**Architecture:** Three units per the spec (`docs/specs/2026-08-05-durable-ingestion-queue-design.md`): a transport-only **producer** (fetch + idempotent enqueue by Twist id), a **queue store** (persistent state machine: `queued → processing → done|skipped|failed`), and a **consumer** (per-peer serialized claims, recorded policy verdicts, retry ladder, boot-only orphan probing). `monitor.js` shrinks to wiring.

**Tech Stack:** Plain Node ESM (>=20), zero runtime dependencies, `node --test` + `node:assert`. Persistence = JSON file with tmp+fsync+rename, matching `src/state.js`.

## Global Constraints

- Zero runtime dependencies; only `node:` builtins (spec: same zero-dependency pattern as the cursor store).
- Node >= 20 (`package.json` engines) — no `node:sqlite`.
- All new logic that needs time takes an injected `now()` (ms epoch); never call `Date.now()` inside `src/queue.js` or `src/consumer.js` bodies (testability). Twist `posted_ts` is SECONDS — convert at comparison sites (`postedTs * 1000`).
- `src/inbound.js` imports the openclaw SDK and cannot be unit-tested locally; anything needing tests must live in `queue.js` / `producer.js` / `consumer.js` behind injected function params.
- Retry ladder (spec): `BACKOFF_MS = [30_000, 120_000, 600_000, 3_600_000, 3_600_000]`, `MAX_ATTEMPTS = 6`.
- Concurrency (spec): 1 turn per peer, `MAX_GLOBAL_TURNS = 3`.
- First-sight backlog grace stays `FIRST_SIGHT_GRACE_MS = 2h` (exists in monitor.js).
- Queue depth high-water: 50 non-terminal items → one-shot ops alert. Terminal prune: 30 days.
- Existing `test/routing.test.js` (19 tests) must stay green; run the full suite (`node --test`) at every task end.
- Commit style: `feat:`/`fix:`/`refactor:` + body, `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

## File Structure

- Create `src/queue.js` — queue store: persistence, idempotent enqueue, transitions, claim selection, orphans, prune. No I/O besides its own file; no Twist knowledge beyond the item shape.
- Create `src/producer.js` — poll sweep: fetch unread convs/threads, enqueue everything new, thread-post synthesis, backlog flagging, cursor + read-marker advance. Takes `client`, `cursors`, `queue` injected.
- Create `src/consumer.js` — claim loop, policy verdicts, dispatch orchestration, retry/dead-letter, boot recovery. All effects injected (`runTurn`, `probe`, `react`, `alert`, `classifyPeer`, `admission`, `now`).
- Modify `src/monitor.js` — wiring only: build store/producer/consumer, implement the injected effect functions from existing helpers (`fetchThreadContext`, `buildMessage`, reactions, `handleTwistInbound`), poll loop = `producer.pollOnce()` then `consumer.tick()`.
- Modify `src/inbound.js` — extract the admission gate so the consumer records its verdict pre-dispatch.
- Modify `src/twist-client.js` — add `getThread` if absent (thread-post synthesis needs the body).
- Tests: create `test/queue.test.js`, `test/producer.test.js`, `test/consumer.test.js`.

### Item shape (canonical, used by every task)

```js
{
  id: "conv-msg:<msgId>" | "thread-comment:<commentId>" | "thread-post:<threadId>",
  kind: "conv" | "thread" | "thread-post",
  peerId: "conv:<conversationId>" | "thread:<threadId>",
  conversationId, threadId, channelId,   // numbers or undefined
  messageId,                              // raw Twist id (reactions target)
  objIndex, senderId, senderName, content, postedTs,  // postedTs = Twist seconds
  firstSightBacklog,                      // boolean, set by producer
  state, attempts, nextAttemptAt,         // "queued", 0, 0 at enqueue
  enqueuedAt, claimedAt, finishedAt,      // ms epoch (claimedAt/finishedAt undefined until set)
  reason, lastError                       // strings, undefined until set
}
```

---

### Task 1: Queue store — persistence, idempotent enqueue, transitions, prune

**Files:**
- Create: `src/queue.js`
- Test: `test/queue.test.js`

**Interfaces:**
- Consumes: nothing (leaf unit).
- Produces: `createQueueStore(filePath)` → `{ load(), has(id), get(id), enqueueAll(items[], now) -> number, transition(id, patch) -> item, itemsInState(state) -> item[], nonTerminalCount() -> number, prune(now) }`. States: `queued|processing|done|skipped|failed`; terminal = `done|skipped|failed`. `enqueueAll` persists ONCE for the batch and returns how many were new; duplicates (live or tombstoned) are no-ops. `transition` merges the patch, stamps `finishedAt` when the patch's state is terminal, persists, returns the updated item. `prune` moves terminal items older than 30 days to a `tombstones` id-set.

- [ ] **Step 1: Write the failing tests**

```js
// test/queue.test.js
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";

const T0 = 1_785_900_000_000;
function item(id, over = {}) {
  return {
    id, kind: "conv", peerId: "conv:1", conversationId: 1, messageId: id.split(":")[1],
    objIndex: 0, senderId: 42, senderName: "Hugh", content: "hi", postedTs: 1785900000,
    firstSightBacklog: false, state: "queued", attempts: 0, nextAttemptAt: 0,
    enqueuedAt: T0, ...over,
  };
}
function freshStore() {
  const dir = mkdtempSync(join(tmpdir(), "twistq-"));
  return { path: join(dir, "queue.json"), dir };
}

test("enqueueAll persists new items once and dedups by id", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  assert.equal(await q.enqueueAll([item("conv-msg:1"), item("conv-msg:2")], T0), 2);
  assert.equal(await q.enqueueAll([item("conv-msg:2"), item("conv-msg:3")], T0), 1);
  assert.equal(q.itemsInState("queued").length, 3);
  const reloaded = createQueueStore(path);
  await reloaded.load();
  assert.equal(reloaded.itemsInState("queued").length, 3); // survived restart
});

test("transition merges patch, stamps finishedAt on terminal, persists", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1")], T0);
  const claimed = await q.transition("conv-msg:1", { state: "processing", claimedAt: T0 + 5, attempts: 1 });
  assert.equal(claimed.state, "processing");
  const done = await q.transition("conv-msg:1", { state: "done" }, T0 + 9);
  assert.equal(done.finishedAt, T0 + 9);
  const reloaded = createQueueStore(path);
  await reloaded.load();
  assert.equal(reloaded.get("conv-msg:1").state, "done");
});

test("prune tombstones old terminal items; tombstoned ids still dedup", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1")], T0);
  await q.transition("conv-msg:1", { state: "done" }, T0);
  const THIRTYONE_DAYS = 31 * 24 * 3600 * 1000;
  await q.prune(T0 + THIRTYONE_DAYS);
  assert.equal(q.get("conv-msg:1"), undefined);
  assert.equal(q.has("conv-msg:1"), true); // tombstone
  assert.equal(await q.enqueueAll([item("conv-msg:1")], T0 + THIRTYONE_DAYS), 0);
});

test("crash simulation: a reload at any point sees a consistent store", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1"), item("conv-msg:2")], T0);
  await q.transition("conv-msg:1", { state: "processing", claimedAt: T0, attempts: 1 });
  // "crash": reopen from disk without any shutdown hook
  const q2 = createQueueStore(path);
  await q2.load();
  assert.equal(q2.get("conv-msg:1").state, "processing");
  assert.equal(q2.get("conv-msg:2").state, "queued");
  assert.equal(q2.nonTerminalCount(), 2);
  // file is valid JSON at rest (atomic rename, never partial)
  JSON.parse(readFileSync(path, "utf8"));
});

test("nonTerminalCount counts queued+processing only", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([item("conv-msg:1"), item("conv-msg:2"), item("conv-msg:3")], T0);
  await q.transition("conv-msg:1", { state: "skipped", reason: "no-mention" }, T0);
  await q.transition("conv-msg:2", { state: "processing", claimedAt: T0, attempts: 1 });
  assert.equal(q.nonTerminalCount(), 2);
});
```

- [ ] **Step 2: Run to verify failure** — `node --test test/queue.test.js` → FAIL (`createQueueStore` not exported).

- [ ] **Step 3: Implement `src/queue.js`**

```js
// Persistent job-queue store for Twist ingestion. Single-process, single JSON
// file. Correctness contract: an item id, once seen (live or tombstoned), is
// never re-enqueued; every transition persists before callers proceed.
import { readFile, writeFile, rename, mkdir, open } from "node:fs/promises";
import { dirname } from "node:path";

const TERMINAL = new Set(["done", "skipped", "failed"]);
const PRUNE_AFTER_MS = 30 * 24 * 3600 * 1000;

export function createQueueStore(filePath) {
  let data = { items: {}, tombstones: [] };
  let tombstoneSet = new Set();
  let loaded = false;
  let writeChain = Promise.resolve();

  async function load() {
    try {
      const parsed = JSON.parse(await readFile(filePath, "utf8"));
      data = { items: parsed.items ?? {}, tombstones: parsed.tombstones ?? [] };
    } catch (err) {
      if (err.code !== "ENOENT") throw err;
    }
    tombstoneSet = new Set(data.tombstones);
    loaded = true;
  }
  function ensure() {
    if (!loaded) throw new Error("twist queue store used before load()");
  }
  function persist() {
    const snapshot = JSON.stringify(data, null, 1);
    writeChain = writeChain.then(async () => {
      await mkdir(dirname(filePath), { recursive: true });
      const tmp = `${filePath}.tmp`;
      const fh = await open(tmp, "w");
      try {
        await fh.writeFile(snapshot);
        await fh.sync();
      } finally {
        await fh.close();
      }
      await rename(tmp, filePath);
    });
    return writeChain;
  }

  return {
    load,
    has: (id) => (ensure(), id in data.items || tombstoneSet.has(id)),
    get: (id) => (ensure(), data.items[id]),
    async enqueueAll(items, now) {
      ensure();
      let added = 0;
      for (const it of items) {
        if (it.id in data.items || tombstoneSet.has(it.id)) continue;
        data.items[it.id] = { ...it, state: "queued", attempts: it.attempts ?? 0, nextAttemptAt: it.nextAttemptAt ?? 0, enqueuedAt: it.enqueuedAt ?? now };
        added++;
      }
      if (added) await persist();
      return added;
    },
    async transition(id, patch, now) {
      ensure();
      const cur = data.items[id];
      if (!cur) throw new Error(`transition on unknown item ${id}`);
      const next = { ...cur, ...patch };
      if (TERMINAL.has(next.state) && next.finishedAt === undefined) next.finishedAt = now ?? patch.claimedAt ?? cur.claimedAt ?? cur.enqueuedAt;
      data.items[id] = next;
      await persist();
      return next;
    },
    itemsInState: (state) => (ensure(), Object.values(data.items).filter((i) => i.state === state)),
    nonTerminalCount: () => (ensure(), Object.values(data.items).filter((i) => !TERMINAL.has(i.state)).length),
    async prune(now) {
      ensure();
      let changed = false;
      for (const [id, it] of Object.entries(data.items)) {
        if (TERMINAL.has(it.state) && now - (it.finishedAt ?? it.enqueuedAt) > PRUNE_AFTER_MS) {
          delete data.items[id];
          tombstoneSet.add(id);
          changed = true;
        }
      }
      if (changed) {
        data.tombstones = [...tombstoneSet];
        await persist();
      }
    },
  };
}
```

- [ ] **Step 4: Run** — `node --test test/queue.test.js` → PASS (5/5); `node --test` → 24 pass.
- [ ] **Step 5: Commit** — `git add src/queue.js test/queue.test.js && git commit -m "feat: queue store — persistent idempotent state machine for Twist ingestion"`

---

### Task 2: Queue store — claim selection with per-peer + global concurrency

**Files:**
- Modify: `src/queue.js` (add `selectClaimable`)
- Test: `test/queue.test.js` (append)

**Interfaces:**
- Produces: `selectClaimable(now, busyPeerIds:Set<string>, slotsFree:number) -> item | null` — among `state === "queued"` with `nextAttemptAt <= now` and `peerId` not in `busyPeerIds`, returns the oldest by `(postedTs, objIndex)`; `null` when none or `slotsFree <= 0`. Pure read (caller transitions).

- [ ] **Step 1: Failing tests (append to `test/queue.test.js`)**

```js
test("selectClaimable: oldest first, skips busy peers, honors backoff and slots", async () => {
  const { path } = freshStore();
  const q = createQueueStore(path);
  await q.load();
  await q.enqueueAll([
    item("conv-msg:1", { peerId: "conv:1", postedTs: 100 }),
    item("conv-msg:2", { peerId: "conv:1", postedTs: 50 }),
    item("conv-msg:3", { peerId: "conv:2", postedTs: 200, nextAttemptAt: T0 + 999_999 }),
    item("conv-msg:4", { peerId: "conv:3", postedTs: 300 }),
  ], T0);
  assert.equal(q.selectClaimable(T0, new Set(), 3).id, "conv-msg:2");       // oldest overall
  assert.equal(q.selectClaimable(T0, new Set(["conv:1"]), 3).id, "conv-msg:4"); // conv:1 busy, 3 backing off
  assert.equal(q.selectClaimable(T0 + 1_000_000, new Set(["conv:1", "conv:3"]), 3).id, "conv-msg:3"); // backoff elapsed
  assert.equal(q.selectClaimable(T0, new Set(), 0), null);                   // no slots
});
```

- [ ] **Step 2: Run** — FAIL (`selectClaimable` not a function).
- [ ] **Step 3: Implement (add to the returned object in `createQueueStore`)**

```js
    selectClaimable(now, busyPeerIds, slotsFree) {
      ensure();
      if (slotsFree <= 0) return null;
      let best = null;
      for (const it of Object.values(data.items)) {
        if (it.state !== "queued" || it.nextAttemptAt > now || busyPeerIds.has(it.peerId)) continue;
        if (!best || it.postedTs < best.postedTs || (it.postedTs === best.postedTs && it.objIndex < best.objIndex)) best = it;
      }
      return best;
    },
```

- [ ] **Step 4: Run** — `node --test test/queue.test.js` → PASS; full suite green.
- [ ] **Step 5: Commit** — `git commit -am "feat: claim selection — oldest-first with per-peer exclusion and backoff"`

---

### Task 3: Producer — fetch-and-enqueue sweep

**Files:**
- Create: `src/producer.js`
- Modify: `src/twist-client.js` (add `getThread(threadId, signal)` → `GET threads/getone?id=` — only if not already present; check first)
- Test: `test/producer.test.js`

**Interfaces:**
- Consumes: queue store from Task 1 (`has`, `enqueueAll`), cursor store from `src/state.js` (`isFirstSight`, `getCursor`, `setCursor`), routing helpers (`newInboundItems`, `advanceCursor`), a `client` with `getUnreadConversations`, `getUnreadThreads`, `getConversationMessages`, `getThreadComments`, `getThread`, `markThreadRead`.
- Produces: `createProducer({ client, queue, cursors, botUserId, freshSinceTs, now, log }) -> { pollOnce() }`. `pollOnce` enqueues every new non-self item (mapped to the canonical item shape), sets `firstSightBacklog: true` on items with `posted_ts * 1000 < freshSinceTs * 1000` when their container is first-sight, synthesizes `thread-post:<id>` on thread first sight, advances cursors AFTER `enqueueAll` resolves, and calls `markThreadRead` best-effort.

- [ ] **Step 1: Failing tests**

```js
// test/producer.test.js
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createCursorStore } from "../src/state.js";
import { createProducer } from "../src/producer.js";

const BOT = 634870;
const NOW_MS = 1_785_900_000_000;
const FRESH_TS = 1_785_890_000; // seconds; poller "boot minus grace"

function fakeClient(state) {
  return {
    getUnreadConversations: async () => state.convs ?? [],
    getUnreadThreads: async () => state.threads ?? [],
    getConversationMessages: async (id) => state.convMsgs?.[id] ?? [],
    getThreadComments: async (id) => state.threadComments?.[id] ?? [],
    getThread: async (id) => state.threadObjs?.[id],
    markThreadRead: async (id, idx) => { (state.marked ??= []).push([id, idx]); },
  };
}
async function build(state) {
  const dir = mkdtempSync(join(tmpdir(), "twistp-"));
  const queue = createQueueStore(join(dir, "queue.json"));
  const cursors = createCursorStore(join(dir, "cursors.json"));
  await queue.load(); await cursors.load();
  const producer = createProducer({ client: fakeClient(state), queue, cursors, botUserId: BOT, freshSinceTs: FRESH_TS, now: () => NOW_MS, log: () => {} });
  return { queue, cursors, producer };
}

test("enqueues fresh conversation messages incl. obj_index 0; excludes self; advances cursor", async () => {
  const { queue, cursors, producer } = await build({
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [
      { id: 501, obj_index: 0, creator: 427360, posted_ts: FRESH_TS + 100, content: "hey bot" },
      { id: 502, obj_index: 1, creator: BOT, posted_ts: FRESH_TS + 101, content: "self" },
    ] },
  });
  await producer.pollOnce();
  assert.equal(queue.has("conv-msg:501"), true);
  assert.equal(queue.has("conv-msg:502"), false);
  assert.equal(queue.get("conv-msg:501").firstSightBacklog, false);
  assert.equal(cursors.getCursor("conversations", 9), 1); // advanced past self post too
});

test("first-sight backlog items are enqueued flagged, not dropped", async () => {
  const { queue, producer } = await build({
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [{ id: 501, obj_index: 0, creator: 427360, posted_ts: FRESH_TS - 5000, content: "old" }] },
  });
  await producer.pollOnce();
  assert.equal(queue.get("conv-msg:501").firstSightBacklog, true);
});

test("thread sweep enqueues comments AND synthesizes the opening post on first sight", async () => {
  const { queue, producer } = await build({
    threads: [{ thread_id: 7, channel_id: 3, direct_mention: true }],
    threadObjs: { 7: { id: 7, title: "Budget?", content: "[Bot](twist-mention://634870) thoughts?", creator: 427360, posted_ts: FRESH_TS + 50 } },
    threadComments: { 7: [{ id: 88, obj_index: 1, creator: 427360, posted_ts: FRESH_TS + 60, content: "ping" }] },
  });
  await producer.pollOnce();
  assert.equal(queue.has("thread-post:7"), true);
  assert.equal(queue.has("thread-comment:88"), true);
  assert.equal(queue.get("thread-post:7").peerId, "thread:7");
});

test("second poll is a no-op (idempotent), cursor bounds refetch", async () => {
  const state = {
    convs: [{ conversation_id: 9 }],
    convMsgs: { 9: [{ id: 501, obj_index: 0, creator: 427360, posted_ts: FRESH_TS + 100, content: "hey" }] },
  };
  const { queue, producer } = await build(state);
  await producer.pollOnce();
  await producer.pollOnce();
  assert.equal(queue.itemsInState("queued").length, 1);
});
```

- [ ] **Step 2: Run** — FAIL (`createProducer` missing).
- [ ] **Step 3: Implement `src/producer.js`**

```js
// Transport-only sweep: everything new gets enqueued by id; no policy here.
// Cursor = refetch bound only. Invariant: cursor advances only after the
// enqueue batch is durably persisted, so nothing below a cursor is un-enqueued.
import { newInboundItems, advanceCursor } from "./routing.js";

const ITEM_FETCH_LIMIT = 30;

export function createProducer({ client, queue, cursors, botUserId, freshSinceTs, now, log }) {
  const isBacklog = (firstSight, postedTs) => firstSight && !(typeof postedTs === "number" && postedTs >= freshSinceTs);

  function toItem({ raw, kind, peerId, conversationId, threadId, channelId, firstSight }) {
    return {
      id: kind === "conv" ? `conv-msg:${raw.id}` : kind === "thread" ? `thread-comment:${raw.id}` : `thread-post:${threadId}`,
      kind, peerId, conversationId, threadId, channelId,
      messageId: raw.id, objIndex: raw.obj_index ?? 0,
      senderId: raw.creator, senderName: raw.creator_name ?? String(raw.creator),
      content: raw.content ?? "", postedTs: raw.posted_ts ?? 0,
      firstSightBacklog: isBacklog(firstSight, raw.posted_ts),
      state: "queued", attempts: 0, nextAttemptAt: 0, enqueuedAt: now(),
    };
  }

  async function sweepConversation(c) {
    const convId = c.conversation_id;
    const firstSight = cursors.isFirstSight("conversations", convId);
    const cursor = firstSight ? -1 : cursors.getCursor("conversations", convId);
    const messages = await client.getConversationMessages(convId, { limit: ITEM_FETCH_LIMIT });
    const fresh = newInboundItems(messages, cursor, botUserId)
      .filter((m) => !queue.has(`conv-msg:${m.id}`))
      .map((raw) => toItem({ raw, kind: "conv", peerId: `conv:${convId}`, conversationId: convId, firstSight }));
    await queue.enqueueAll(fresh, now());
    await cursors.setCursor("conversations", convId, advanceCursor(cursor, messages));
  }

  async function sweepThread(t) {
    const threadId = t.thread_id;
    const firstSight = cursors.isFirstSight("threads", threadId);
    const cursor = firstSight ? -1 : cursors.getCursor("threads", threadId);
    const comments = await client.getThreadComments(threadId, { limit: ITEM_FETCH_LIMIT });
    const items = newInboundItems(comments, cursor, botUserId)
      .filter((cm) => !queue.has(`thread-comment:${cm.id}`))
      .map((raw) => toItem({ raw, kind: "thread", peerId: `thread:${threadId}`, threadId, channelId: t.channel_id, firstSight }));
    if (firstSight && !queue.has(`thread-post:${threadId}`)) {
      try {
        const post = await client.getThread(threadId);
        if (post && String(post.creator) !== String(botUserId)) {
          items.push(toItem({ raw: { ...post, id: post.id, obj_index: 0 }, kind: "thread-post", peerId: `thread:${threadId}`, threadId, channelId: t.channel_id, firstSight }));
        }
      } catch (err) {
        log(`thread post fetch failed ${threadId}: ${String(err)}`);
      }
    }
    await queue.enqueueAll(items, now());
    const nextCursor = advanceCursor(cursor, comments);
    await cursors.setCursor("threads", threadId, nextCursor);
    if (Number.isFinite(nextCursor)) {
      try { await client.markThreadRead(threadId, nextCursor); } catch (err) { log(`markThreadRead ${threadId} failed: ${String(err)}`); }
    }
  }

  return {
    async pollOnce() {
      const convs = await client.getUnreadConversations();
      for (const c of convs) {
        try { await sweepConversation(c); } catch (err) { log(`conv sweep ${c.conversation_id} failed: ${String(err)}`); }
      }
      const threads = (await client.getUnreadThreads()).filter((t) => t.direct_mention);
      for (const t of threads) {
        try { await sweepThread(t); } catch (err) { log(`thread sweep ${t.thread_id} failed: ${String(err)}`); }
      }
    },
  };
}
```

Note on `thread-post` id: the item id uses the THREAD id (stable, one post per thread); `messageId` carries the post's own id for reactions. `obj_index: 0` keeps ordering before all comments.

- [ ] **Step 4: `getThread` check** — open `src/twist-client.js`; if it lacks `getThread`, add alongside the existing getters:

```js
  getThread: (threadId, signal) => get("threads/getone", { id: threadId }, signal),
```

(match the file's existing `get(path, params, signal)` helper shape exactly — read it first.)

- [ ] **Step 5: Run** — `node --test test/producer.test.js` → PASS; full suite green.
- [ ] **Step 6: Commit** — `git add src/producer.js src/twist-client.js test/producer.test.js && git commit -m "feat: producer — transport-only sweep with idempotent enqueue and thread-post synthesis"`

---

### Task 4: Admission extraction in `src/inbound.js`

**Files:**
- Modify: `src/inbound.js`

**Interfaces:**
- Produces: `export async function admissionVerdict({ message, account, cfg })` → `{ admit: boolean, admission: string, commandAuthorized: boolean }` — exactly the `createChannelIngressResolver(...).message(...)` block currently inlined in `handleTwistInbound` (lines ~61–100), moved verbatim into the new function (including the `dmPolicy`/`groupPolicy`/`wasMentioned`/`requireMention` derivation).
- `handleTwistInbound({ ..., verdict })` gains an optional pre-computed `verdict`; when provided it skips its internal gate (`if (!verdict) verdict = await admissionVerdict(...)`; then `if (!verdict.admit) { log; return; }`). Behavior with no `verdict` argument is byte-for-byte today's.

No local unit tests possible (SDK imports — see Global Constraints); the consumer tests (Task 5) cover the verdict flow via injection, and the full-suite run proves no import breakage.

- [ ] **Step 1: Refactor as described.** The moved block returns `{ admit: access.ingress.admission === "dispatch", admission: access.ingress.admission, commandAuthorized: access.commandAccess?.authorized ?? false }`.
- [ ] **Step 2: Run** — `node --test` → all green (routing/queue/producer untouched by this file, this verifies parseability via any transitive import only; ALSO run `node -e "import('./src/inbound.js').catch(e => { console.error(e.message); process.exit(1) })"` and expect the SDK import error text UNCHANGED from before the edit — i.e., the file still parses, failing only on the absent SDK, same as pre-change).
- [ ] **Step 3: Commit** — `git commit -am "refactor: extract admissionVerdict so the consumer can record ingress denials"`

---

### Task 5: Consumer — claim loop, policy verdicts, retry ladder, dead-letter

**Files:**
- Create: `src/consumer.js`
- Test: `test/consumer.test.js`

**Interfaces:**
- Consumes: queue store (`selectClaimable`, `transition`, `itemsInState`, `nonTerminalCount`), `contentMentionsBot` from `./routing.js`.
- Produces: `createConsumer(deps) -> { tick(), recoverOrphans(), inFlightCount() }` with `deps = { queue, botUserId, now, log, classifyPeer(item) -> Promise<"dm"|"groupdm"|"thread">, admission(item) -> Promise<{admit, admission, commandAuthorized}>, runTurn(item, {commandAuthorized}) -> Promise<void>, probe(item) -> Promise<boolean>, react(item, verb, emoji) -> Promise<void>, alert(text) -> Promise<void>, replyInPlace(item, text) -> Promise<void> }`.
- Constants exported for tests: `BACKOFF_MS = [30_000, 120_000, 600_000, 3_600_000, 3_600_000]`, `MAX_ATTEMPTS = 6`, `MAX_GLOBAL_TURNS = 3`, `HIGH_WATER = 50`, `HUNG_TURN_ALERT_MS = 30 * 60_000`.
- Error classification: `export function isPermanentError(err)` — true when `err.status` ∈ {400, 404, 410} or `/not found|does not exist|deleted/i.test(err.message)`.

- [ ] **Step 1: Failing tests**

```js
// test/consumer.test.js
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createQueueStore } from "../src/queue.js";
import { createConsumer, BACKOFF_MS, MAX_ATTEMPTS, isPermanentError } from "../src/consumer.js";

const BOT = 634870;
const T0 = 1_785_900_000_000;
const MENTION = `[Bot](twist-mention://${BOT}) hello`;

function baseItem(id, over = {}) {
  return {
    id, kind: "conv", peerId: "conv:1", conversationId: 1, messageId: 500,
    objIndex: 0, senderId: 427360, senderName: "Hugh", content: MENTION, postedTs: 1785900000,
    firstSightBacklog: false, state: "queued", attempts: 0, nextAttemptAt: 0, enqueuedAt: T0, ...over,
  };
}
async function harness({ items = [], clock = { t: T0 }, runTurn, probe, classifyPeer, admission } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistc-"));
  const queue = createQueueStore(join(dir, "queue.json"));
  await queue.load();
  await queue.enqueueAll(items, clock.t);
  const calls = { turns: [], reacts: [], alerts: [], replies: [] };
  const consumer = createConsumer({
    queue, botUserId: BOT, now: () => clock.t, log: () => {},
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
async function drain(consumer) { await consumer.tick(); await consumer.tick(); } // claim pass + settle pass

test("happy path: mention dispatched, reactions cycled, item done", async () => {
  const { queue, consumer, calls } = await harness({ items: [baseItem("conv-msg:500")] });
  await drain(consumer);
  assert.deepEqual(calls.turns, ["conv-msg:500"]);
  assert.equal(queue.get("conv-msg:500").state, "done");
  assert.deepEqual(calls.reacts.map((r) => `${r[1]}:${r[2]}`), ["add:⏳", "remove:⏳", "add:✅"]);
});

test("policy skips are terminal and reasoned: no-mention / backlog / admission", async () => {
  const { queue, consumer } = await harness({
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

test("permanent errors classify to skipped:gone without retries", async () => {
  const err = Object.assign(new Error("Message not found"), { status: 404 });
  const { queue, consumer } = await harness({ runTurn: async () => { throw err; }, items: [baseItem("conv-msg:500")] });
  await drain(consumer);
  assert.equal(queue.get("conv-msg:500").state, "skipped");
  assert.equal(queue.get("conv-msg:500").reason, "gone");
  assert.equal(isPermanentError(err), true);
});

test("per-peer serialization: second item in same peer waits, then runs", async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const { queue, consumer, calls } = await harness({
    runTurn: async (item) => { calls.turns.push(item.id); if (item.id === "conv-msg:1") await gate; },
    items: [baseItem("conv-msg:1", { postedTs: 100 }), baseItem("conv-msg:2", { postedTs: 200 })],
  });
  await consumer.tick();
  assert.deepEqual(calls.turns, ["conv-msg:1", "conv-msg:1"].slice(0, 1)); // only first claimed
  await consumer.tick();
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
```

- [ ] **Step 2: Run** — FAIL (`createConsumer` missing).
- [ ] **Step 3: Implement `src/consumer.js`**

```js
// State-machine orchestration. All effects are injected; nothing here touches
// the network or the SDK directly, so the whole delivery guarantee is testable.
import { contentMentionsBot } from "./routing.js";

export const BACKOFF_MS = [30_000, 120_000, 600_000, 3_600_000, 3_600_000];
export const MAX_ATTEMPTS = 6;
export const MAX_GLOBAL_TURNS = 3;
export const HIGH_WATER = 50;
export const HUNG_TURN_ALERT_MS = 30 * 60_000;

export function isPermanentError(err) {
  const status = err?.status ?? err?.statusCode;
  if (status === 400 || status === 404 || status === 410) return true;
  return /not found|does not exist|deleted/i.test(String(err?.message ?? ""));
}

export function createConsumer({ queue, botUserId, now, log, classifyPeer, admission, runTurn, probe, react, alert, replyInPlace }) {
  const inFlight = new Map(); // peerId -> { id, startedAt, hungAlerted }
  let highWaterAlerted = false;

  async function policyVerdict(item) {
    if (item.firstSightBacklog) return { skip: "backlog" };
    const kind = item.kind === "conv" ? await classifyPeer(item) : "thread";
    if (kind !== "dm" && !contentMentionsBot(item.content, botUserId)) return { skip: "no-mention" };
    const adm = await admission(item);
    if (!adm.admit) return { skip: `admission:${adm.admission}` };
    return { commandAuthorized: adm.commandAuthorized };
  }

  async function settle(item, run) {
    try {
      const verdict = await policyVerdict(item);
      if (verdict.skip) {
        await queue.transition(item.id, { state: "skipped", reason: verdict.skip }, now());
        return;
      }
      await react(item, "add", "⏳");
      await runTurn(item, { commandAuthorized: verdict.commandAuthorized });
      await react(item, "remove", "⏳");
      await react(item, "add", "✅");
      await queue.transition(item.id, { state: "done" }, now());
    } catch (err) {
      await react(item, "remove", "⏳").catch?.(() => {});
      if (isPermanentError(err)) {
        await queue.transition(item.id, { state: "skipped", reason: "gone", lastError: String(err) }, now());
        return;
      }
      const attempts = queue.get(item.id)?.attempts ?? item.attempts;
      if (attempts >= MAX_ATTEMPTS) {
        await react(item, "add", "❌");
        await queue.transition(item.id, { state: "failed", lastError: String(err) }, now());
        await replyInPlace(item, "I hit an error answering this and have exhausted retries — it's been flagged for review.").catch(() => {});
        await alert(`twist queue: item ${item.id} (${item.peerId}) failed after ${attempts} attempts: ${String(err).slice(0, 300)}`).catch(() => {});
        return;
      }
      const delay = BACKOFF_MS[Math.min(attempts - 1, BACKOFF_MS.length - 1)];
      log(`turn failed for ${item.id} (attempt ${attempts}), retrying in ${delay}ms: ${String(err)}`);
      await queue.transition(item.id, { state: "queued", nextAttemptAt: now() + delay, lastError: String(err) }, now());
    } finally {
      inFlight.delete(item.peerId);
    }
  }

  return {
    inFlightCount: () => inFlight.size,
    async recoverOrphans() {
      for (const item of queue.itemsInState("processing")) {
        const answered = await probe(item).catch((err) => { log(`probe failed for ${item.id}: ${String(err)}`); return false; });
        await queue.transition(item.id, answered ? { state: "done" } : { state: "queued", nextAttemptAt: 0 }, now());
        log(`boot recovery: ${item.id} -> ${answered ? "done (reply found)" : "requeued"}`);
      }
    },
    async tick() {
      if (!highWaterAlerted && queue.nonTerminalCount() > HIGH_WATER) {
        highWaterAlerted = true;
        await alert(`twist queue: depth ${queue.nonTerminalCount()} exceeds high-water ${HIGH_WATER} — consumer may be wedged`).catch(() => {});
      }
      for (const [peerId, f] of inFlight) {
        if (!f.hungAlerted && now() - f.startedAt > HUNG_TURN_ALERT_MS) {
          f.hungAlerted = true;
          await alert(`twist queue: turn for ${f.id} (${peerId}) running > ${HUNG_TURN_ALERT_MS / 60000}min`).catch(() => {});
        }
      }
      while (true) {
        const item = queue.selectClaimable(now(), new Set(inFlight.keys()), MAX_GLOBAL_TURNS - inFlight.size);
        if (!item) break;
        const claimed = await queue.transition(item.id, { state: "processing", claimedAt: now(), attempts: item.attempts + 1 });
        inFlight.set(claimed.peerId, { id: claimed.id, startedAt: now(), hungAlerted: false });
        void settle(claimed); // fire-and-forget; settle() clears inFlight
      }
    },
  };
}
```

Implementation note for the test's `drain()`: `tick()` claims synchronously but `settle` runs async — tests call `tick()` twice with microtask turns between (the provided `drain` helper). If timing proves flaky, `settle` promises may be tracked in a `Set` and a test-only `await consumer.idle()` added — implement `idle()` (await all tracked settle promises) rather than sprinkling sleeps.

- [ ] **Step 4: Run** — `node --test test/consumer.test.js` → PASS (8/8); full suite green.
- [ ] **Step 5: Commit** — `git add src/consumer.js test/consumer.test.js && git commit -m "feat: consumer — serialized claims, recorded verdicts, retry ladder, loud dead-letters"`

---

### Task 6: Monitor rewiring

**Files:**
- Modify: `src/monitor.js` (major surgery — `processConversation`, `processThread`, `fireDispatch`, and the first-sight cursor logic are REPLACED by producer/consumer wiring; keep `participantKind`, `fetchThreadContext`, `buildTranscript` usage, `buildMessage`, reactions, statusSink, start/stop plumbing)

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces: the same external monitor contract as today: `monitorTwistProvider({ accountId, config, runtime, abortSignal, statusSink, cursors })` → `{ stop }` (signature unchanged; `cursors` param stays). New: a queue store is created at `twist-state/queue.json` (same directory as the cursors file — derive via `dirname(cursorsPath)`; the path the plugin passes to `createCursorStore` is available where the store is constructed — pass a `queuePath` alongside in the same call site, found in `src/channel.js` or `index.js`; read those files first and follow the existing wiring pattern).

- [ ] **Step 1: Rewire.** Inside `monitorTwistProvider`:

```js
  const queue = createQueueStore(queuePath); // constructed alongside cursor store; see call-site note above
  await queue.load();

  const producer = createProducer({ client, queue, cursors, botUserId, freshSinceTs, now: Date.now, log });

  // --- injected effects (each built from code that already exists in this file) ---
  const peerKindCache = new Map();
  const classifyPeer = (item) => participantKind(item.conversationId, peerKindCache); // existing helper

  const admission = async (item) => {
    const message = await toNormalizedMessage(item); // see step 2
    return admissionVerdict({ message, account, cfg: core.config.current() ?? cfg });
  };

  const runTurn = async (item, { commandAuthorized }) => {
    const message = await toNormalizedMessage(item, { withContext: true });
    await handleTwistInbound({ message, account, cfg: core.config.current() ?? cfg, runtime, client, statusSink,
      verdict: { admit: true, admission: "dispatch", commandAuthorized } });
  };

  const probe = async (item) => {
    const since = (item.claimedAt ?? 0) / 1000; // Twist ts are seconds
    const posts = item.kind === "conv"
      ? await client.getConversationMessages(item.conversationId, { limit: ITEM_FETCH_LIMIT })
      : await client.getThreadComments(item.threadId, { limit: ITEM_FETCH_LIMIT });
    return posts.some((p) => String(p.creator) === String(botUserId) && (p.posted_ts ?? 0) >= since);
  };

  const react = (item, verb, emoji) => safeReact(verb, { kind: item.kind === "conv" ? "conv" : "thread", messageId: item.messageId }, emoji); // adapt safeReact's target builder
  const alert = (text) => postToTwist({ client, ...resolveOutboundTarget(null, account.config.defaultTo), text }).catch((e) => log(String(e)));
  const replyInPlace = (item, text) => postToTwist({ client, kind: item.kind === "conv" ? "conv" : "thread", id: item.kind === "conv" ? item.conversationId : item.threadId, text });

  const consumer = createConsumer({ queue, botUserId, now: Date.now, log, classifyPeer, admission, runTurn, probe, react, alert, replyInPlace });
  await consumer.recoverOrphans(); // boot-only orphan probing

  async function pollOnce() {
    await producer.pollOnce();
    await consumer.tick();
    await queue.prune(Date.now());
  }
```

- [ ] **Step 2: `toNormalizedMessage(item, { withContext })`** — builds the same normalized message `handleTwistInbound` gets today (see its JSDoc: `{messageId,kind,conversationId,threadId,groupId,peerKind,peerId,isGroup,senderId,senderName,text,timestamp,directMention,...context}`), reusing `routingPeer`, `participantKind`, and (when `withContext`) the existing `fetchThreadContext`/`buildTranscript` for transcripts. `kind` for the message: `"thread"` for both `thread` and `thread-post` items, else `participantKind` result. `directMention: contentMentionsBot(item.content, botUserId)`. `timestamp: item.postedTs * 1000`.
- [ ] **Step 3: Delete** `processConversation`, `processThread`, `fireDispatch`, the `inFlight` set, and the `firstSightCursor` import (it stays exported in routing.js for the producer's backlog semantics — actually the producer computes backlog directly; REMOVE `firstSightCursor` from routing.js exports ONLY IF nothing imports it — `grep -rn firstSightCursor src/ test/` first; if only old tests reference it, keep the function and its tests: they document cursor semantics still used at migration).
- [ ] **Step 4: Run** — full `node --test` green (routing 19 + queue + producer + consumer). Syntax-check monitor: `node -e "import('./src/monitor.js').catch(e => console.error(e.message))"` — must fail ONLY on the SDK import (same as pre-change), not on syntax.
- [ ] **Step 5: Commit** — `git commit -am "refactor: monitor becomes producer/consumer wiring — at-most-once ingestion removed"`

---

### Task 7: Docs, version, ship gate

**Files:**
- Modify: `README.md` (replace the cursor-semantics paragraph with the queue lifecycle: states, retry ladder, skip reasons table, boot recovery, `twist-state/queue.json`)
- Modify: `package.json` (`version` → `0.4.0`)

- [ ] **Step 1:** README + version bump; document the migration note (cursors.json untouched; old code ignores queue.json → rollback = repin submodule).
- [ ] **Step 2:** Full suite: `node --test` → all green.
- [ ] **Step 3: Commit** — `git commit -am "docs: queue lifecycle + 0.4.0"`
- [ ] **Step 4: Ship gate (in order):** push branch; open PR on `sanctuarycomputer/openclaw-twist`; run the adversarial review loop (≥2 fresh-context reviewers with distinct lenses: crash-recovery correctness, double-dispatch, migration/first-boot, plus a live-shape probe of any Twist API field assumptions e.g. `threads/getone` response and `creator_name` presence); fix findings; merge; bump `plugins/twist` submodule in stacksbot via PR; merge; verify the Render deploy actually serves the new code (grep the box for `selectClaimable` in `/app/plugins/twist/src/queue.js`); then live smoke test: send a fresh group-DM mention + a thread mention created by mentioning the bot (opening post), confirm both answered and `queue.json` shows `done` items.

## Self-Review (completed)

- **Spec coverage:** producer/idempotent-enqueue → T3; queue FSM/persistence/prune/high-water → T1/T2/T5; per-peer + global concurrency → T2/T5; skip taxonomy incl. `admission` (T4 extraction) and `gone` → T5; retry ladder + dead-letter loudness → T5; boot-only orphan probing with claimedAt → T5 (probe impl in T6); opening-post synthesis → T3; markThreadRead after enqueue → T3; migration/rollback → T6/T7; reactions kept → T5/T6.
- **Placeholder scan:** none found; every code step is concrete.
- **Type consistency:** item shape defined once (header) and used verbatim in T1/T3/T5 tests; `selectClaimable(now, busyPeerIds, slotsFree)` consistent T2→T5; `admissionVerdict` return shape consistent T4→T5→T6; `posted_ts` seconds vs ms conversions called out at each comparison site (probe, backlog).
- **Known judgment call for implementers:** the exact `queuePath` plumbing (T6) depends on where `createCursorStore` is constructed (`channel.js`/`index.js`) — the task says to read and mirror that call site rather than guessing an import path from the plan.
