# Durable Ingestion Queue for openclaw-twist

**Date:** 2026-08-05 · **Status:** Approved (design session with Hugh) · **Supersedes:** the cursor-only ingestion in `monitor.js`/`state.js`

## Problem

Six distinct bugs (openclaw-twist#1–#4 plus two unfixed holes: the in-flight
skip drop and unanswered opening-post mentions) share one root cause: the
poll loop advances its cursor past a message before — or regardless of —
whether that message was handled. At-most-once delivery means every
classification mistake, crash, or skip becomes permanent silent message
loss, detectable only by a human noticing they were ignored.

**Requirement (Hugh):** treat ingestion as a hardened idempotent job queue.
Every fetched message reaches a terminal outcome, even across failures. The
only sanctioned non-answer is "too old at first encounter," which in
continuous production should essentially never occur. No separate audit
layer — the queue's own state machine is the guarantee.

## Architecture

Three units replace today's single fetch-decide-dispatch-advance pass:

```
Twist API ──▶ Producer (poll) ──▶ Queue (persistent FSM) ──▶ Consumer ──▶ agent turn ──▶ reply
                   │                                            │
              cursor = fetch                              policy verdicts
              optimization only                           recorded, never erased
```

### Producer (transport only)

Every `pollIntervalMs`: fetch unread conversations (all) and unread threads
(`direct_mention` only, unchanged), fetch their recent items, and **enqueue
everything new by Twist's globally-unique message/comment id**. The only
exclusion at this layer is self-authored posts. No mention filtering, no
freshness heuristics, no policy.

- **Idempotent enqueue** is the correctness mechanism: an id already present
  in the queue (any state, including pruned-tombstone window) is a no-op.
  Cursors survive purely to bound refetching; a cursor bug now causes
  refetch + dedup, not loss.
- Cursor advances only after the enqueue batch is durably persisted.
- **Opening-post mentions:** on first sight of a thread, the thread body
  itself is enqueued as a synthetic item (id `thread-post:<threadId>`), so
  threads created by @mentioning the bot are answered.
- The server-side read-marker advance (`markThreadRead`, the unread-scan
  scale bound) moves to after durable enqueue — safe, because the queue now
  owns the items.

### Queue (persistent state machine)

States: `queued → processing → done | skipped | failed`.

Item record: `{ id, kind (thread|conv|thread-post), peerId, threadId?,
conversationId?, channelId?, objIndex, senderId, senderName, content,
postedTs, enqueuedAt, state, attempts, nextAttemptAt?, claimedAt?,
lastError?, reason?, finishedAt? }`

- Persistence: single JSON file in `twist-state/` (same zero-dependency
  pattern as the cursor store), serialized writes, tmp-file + fsync +
  atomic rename. Every state transition persists before its side effects
  proceed (claim persists `processing` before the turn starts).
- Terminal items are pruned after 30 days; pruned ids remain in a compact
  tombstone set until their container's cursor is safely past them.
- A high-water alert (one-shot per breach, to the ops thread) fires if
  non-terminal depth exceeds 50 — a wedged consumer must not be silent.

### Consumer (policy + dispatch)

Claims `queued` items with `nextAttemptAt <= now`, oldest-first, under two
concurrency rules: **at most one turn per peer** (preserves session
coherence and makes probing unambiguous; later mentions in a busy thread
wait instead of being dropped — this deletes the in-flight-skip bug) and
**at most 3 turns globally** (box/spend protection).

Policy evaluates at claim time and records verdicts as terminal `skipped`
states, never by erasing transport data:

- `skipped: no-mention` — group/thread item without a bot mention
  (routing rules unchanged: threads and group DMs are mention-only, 1:1
  DMs are open).
- `skipped: admission` — OpenCLAW ingress gate returned non-dispatch
  (today this drop is invisible inside `handleTwistInbound`; the gate
  moves ahead of dispatch so its verdict is recorded).
- `skipped: backlog` — item was already older than the first-sight grace
  window (2h) when its container was first seen. The only sanctioned
  non-answer; in steady-state production it never fires.
- `skipped: gone` — trigger no longer exists on Twist (deleted message /
  permanent 4xx). Non-retryable by classification.

Dispatch reuses `handleTwistInbound` (minus its internal admission gate)
and the existing ⏳/✅/❌ reaction lifecycle.

## Delivery guarantee

- **Failure → retry with backoff.** attempts++, `nextAttemptAt` per ladder
  30s → 2m → 10m → 1h → 1h, max 6 attempts. Only retryable errors ride the
  ladder (transient API/model/network); permanent errors classify to
  `skipped: gone`.
- **Retries exhausted → `failed`, loudly.** In-place reply ("hit an error
  answering this — flagged for review") + ops-thread alert with the item
  reference + ❌ reaction. Nobody is left wondering if they were ignored.
- **Crash recovery: boot-only orphan probing.** The consumer is a single
  process, so a `processing` item found at boot is definitively orphaned
  (the turn died with the process). For each orphan, probe Twist: did the
  bot post in that peer after the item's `claimedAt`? Per-peer
  serialization guarantees such a post can only be this item's reply →
  `done`; otherwise → `queued` (attempt preserved). Crashes therefore
  produce neither drops nor duplicates.
- **No lease-expiry probing while running.** A live turn exceeding 30
  minutes triggers a watchdog *alert*, never a concurrent re-dispatch —
  premature lease expiry is the classic double-send bug in these systems.
- **Graceful stop:** stop claiming, let in-flight turns finish or fall to
  boot recovery.

## Migration

First boot with queue code: existing `cursors.json` is imported unchanged
as the fetch baseline (everything at-or-below a cursor is seen; queue
starts empty). Tracked threads/conversations behave identically from the
first poll. Rollback = repin the previous submodule sha; the queue file is
additive and ignored by old code.

## Testing

- Queue store: crash simulation between every persist point (write tmp,
  post-rename, mid-transition) → reload → assert no item lost, duplicated,
  or stuck.
- Producer: enqueue idempotency (same fetch twice), cursor-behind refetch,
  thread-post synthesis, self-post exclusion.
- Consumer: full state-machine walk (retry ladder timing, terminal states,
  per-peer + global concurrency, skip classifications).
- Boot recovery: orphan probe against a faked client (reply-after-claim →
  done; silence → requeued; deleted → skipped: gone).
- Existing 19 routing tests unchanged; `firstSightCursor` semantics move
  into the producer's backlog classification.
- Adversarial review gate before the live gateway runs it.

## Out of scope

External reconciliation audit (cut by Hugh — the queue is the guarantee),
non-mention thread follow-ups (routing policy unchanged), any change to
outbound delivery or session routing.
