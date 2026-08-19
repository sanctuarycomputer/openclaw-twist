# openclaw-twist

A [Twist](https://twist.com) (Doist) channel plugin for [OpenCLAW](https://openclaw.ai). It lets an OpenCLAW agent act as a Twist bot account — answering direct messages and replying when it's `@mentioned` in group DMs and channel threads — the same way OpenCLAW integrates with Slack, Telegram, and friends.

Built and used in production at [Sanctuary Computer](https://sanctuary.computer) for our "Stacksbot" assistant.

## What it does

| Context | Behavior | Session |
|---|---|---|
| **1:1 DM** | Replies to every message | Persistent per-DM |
| **Group DM** (multi-person) | Replies **only** when the bot is `@mentioned` | Per-conversation |
| **Channel thread** | Replies **only** when the bot is `@mentioned` | Separate per thread |

### Outbound thread notifications

When the bot posts a comment to a channel thread, it notifies the channel's **default participants** (the "Set default participants" setting in Twist's channel settings) instead of all thread subscribers. This is automatically read from the channel's `default_recipients` and `use_default_recipients` fields — no extra configuration required. If a channel has no default recipients configured, Twist's own default notification behavior applies. Direct messages and group DMs are unaffected.

While a turn runs, the triggering message gets an **⏳** reaction, which becomes **✅** on success, or **❌** only once the item has *finally* failed (retries exhausted) — a retryable error just clears the ⏳ and tries again later. This works for thread comments, DMs, and a thread's opening post alike. So you can see at a glance that the bot picked your message up and whether it's settled.

**The ack contract:** ⏳ means *acknowledged*, not *started*. It's added the moment the message is durably enqueued — within one poll interval of you sending it — for every `@mention` and every message in a 1:1 DM, regardless of how much unprocessed history sits in front of it. Nothing the bot won't answer is acknowledged: first-sight backlog and anything already past the 24h replay horizon get no ⏳, so coming back from an outage never showers a stale container in reactions. ✅ means *answered*. (Previously the ⏳ waited for the consumer to claim the item, so a mention behind a backlog of unanswerable comments could sit silent for minutes.) If the message turns out not to be answerable after all — admission denied, or past the replay horizon — the ⏳ is removed when the item is recorded `skipped`, so an acknowledged message never hangs there forever.

The agent receives full Twist context, not just the bare mention: the **thread title**, **channel name**, and a **transcript** of the surrounding discussion (Twist `[Name](twist-mention://id)` markup is cleaned to readable `@Name`).

## How it works

This channel **polls** Twist's unread endpoints on an interval (default 15s) from **inside the gateway process** — there's no separate service to run or supervise. It registers via the channel `gateway.startAccount` lifecycle (`runStoppablePassiveMonitor`), filters channel threads to mention-only before fetching (so it ignores the noise of every unread thread), and **baselines on first sight** so it never replies to pre-existing backlog. Fetching and dispatch are decoupled by a durable job queue, so every message reaches a terminal outcome — even across crashes — and one slow agent turn never stalls polling.

### Ingestion queue

A producer/queue/consumer pipeline replaces a single fetch-decide-dispatch pass:

- **Producer** (transport only): every poll, enqueues every new message/comment by Twist's globally-unique id. Enqueue is idempotent — an id already in the queue, live or tombstoned, is a no-op — so a cursor bug now causes a harmless refetch, not lost messages. No policy at this layer, only self-post exclusion. Fetches **page forward from the cursor** (ascending, up to 10 pages per container per poll) rather than grabbing the newest N — a burst bigger than one fetch window is drained across polls instead of being skipped. A thread's opening post is enqueued too (id `thread-post:<threadId>`), so a thread created by @mentioning the bot gets answered.
- **Queue**: a persistent state machine, one record per message. States: `queued → processing → done | skipped | failed`. Every transition is durably persisted (tmp-file + fsync + atomic rename) before its side effects proceed.
- **Consumer**: claims `queued` items oldest-first, at most **one turn per peer** at a time (keeps a thread/DM's turns ordered and its session coherent — a busy peer's next mention waits instead of being dropped) and at most **3 turns globally** (box/spend protection). Policy — mention check, admission gate — is evaluated at claim time and recorded as a terminal `skipped` state, never silently dropped. Each tick starts with a **skip-drain pre-pass**: items the policy can condemn without any lookup (backlog, stale, a thread comment with no mention) are settled terminally before any claiming, and deliberately outside the per-peer and global-turn gates — a skip runs no agent and touches no session, so neither gate has anything to protect. A cold thread's whole unanswerable history therefore drains in a single poll (instead of one item per poll), the mention behind it is claimed in that same pass, and the drain still works when every turn slot is busy or the peer already has a turn running.

Skip reasons (all logged, none silent):

| Reason | Meaning |
|---|---|
| `backlog` | Item was already older than the 2h first-sight grace window when its thread/conversation was first seen. The only sanctioned non-answer — in steady-state production it should never fire. |
| `stale` | Item was already older than the 24h **replay horizon** when the consumer claimed it. Paging forward from the cursor drains an entire outage gap, so this is the guard that stops the bot publicly answering day-old mentions on its way back up. |
| `no-mention` | Group DM / channel thread item without a bot `@mention` (1:1 DMs are open). |
| `admission:<verdict>` | OpenCLAW's ingress gate returned a non-dispatch verdict. |
| `gone` | The trigger no longer exists on Twist (deleted message, permanent 4xx). Non-retryable. |

**Retries:** a failed turn rides a backoff ladder — 30s, 2m, 10m, 1h, 1h — up to 6 attempts. The ⏳ stays on the message across the whole ladder (the item is still going to be answered); it's only cleared when the outcome is final. A turn that delivered **nothing** counts as a failure, not an answer, and goes back on the ladder: that covers a turn whose only output was OpenCLAW's `incomplete_turn` placeholder ("⚠️ Agent couldn't generate a response…", which is never posted) and a turn whose only reply threw on the way to Twist. Otherwise a transient flake silently consumes the request. When the placeholder *trails real content* (OpenCLAW appends it to tool-presentation output), only the placeholder line is stripped — the real content ships and the turn counts as answered, because retrying it would post that content twice. Once retries are exhausted the item goes `failed` **loudly**: a ❌ reaction, an in-place reply telling the sender it hit an error, and an alert to the ops thread. Nobody is left wondering if they were ignored.

**Crash recovery:** on boot only, any item still `processing` (the process died mid-turn) is an orphan. The consumer probes Twist: if the bot posted in that peer after the item's `claimedAt`, the reply already went out → `done`; otherwise the item goes back to `queued` (attempts preserved) — unless it has already burned all 6 attempts, in which case it's dead-lettered rather than re-claimed on every boot forever (a poison message must not become a crash loop). A live turn is never re-probed or re-dispatched while the process is running — only at boot.

**Backpressure:** if the non-terminal queue depth exceeds 50, a one-shot alert fires to the ops thread — a wedged consumer must not be silent.

**Corrupt store:** if the queue file is unreadable (half-written crash, hand-edited, wrong shape), it's quarantined to `queue.json.corrupt-<timestamp>` and the queue starts empty rather than boot-looping the account; cursors still bound what gets refetched, so this can't cause old messages to be re-answered. The cursor file gets the same treatment (`cursors.json.corrupt-<timestamp>`), and both files are written tmp-file + fsync + atomic rename, so neither is ever partial at rest. Losing cursors is safe: every thread/conversation reads as first sight again, and the 2h grace window baselines the backlog away.

### Webhook ingress (optional, off by default)

If the gateway is reachable from the internet, Twist's **outgoing webhooks** can cut the "someone mentioned the bot → the bot starts working" latency from up to one poll interval down to a couple of seconds. It is a *progressive enhancement*: it changes **nothing** about how messages are ingested, only *when*.

**The contract — read this before enabling it.**

- **Twist outgoing webhooks are unsigned.** There is no HMAC, no timestamp, nothing to verify a delivery actually came from Twist. So the payload is **never ingested as data**.
- **Hint-only, fetch-on-notify.** The single thing taken from a delivery is a *container hint* — "something may have happened in thread 123". That hint triggers a targeted sweep of a container the pipeline already tracks (see the gate below), which **re-fetches the truth from the Twist API with our own token**, through exactly the same cursor/dedup/fast-ack path the poll loop uses. Message content, sender, channel, timestamps in the payload are all ignored; even the thread's `channel_id` (which selects per-channel mention policy) is re-read from the API rather than believed.
- **Therefore the blast radius of a perfectly forged payload is one rate-limited, authenticated re-poll of one container.** It cannot inject a message, impersonate a sender, or make the bot say anything.
- **Poll remains the source of truth.** A delivery that never arrives, arrives twice, arrives out of order, or is dropped entirely **costs nothing** — the next poll picks the same messages up, deduped by id. There is no reconciliation to do and no failure mode to monitor. If the webhook is broken, the bot is simply as fast as it was before.
- Unrecognizable payloads fall back to scheduling a normal full poll, which is what would have happened anyway.
- Hints are **debounced per container, leading-edge**: the first hint for an idle container sweeps immediately, and further hints during the next ~2s collapse into a single trailing sweep. So a lone mention pays no debounce delay at all, while a burst on one thread still costs at most two sweeps.
- A targeted sweep then runs **straight away, off the poll loop's cycle chain** — waiting behind a running poll cycle was the single largest remaining source of ingest latency (measured: the same DM landing at 3.0s or 6.6s depending only on where the poll cycle happened to be). At most one sweep per container runs at a time, and at most a handful across all containers; past that, hints fall back to the chain-bound path. *Claiming* stays chain-serialized, so per-peer and global turn caps are untouched. Every stage is bounded; see [Behaviour under flood](#behaviour-under-flood).

**Setup.** Both keys are required; set only one and no route is registered (a path without a token would be an unauthenticated trigger open to the internet).

```json5
channels: {
  twist: {
    webhook: "/twist/events",          // gateway route path
    // webhookToken: "${TWIST_WEBHOOK_TOKEN}",  // prefer the env var
  },
}
```

Then in Twist → **Settings → Integrations → Add integration → Outgoing webhook**, set the URL to:

```
https://<gateway-host>/twist/events?token=<TWIST_WEBHOOK_TOKEN>
```

The token goes **in the URL query string** because Twist's outgoing-webhook UI accepts a URL and nothing else — no custom headers. Since the delivery is unsigned, that token is the *only* authentication on the endpoint. `X-Twist-Webhook-Token: <token>` and `Authorization: Bearer <token>` are also accepted, for curl and for relays that can set headers.

**Treat the whole URL as a secret.** Tokens shorter than 24 characters are refused outright (logged as an error, ingress stays off) — use `openssl rand -hex 32`. Keep it out of committed config via `TWIST_WEBHOOK_TOKEN`.

> **Operational note:** a token in a query string **appears in Render's platform request logs**, and in any proxy or CDN log in front of the gateway. That is inherent to the URL-token design Twist forces, not a bug in this plugin — but it means anyone with log access effectively has the token. Rotate on any suspicion of leak, and on staff offboarding. **Rotation is two steps that must happen together:** update `TWIST_WEBHOOK_TOKEN` (or `channels.twist.webhookToken`) *and* update the URL in Twist's integration settings. Between the two the endpoint returns 401 and deliveries are dropped — which costs nothing, because the poll loop covers the gap.

### Behaviour under flood

The endpoint is reachable by anyone who learns the URL, so it is built to make a flood boring rather than to assume it won't happen. Every layer degrades toward "just do a normal poll", which is always safe because a full poll is a strict superset of any set of targeted sweeps.

| Layer | Bound |
|---|---|
| Rate limit | 120 requests/min per (path, client IP), fixed window (SDK default) → 429 |
| Concurrency | 8 in-flight handlers per (path, client IP) (SDK default) → 429. Catches slow-body floods that stay under the rate cap |
| Body | 64 KiB, 10s read timeout → 413 / 408 |
| Debounce | Leading-edge per container: first hint sweeps at once, then at most one trailing sweep per ~2s window |
| Bypass sweeps | 1 in flight per container (a second hint is dropped — the running sweep already re-fetches current state), 4 across all containers. Beyond that, hints fall back to the chain-bound funnel |
| Pending hints | 32 distinct containers max. Beyond that the pending set is **dropped** and one full poll runs instead |
| Per cycle | At most 32 containers swept; any remainder becomes a full poll |
| Cycle deadline | 2 minutes. A cycle that overruns is abandoned (loudly logged) and the poll chain is released, so a stalling Twist API can never make the bot go quiet. Cursor + queue dedup make the next cycle's re-sweep safe |
| Container gates | Untracked threads *and* conversations are never swept on a hint — they degrade to a full poll |

Client IPs are resolved through the gateway's `trustedProxies` config. Without it, everything behind a reverse proxy shares one rate-limit bucket and a single sender could push everyone else into 429s — so **after deploying, verify that two different source IPs land in distinct buckets** (send >120 req/min from one and confirm the other still gets 200s).

**The enhancement is purely accelerative — it never widens what gets ingested.** A targeted sweep only ever runs against a container the pipeline **already tracks**. Anything else degrades to scheduling an ordinary full poll:

- **Threads** — the poll filters channel threads to `direct_mention` before sweeping, so an ungated hint would first-sight threads the poll would never have touched and turn workspace chatter into durable queue rows. A brand-new thread that *does* mention the bot is still picked up promptly, by the poll the hint just triggered.
- **Conversations** — the poll only sweeps conversations off the *unread* list. An ungated hint would let whoever holds the URL token name any conversation id and have the bot fetch it: an authenticated enumeration primitive. Gated, a hint reveals nothing about a conversation we don't already follow, and a genuinely new conversation still arrives via the hint-triggered poll.

The upshot: you can install the integration workspace-wide without it inflating the queue or leaking anything.

Requests are guarded the same way the host's bundled webhook channels guard theirs: POST only, per-IP fixed-window rate limiting, JSON content-type enforcement, a 64 KiB/10s bounded body read, constant-time token comparison, and anomaly counters on every rejection. The handler schedules and returns — the HTTP 200 never waits on a Twist round-trip.

### State files

- `.state/cursors.json` — per-thread/conversation fetch cursor. A refetch-bound optimization only, not a dedup source of truth. Twist's own read state is never mutated by it.
- `.state/queue.json` — the durable queue. Source of truth for what's been seen and its outcome.
- `.state/webhook-trace.jsonl` — webhook latency breadcrumbs, one JSON line per accepted hint (`{t, k, e}`: receive time, container key, and the event's own `posted_ts` when the payload carried one). Pure diagnostics: never read back by the pipeline, rotated at ~1MB (one previous generation kept), and safe to delete at any time. `e` comes from the **unsigned** payload and is used only to split Twist's delivery latency from ours — never as a cursor, freshness input, or dedup key.

### Migration

On first boot with a version that has the queue, the existing `cursors.json` is imported unchanged as the fetch baseline (everything at-or-below a cursor is treated as already seen); the queue itself starts empty and fills from the next poll onward. Rollback = repin the previous plugin version — the queue file is additive and ignored by old code.

## Requirements

- OpenCLAW **≥ 2026.6.6**
- A dedicated **Twist bot account** and an OAuth token for it, with scopes:
  `user:read`, `workspaces:read`, `channels:read`, `threads:read`/`write`,
  `comments:read`/`write`, `messages:read`/`write`, `reactions:write`
  (add `search:read` if you also use the optional MCP server below).
- The bot account's numeric **user id** (for `@mention` detection). You can find it with:
  ```bash
  curl -s -H "Authorization: Bearer $TWIST_TOKEN" \
    https://api.twist.com/api/v3/users/get_session_user | jq '{id, name}'
  ```
  > **Note:** the Twist bearer value includes the literal `oauth2:` prefix
  > (e.g. `oauth2:abc123…`). Sending the bare token returns `403 Invalid token`.

## Install

```bash
# from a local checkout (recommended for now)
openclaw plugins install --link /path/to/openclaw-twist
```

The host resolves `openclaw/plugin-sdk/*` itself — you do **not** need to vendor or symlink `openclaw` into the plugin.

## Configuration

Provide credentials via **environment variables** (preferred — nothing secret in your config):

```bash
export TWIST_TOKEN="oauth2:…"      # bot account token (incl. oauth2: prefix)
export TWIST_WORKSPACE_ID="133876"
export TWIST_BOT_USER_ID="634870"
```

Then enable the channel in `~/.openclaw/openclaw.json`:

```json5
{
  channels: {
    twist: {
      enabled: true,
      dmPolicy: "open",            // any workspace member can DM the bot
      allowFrom: ["*"],            // required when dmPolicy is "open"
      groupPolicy: "open",
      groups: { "*": { requireMention: true } },
    },
  },
  plugins: {
    load: { paths: ["/path/to/openclaw-twist"] },
    entries: { twist: { enabled: true } },
  },
}
```

See [`openclaw.twist.example.json5`](./openclaw.twist.example.json5) for a fully-annotated example.

**Secret resolution order** for the token: `channels.twist.token` → `channels.twist.tokenFile` → `TWIST_TOKEN`. If you must put it in config, use `"${TWIST_TOKEN}"` substitution rather than a literal. `workspaceId`/`botUserId` fall back to `TWIST_WORKSPACE_ID`/`TWIST_BOT_USER_ID`.

### Config reference

| Key | Type | Default | Notes |
|---|---|---|---|
| `enabled` | boolean | `false` | Turn the channel on |
| `token` / `tokenFile` | string | — | Bot token (incl. `oauth2:`), or a file path |
| `workspaceId` | int/string | — | Twist workspace id |
| `botUserId` | int/string | — | Bot account user id (mention target) |
| `pollIntervalMs` | int | `15000` | Poll cadence (2000–600000) |
| `dmPolicy` | enum | `open` | `pairing` \| `allowlist` \| `open` \| `disabled` |
| `allowFrom` | string[] | — | DM allowlist (`["*"]` for open) |
| `groupPolicy` | enum | `open` | `open` \| `allowlist` \| `disabled` |
| `groups."*".requireMention` | boolean | `true` | Require `@mention` in groups/threads |
| `defaultTo` | string | — | Default delivery target for cron/proactive messages (e.g. `"thread:7882650"` or `"conv:123"`). Falls back to `TWIST_DEFAULT_TO` env var. |
| `webhook` | string | — | Optional gateway route path for [webhook ingress](#webhook-ingress-optional-off-by-default), e.g. `"/twist/events"`. Falls back to `TWIST_WEBHOOK_PATH`. |
| `webhookToken` | string | — | Shared secret presented as `?token=…`. Falls back to `TWIST_WEBHOOK_TOKEN`. Required alongside `webhook` — set one without the other and nothing is registered. **Minimum 24 characters**; shorter tokens are refused with an error and ingress stays off. |

## Optional: richer Twist tools via MCP

Register Doist's official [`@doist/twist-ai`](https://github.com/Doist/twist-ai) MCP server to give the agent extra Twist tools (search, inbox, react, mark-done, build-link) inside its sessions:

```bash
openclaw mcp add twist-ai --command npx --arg -y --arg @doist/twist-ai \
  --env TWIST_API_KEY="$TWIST_TOKEN" --parallel
```

This channel does **not** require the MCP server — it handles inbound routing and replies on its own. The MCP is purely additive agent capability.

## Development

```bash
npm test          # routing + queue + producer + consumer + cursor-store suites (no network, no SDK needed)
```

`node --test` covers the whole ingestion pipeline without touching the network: routing rules and markup (`routing.test.js`), the durable store's persistence/dedup/pruning/quarantine (`queue.test.js`), forward pagination and first-sight baselining (`producer.test.js`), the delivery state machine — retries, dead-lettering, per-peer and global concurrency, crash recovery (`consumer.test.js`), and cursor durability (`state.test.js`). The pure logic (routing rules, mention/self-filtering, cursor advancement, target parsing, markup cleaning) lives in `src/routing.js`. SDK-coupled code (`src/channel.js`, `src/inbound.js`, `src/monitor.js`) is validated by loading in a running gateway.

For local iteration against your own gateway, you can symlink the host SDK so standalone `node` imports resolve (gitignored):
```bash
mkdir -p node_modules && ln -s "$(npm root -g)/openclaw" node_modules/openclaw
```

## Limitations (v1)

- Single account per workspace.
- Text only — no inbound/outbound attachments or media.
- No streaming (replies post as a whole message).
- Reactions are status indicators only (not used as triggers).
- Group **group-mentions** (`twist-group-mention://`) aren't treated as a direct mention.

## License

[MIT](./LICENSE) © Sanctuary Computer
