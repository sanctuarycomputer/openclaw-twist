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

The agent receives full Twist context, not just the bare mention: the **thread title**, **channel name**, and a **transcript** of the surrounding discussion (Twist `[Name](twist-mention://id)` markup is cleaned to readable `@Name`).

## How it works

Twist's API can't deliver webhooks to a loopback-bound gateway, so this channel **polls** Twist's unread endpoints on an interval (default 15s) from **inside the gateway process** — there's no separate service to run or supervise. It registers via the channel `gateway.startAccount` lifecycle (`runStoppablePassiveMonitor`), filters channel threads to mention-only before fetching (so it ignores the noise of every unread thread), and **baselines on first sight** so it never replies to pre-existing backlog. Fetching and dispatch are decoupled by a durable job queue, so every message reaches a terminal outcome — even across crashes — and one slow agent turn never stalls polling.

### Ingestion queue

A producer/queue/consumer pipeline replaces a single fetch-decide-dispatch pass:

- **Producer** (transport only): every poll, enqueues every new message/comment by Twist's globally-unique id. Enqueue is idempotent — an id already in the queue, live or tombstoned, is a no-op — so a cursor bug now causes a harmless refetch, not lost messages. No policy at this layer, only self-post exclusion. Fetches **page forward from the cursor** (ascending, up to 10 pages per container per poll) rather than grabbing the newest N — a burst bigger than one fetch window is drained across polls instead of being skipped. A thread's opening post is enqueued too (id `thread-post:<threadId>`), so a thread created by @mentioning the bot gets answered.
- **Queue**: a persistent state machine, one record per message. States: `queued → processing → done | skipped | failed`. Every transition is durably persisted (tmp-file + fsync + atomic rename) before its side effects proceed.
- **Consumer**: claims `queued` items oldest-first, at most **one turn per peer** at a time (keeps a thread/DM's turns ordered and its session coherent — a busy peer's next mention waits instead of being dropped) and at most **3 turns globally** (box/spend protection). Policy — mention check, admission gate — is evaluated at claim time and recorded as a terminal `skipped` state, never silently dropped.

Skip reasons (all logged, none silent):

| Reason | Meaning |
|---|---|
| `backlog` | Item was already older than the 2h first-sight grace window when its thread/conversation was first seen. The only sanctioned non-answer — in steady-state production it should never fire. |
| `no-mention` | Group DM / channel thread item without a bot `@mention` (1:1 DMs are open). |
| `admission:<verdict>` | OpenCLAW's ingress gate returned a non-dispatch verdict. |
| `gone` | The trigger no longer exists on Twist (deleted message, permanent 4xx). Non-retryable. |

**Retries:** a failed turn rides a backoff ladder — 30s, 2m, 10m, 1h, 1h — up to 6 attempts. Once retries are exhausted the item goes `failed` **loudly**: a ❌ reaction, an in-place reply telling the sender it hit an error, and an alert to the ops thread. Nobody is left wondering if they were ignored.

**Crash recovery:** on boot only, any item still `processing` (the process died mid-turn) is an orphan. The consumer probes Twist: if the bot posted in that peer after the item's `claimedAt`, the reply already went out → `done`; otherwise the item goes back to `queued` (attempts preserved) — unless it has already burned all 6 attempts, in which case it's dead-lettered rather than re-claimed on every boot forever (a poison message must not become a crash loop). A live turn is never re-probed or re-dispatched while the process is running — only at boot.

**Backpressure:** if the non-terminal queue depth exceeds 50, a one-shot alert fires to the ops thread — a wedged consumer must not be silent.

**Corrupt store:** if the queue file is unreadable (half-written crash, hand-edited, wrong shape), it's quarantined to `queue.json.corrupt-<timestamp>` and the queue starts empty rather than boot-looping the account; cursors still bound what gets refetched, so this can't cause old messages to be re-answered.

### State files

- `.state/cursors.json` — per-thread/conversation fetch cursor. A refetch-bound optimization only, not a dedup source of truth. Twist's own read state is never mutated by it.
- `.state/queue.json` — the durable queue. Source of truth for what's been seen and its outcome.

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

## Optional: richer Twist tools via MCP

Register Doist's official [`@doist/twist-ai`](https://github.com/Doist/twist-ai) MCP server to give the agent extra Twist tools (search, inbox, react, mark-done, build-link) inside its sessions:

```bash
openclaw mcp add twist-ai --command npx --arg -y --arg @doist/twist-ai \
  --env TWIST_API_KEY="$TWIST_TOKEN" --parallel
```

This channel does **not** require the MCP server — it handles inbound routing and replies on its own. The MCP is purely additive agent capability.

## Development

```bash
npm test          # routing/parsing/markup unit tests (no network, no SDK needed)
```

The pure logic (routing rules, mention/self-filtering, cursor advancement, target parsing, markup cleaning) lives in `src/routing.js` and is fully unit-tested. SDK-coupled code (`src/channel.js`, `src/inbound.js`, `src/monitor.js`) is validated by loading in a running gateway.

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
