// failure-line.js — the failure line: one `STACKSBOT_FAILURE {json}` line per failure, written to the
// gateway's stdout so Stacksbot's System Health Review can count the inbox's failures.
//
// Vendored from sanctuarycomputer/stacksbot scripts/lib/failure-line.mjs at a3bf1f5a (contract
// docs/contracts/failure-line.md 1.0). Change it only with that file; test/failure-line.test.js pins it to the
// contract's fixture lines. SOURCES is home's scripts/lib/failure-producers.mjs list, written out here.
//
// The line is closed by construction: the formatter reads each known field once, validates that snapshot
// against a closed vocabulary or a narrow pattern, and writes only it. No message text, secret, session key,
// or id other than a uuid can reach the line, and the formatter never throws.
export const PREFIX = "STACKSBOT_FAILURE ";
export const VERSION = 1;
export const SOURCES = Object.freeze([
  "job-failure-alert",
  "notion-sync-failure",
  "deploy-smoke-test",
  "self-model-check",
  "spend-guard-session",
  "spend-guard-credit",
  "fix-run-flagged",
  "twist-inbox-dead-letter",
  "twist-inbox-storage",
  "twist-inbox-backlog",
  "twist-inbox-hung-turn",
  "twist-inbox-loop-breaker",
  "system-cron",
  "job-delivery",
  "twist-inbox-retry",
  "gateway",
]);
export const KINDS = Object.freeze(["job", "sync", "check", "inbox", "tool", "turn", "model"]);
export const CLASSES = Object.freeze([
  "interrupted",
  "exec-failed",
  "mcp-tool",
  "incomplete-turn",
  "subagent",
  "timeout",
  "session-init",
  "unknown",
  "unschedulable",
  "fanout",
  "delivery-failed",
  "smoke-regression",
  "capped",
  "stale",
  "unavailable",
  "runaway-session",
  "billing",
  "fix-run-flagged",
  "retry",
  "dead-letter",
  "loop-trip",
  "storage",
  "backlog",
  "hung-turn",
  "tool-error",
  "turn-error",
  "model-error",
]);
export const CAUSES = Object.freeze([
  "http-4xx",
  "http-5xx",
  "http-429",
  "timeout",
  "connection",
  "billing",
  "rate-limited",
  "bot-down",
  "none",
  "unknown",
]);
export const SESSIONS = Object.freeze(["cron", "user", "heartbeat", "subagent", "other"]);
export const INBOX_STEPS = Object.freeze(["consumer", "dead-letter", "loop-breaker", "storage", "backlog", "watchdog"]);
export const MAX_LINE_BYTES = 512;
export const SKEW_MS = 60_000;
export const MAX_DELAY_MS = 420_000;
export const RATE_WINDOW_MS = 60_000;
export const FIELD_ORDER = Object.freeze([
  "v",
  "fid",
  "at",
  "kind",
  "source",
  "what",
  "class",
  "cause",
  "alerted",
  "streak",
  "session",
  "job_id",
  "run",
  "attempt",
  "suppressed",
]);
export const OPTIONAL_DROP_ORDER = Object.freeze(["suppressed", "attempt", "run", "job_id", "streak", "session"]);

const FID_RE = /^f-[0-9a-z]{8,9}-[0-9a-z]{4}$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const NAME_RE = /^[a-z0-9][a-z0-9_./:-]{0,79}$/;
const MODEL_RE = /^[a-z0-9][a-z0-9_.-]*\/[a-z0-9][a-z0-9_./:-]*$/;
const MAX_WHAT = 80;
const TOKEN_PREFIXES = [
  "sk-",
  "ghp_",
  "gho_",
  "ghs_",
  "ghu_",
  "github_pat_",
  "xoxa-",
  "xoxb-",
  "xoxp-",
  "xoxr-",
  "xoxs-",
  "ntn_",
  "secret_",
  "akia",
  "eyj",
  "glpat-",
];
const TOKEN_PREFIX_RE = new RegExp(`(?:^|[._/:-])(?:${TOKEN_PREFIXES.join("|")})`);
const LONG_RUN_RE = /[a-z0-9]{20,}/g;

/** A run of 20+ [a-z0-9] holding a letter and a digit, or a token prefix at the start or after a separator. */
function secretShaped(value) {
  for (const run of value.match(LONG_RUN_RE) ?? []) if (/[a-z]/.test(run) && /[0-9]/.test(run)) return true;
  return TOKEN_PREFIX_RE.test(value);
}

/** An openclaw session key (`agent:<id>:<channel>:…`) or anything with two colons. */
function sessionKeyShaped(value) {
  return value.startsWith("agent:") || value.split(":").length > 2;
}

/**
 * The per-kind `what` rule (contract §3), shared by the formatter and the parser so they can't drift.
 * @returns {string | null} the value `String()`ed once, trimmed and lowercased, or null when it fails.
 */
export function checkWhat(kind, value) {
  if (!KINDS.includes(kind) || value === undefined || value === null) return null;
  let v;
  try {
    v = String(value).trim().toLowerCase();
  } catch {
    return null;
  }
  if (v.length === 0 || v.length > MAX_WHAT) return null;
  if (kind === "inbox" && !INBOX_STEPS.includes(v)) return null;
  if (kind === "turn" && !SESSIONS.includes(v)) return null;
  if (kind === "model" && !MODEL_RE.test(v)) return null;
  if (kind !== "inbox" && kind !== "turn" && kind !== "model" && !NAME_RE.test(v)) return null;
  if (secretShaped(v) || sessionKeyShaped(v)) return null;
  return v;
}

const oneOf = (list, value) => (typeof value === "string" && list.includes(value) ? value : undefined);
const intIn = (value, lo, hi) => (Number.isInteger(value) && value >= lo && value <= hi ? value : undefined);
const uuid = (value) => (typeof value === "string" && UUID_RE.test(value) ? value : undefined);

/** The optional fields' rules, shared by the formatter and the parser: the value, or undefined when it fails. */
const OPTIONAL_RULES = {
  streak: (v) => intIn(v, 1, 9999),
  session: (v) => oneOf(SESSIONS, v),
  job_id: uuid,
  run: uuid,
  attempt: (v) => intIn(v, 1, 99),
  suppressed: (v) => intIn(v, 1, 999999),
};

/** A time as `toISOString()`, read through Date's own slot so a forged Date or a Proxy can't supply text. */
function isoOf(value) {
  let ms;
  if (typeof value === "number" || typeof value === "string") ms = new Date(value).getTime();
  else {
    try {
      ms = Date.prototype.getTime.call(value);
    } catch {
      return undefined;
    }
  }
  return Number.isFinite(ms) ? new Date(ms).toISOString() : undefined;
}

function makeFid(ms, rand) {
  let tail = "";
  for (let i = 0; i < 4; i++) tail += (Math.floor(rand() * 36) % 36).toString(36);
  return `f-${ms.toString(36)}-${tail}`;
}

/** A generated fid that fits its own pattern even under an odd injected clock or random source. */
function newFid(ms, rand) {
  const fid = makeFid(ms, rand);
  return FID_RE.test(fid) ? fid : makeFid(Date.now(), Math.random);
}

const render = (object) => `${PREFIX}${JSON.stringify(object)}\n`;

/**
 * Build one failure line from `fields`.
 * @returns {string | null} the line with its `\n`, or null when `fields` is not a plain object or a read throws.
 */
export function failureLine(fields, { now = Date.now, rand = Math.random, maxBytes = MAX_LINE_BYTES } = {}) {
  try {
    if (fields === null || typeof fields !== "object" || Array.isArray(fields)) return null;
    const snap = {};
    for (const key of FIELD_ORDER) if (Object.hasOwn(fields, key)) snap[key] = fields[key];

    let ms = now();
    if (!Number.isFinite(ms)) ms = Date.now();
    const kind = oneOf(KINDS, snap.kind) ?? "?";
    const out = {
      v: VERSION,
      fid: typeof snap.fid === "string" && FID_RE.test(snap.fid) ? snap.fid : newFid(Math.trunc(ms), rand),
      at: isoOf(snap.at) ?? new Date(ms).toISOString(),
      kind,
      source: oneOf(SOURCES, snap.source) ?? "?",
      what: checkWhat(kind, snap.what) ?? "?",
      class: oneOf(CLASSES, snap.class) ?? "unknown",
      cause: oneOf(CAUSES, snap.cause) ?? "unknown",
      alerted: snap.alerted === true,
    };
    for (const [key, rule] of Object.entries(OPTIONAL_RULES)) {
      const value = rule(snap[key]);
      if (value !== undefined) out[key] = value;
    }

    let line = render(out);
    for (const key of OPTIONAL_DROP_ORDER) {
      if (Buffer.byteLength(line) <= maxBytes) break;
      if (key in out) {
        delete out[key];
        line = render(out);
      }
    }
    return line;
  } catch {
    return null;
  }
}

const CAUSE_RULES = [
  { cause: "billing", re: /billing|credit balance|insufficient.?credit|payment required|\b402\b/i },
  { cause: "rate-limited", re: /rate.?limit/i },
  { cause: "http-429", re: /\b429\b/i },
  { cause: "timeout", re: /timed? ?out|timeout|aborted|ETIMEDOUT/i },
  { cause: "connection", re: /ECONNREFUSED|ECONNRESET|ENOTFOUND|EAI_AGAIN|socket hang up/i },
  { cause: "http-5xx", re: /\b(?:http|status|code)\b\W{0,3}5\d\d\b/i },
  { cause: "http-4xx", re: /\b(?:http|status|code)\b\W{0,3}4\d\d\b/i },
];

/**
 * The closed `cause` of an error text (contract §3). The text is read in memory and dropped.
 * @returns {string} one of CAUSES other than `bot-down`.
 */
export function failureCause(text) {
  if (text === undefined || text === null) return "none";
  if (typeof text === "string" && text.trim() === "") return "none";
  let s;
  try {
    s = String(text);
  } catch {
    return "unknown";
  }
  for (const { cause, re } of CAUSE_RULES) if (re.test(s)) return cause;
  return "unknown";
}

/** The gateway process's own stdout: the sink for plugins, which must not use a logger (it adds a wrapper). */
export const stdoutSink = (line) => process.stdout.write(line);

const MAX_RATE_KEYS = 10_000;
const MAX_SUPPRESSED = 999_999;

/**
 * One line per (kind, source, what, class, cause) per window; the next line written for a key carries
 * `suppressed`. `alerted: true` lines skip the map entirely (contract §7). Past `maxKeys`, entries whose window
 * is over with nothing pending are pruned, and a key that still finds no room is written untracked, so a pending
 * count is never lost and memory stays bounded. `revert(verdict)` undoes the admit that returned `verdict`, for a
 * sink that then failed (a sink must not itself emit through the same limiter). A clock that steps back ends the
 * window rather than stretching it.
 */
export function createRateLimiter({ windowMs = RATE_WINDOW_MS, maxKeys = MAX_RATE_KEYS } = {}) {
  const entries = new Map();
  const undo = new WeakMap(); // verdict → { key, before }: what revert(verdict) restores
  return {
    admit(fields, nowMs) {
      if (fields.alerted === true) return { write: true };
      const key = [fields.kind, fields.source, fields.what, fields.class, fields.cause].join("|");
      const entry = entries.get(key);
      if (!entry) {
        if (entries.size >= maxKeys) {
          for (const [k, e] of entries) if (e.suppressed === 0 && nowMs - e.since >= windowMs) entries.delete(k);
        }
        const verdict = { write: true };
        if (entries.size < maxKeys) {
          entries.set(key, { since: nowMs, suppressed: 0 });
          undo.set(verdict, { key, before: null });
        }
        return verdict;
      }
      if (nowMs >= entry.since && nowMs - entry.since < windowMs) {
        entry.suppressed = Math.min(entry.suppressed + 1, MAX_SUPPRESSED);
        return { write: false };
      }
      const before = { ...entry };
      const suppressed = entry.suppressed;
      entry.since = nowMs;
      entry.suppressed = 0;
      const verdict = suppressed > 0 ? { write: true, suppressed } : { write: true };
      undo.set(verdict, { key, before });
      return verdict;
    },
    revert(verdict) {
      const record = undo.get(verdict);
      if (!record) return;
      undo.delete(verdict);
      if (record.before === null) entries.delete(record.key);
      else entries.set(record.key, record.before);
    },
  };
}

/**
 * Format, rate-limit and write one failure line. Synchronous and best effort: never throws, never returns a
 * promise. A sink must be synchronous; a thenable result counts as not written (its rejection is caught). When
 * the sink fails, the limiter's admit is reverted, so its count goes on the next line.
 * @param {unknown} fields
 * @param {{ sink?: (line: string) => unknown, now?: () => number, rand?: () => number,
 *   limiter?: { admit: (fields: object, nowMs: number) => { write: boolean, suppressed?: number },
 *     revert?: (verdict: object) => void } }} [options]
 * @returns {boolean} true when the sink took the line.
 */
export function emitFailure(fields, { sink, now = Date.now, rand = Math.random, limiter } = {}) {
  let admitted = null;
  const unadmit = () => {
    try {
      if (admitted) limiter.revert?.(admitted);
    } catch {
      // best effort: the line is lost either way
    }
  };
  try {
    if (typeof sink !== "function") return false;
    let ms = now();
    if (!Number.isFinite(ms)) ms = Date.now();
    const clock = () => ms;
    let line = failureLine(fields, { now: clock, rand });
    if (line === null) return false;
    if (limiter) {
      const object = JSON.parse(line.slice(PREFIX.length));
      const verdict = limiter.admit(object, ms);
      if (!verdict.write) return false;
      admitted = verdict;
      if (verdict.suppressed) line = failureLine({ ...object, suppressed: verdict.suppressed }, { now: clock, rand });
    }
    const result = /** @type {any} */ (sink(line));
    if (result !== null && result !== undefined && typeof result.then === "function") {
      unadmit();
      try {
        result.then(undefined, () => {});
      } catch {
        // a thenable whose then throws has nothing to catch
      }
      return false;
    }
    return true;
  } catch {
    unadmit();
    return false;
  }
}
