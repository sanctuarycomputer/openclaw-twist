// SDK-coupled webhook wiring: HTTP route registration + the request pipeline.
//
// Split from src/webhook.js on purpose. Everything in here imports from the `openclaw`
// host, which is NOT resolvable in the plugin's own dev/test environment — so all the
// decision logic (hint extraction, debouncing, the handler) lives in webhook.js and is
// unit-tested there, and this file stays a thin, boring adapter with no logic of its own.
//
// The guard stack mirrors the bundled reference channel (see the host's
// monitor.webhook-*.js): method allowlist → per-IP fixed-window rate limit → JSON
// content-type → shared-secret auth (constant time) → bounded/timed JSON body read →
// anomaly counters on every rejection. Only after all of that does an (unsigned, still
// untrusted) payload reach the hint extractor.
import {
  WEBHOOK_ANOMALY_COUNTER_DEFAULTS,
  WEBHOOK_RATE_LIMIT_DEFAULTS,
  applyBasicWebhookRequestGuards,
  createFixedWindowRateLimiter,
  createWebhookAnomalyTracker,
  readJsonWebhookBodyOrReject,
  registerWebhookTargetWithPluginRoute,
  resolveRequestClientIp,
  resolveWebhookTargetWithAuthOrRejectSync,
  withResolvedWebhookRequestPipeline,
} from "openclaw/plugin-sdk/webhook-ingress";
import { safeEqualSecret } from "openclaw/plugin-sdk/security-runtime";
import { extractRequestToken } from "./webhook.js";

// A Twist event is a handful of small fields. 64 KiB is already generous, and a tight cap
// is free protection: nothing downstream reads more than two ids out of the body.
const MAX_BODY_BYTES = 64 * 1024;
const BODY_TIMEOUT_MS = 10_000;

/** Path → live targets. Module-scoped so the route handler and registration share it. */
const webhookTargets = new Map();

const rateLimiter = createFixedWindowRateLimiter({
  windowMs: WEBHOOK_RATE_LIMIT_DEFAULTS.windowMs,
  maxRequests: WEBHOOK_RATE_LIMIT_DEFAULTS.maxRequests,
  maxTrackedKeys: WEBHOOK_RATE_LIMIT_DEFAULTS.maxTrackedKeys,
});
const anomalyTracker = createWebhookAnomalyTracker({
  maxTrackedKeys: WEBHOOK_ANOMALY_COUNTER_DEFAULTS.maxTrackedKeys,
  ttlMs: WEBHOOK_ANOMALY_COUNTER_DEFAULTS.ttlMs,
  logEvery: WEBHOOK_ANOMALY_COUNTER_DEFAULTS.logEvery,
});

function recordStatus(runtime, path, statusCode) {
  anomalyTracker.record({
    key: `${path}:${statusCode}`,
    statusCode,
    log: runtime?.log,
    message: (count) => `[twist] webhook anomaly path=${path} status=${statusCode} count=${String(count)}`,
  });
}

/**
 * Rate-limit bucket for a request: path + resolved client IP.
 *
 * MUST be keyed on the normalized PATH, never `req.url` — the token rides in the query
 * string, so keying on the raw URL would both leak the secret into limiter state and give
 * every distinct query string its own bucket (i.e. no effective limit at all).
 *
 * The client IP is resolved with the gateway's `trustedProxies` config, which is why this
 * runs after target resolution rather than up front: behind a reverse proxy (the normal
 * deployment) an unconfigured resolver returns the *proxy's* address for every request, so
 * all callers would share one bucket and any single sender could rate-limit everyone else
 * off the endpoint.
 */
function rateLimitKeyFor(req, path, target) {
  const gateway = target?.config?.gateway;
  const ip =
    resolveRequestClientIp(req, gateway?.trustedProxies, gateway?.allowRealIpFallback === true) ??
    req.socket?.remoteAddress ??
    "unknown";
  return `${path}:${ip}`;
}

async function handleTwistWebhookRequest(req, res) {
  return await withResolvedWebhookRequestPipeline({
    req,
    res,
    targetsByPath: webhookTargets,
    allowMethods: ["POST"],
    handle: async ({ targets, path }) => {
      const nowMs = Date.now();
      if (
        !applyBasicWebhookRequestGuards({
          req,
          res,
          rateLimiter,
          rateLimitKey: rateLimitKeyFor(req, path, targets[0]),
          nowMs,
        })
      ) {
        recordStatus(targets[0]?.runtime, path, res.statusCode);
        return true;
      }
      if (!applyBasicWebhookRequestGuards({ req, res, requireJsonContentType: true })) {
        recordStatus(targets[0]?.runtime, path, res.statusCode);
        return true;
      }

      const presented = extractRequestToken(req.url, req.headers);
      const target = resolveWebhookTargetWithAuthOrRejectSync({
        targets,
        res,
        isMatch: (entry) => safeEqualSecret(entry.secret, presented),
      });
      if (!target) {
        recordStatus(targets[0]?.runtime, path, res.statusCode);
        return true;
      }

      const body = await readJsonWebhookBodyOrReject({
        req,
        res,
        maxBytes: MAX_BODY_BYTES,
        timeoutMs: BODY_TIMEOUT_MS,
        emptyObjectOnEmpty: false,
        invalidJsonMessage: "Bad Request",
      });
      if (!body.ok) {
        recordStatus(target.runtime, path, res.statusCode);
        return true;
      }

      // Decide + SCHEDULE only. The sweep itself runs on the poll loop's own turn; nothing
      // here awaits Twist, so the 200 goes out in milliseconds and the upstream never
      // retries us into a stampede. A malformed/garbage body is not an error to the sender
      // either — it degrades to a scheduled full poll and still answers 200.
      try {
        target.onEvent(body.value, { token: presented });
        target.statusSink?.({ lastInboundAt: Date.now() });
      } catch (err) {
        target.runtime?.error?.(`[${target.accountId}] twist webhook handler failed: ${String(err)}`);
      }
      res.statusCode = 200;
      res.end("ok");
      return true;
    },
  });
}

/**
 * Register the account's webhook route. Returns a deregister function; call it on stop.
 *
 * @param {object} p
 * @param {string} p.accountId
 * @param {string} p.path      route path, e.g. "/twist/events"
 * @param {string} p.secret    shared secret presented as ?token=
 * @param {object} p.config    resolved gateway config (for trustedProxies)
 * @param {object} p.runtime   plugin runtime (log/error)
 * @param {(patch:object)=>void} [p.statusSink]
 * @param {(body:unknown, ctx:{token:string})=>object} p.onEvent  the pure handler
 * @returns {() => void} deregister
 */
export function registerTwistWebhookRoute({ accountId, path, secret, config, runtime, statusSink, onEvent }) {
  const { unregister } = registerWebhookTargetWithPluginRoute({
    targetsByPath: webhookTargets,
    target: { path, accountId, secret, config, runtime, statusSink, onEvent },
    route: {
      auth: "plugin",
      match: "exact",
      pluginId: "twist",
      source: "twist-webhook",
      accountId,
      log: runtime?.log,
      handler: async (req, res) => {
        if (!(await handleTwistWebhookRequest(req, res)) && !res.headersSent) {
          res.statusCode = 404;
          res.setHeader("Content-Type", "text/plain; charset=utf-8");
          res.end("Not Found");
        }
        return true;
      },
    },
  });
  return unregister;
}
