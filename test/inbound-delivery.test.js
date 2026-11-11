// The delivery object handleTwistInbound hands to openclaw's dispatchReply. openclaw emits
// message_sent on this path only when delivery.observeMessageSent is set; that event is how
// deliveries get counted. openclaw isn't installed here, so the four plugin-sdk modules
// inbound.js reaches are stubbed with an in-thread loader hook (node:module registerHooks,
// Node >=22.15); on older Node the file skips.
import test from "node:test";
import assert from "node:assert/strict";
import module from "node:module";

const canHook = typeof module.registerHooks === "function";
const skip = canHook ? false : "needs node:module registerHooks (Node >=22.15)";

const STUBS = {
  "openclaw/plugin-sdk/channel-ingress-runtime": `
    export function createChannelIngressResolver() { throw new Error("stub: admission is passed in"); }
    export const defineStableChannelIngressIdentity = (x) => x;`,
  "openclaw/plugin-sdk/inbound-envelope": `
    export function resolveInboundRouteEnvelopeBuilderWithRuntime() {
      return {
        route: { agentId: "main", sessionKey: "agent:main:twist:direct:u1", accountId: "default" },
        buildEnvelope: ({ body }) => ({ storePath: "/nonexistent", body }),
      };
    }`,
  "openclaw/plugin-sdk/runtime-store": `
    export function createPluginRuntimeStore({ errorMessage }) {
      let rt = null;
      return {
        setRuntime: (r) => { rt = r; },
        getRuntime: () => { if (!rt) throw new Error(errorMessage); return rt; },
        clearRuntime: () => { rt = null; },
      };
    }`,
  "openclaw/plugin-sdk/channel-config-helpers": `
    export const createTopLevelChannelConfigAdapter = (o) => o;`,
};

let inbound, runtimeStore;
if (canHook) {
  module.registerHooks({
    resolve(spec, ctx, next) {
      if (spec in STUBS) return { url: `stub:${spec}`, shortCircuit: true };
      // A new SDK import fails here, loudly, instead of resolving to nothing.
      if (spec.startsWith("openclaw/")) throw new Error(`unstubbed openclaw specifier: ${spec}`);
      return next(spec, ctx);
    },
    load(url, ctx, next) {
      if (url.startsWith("stub:")) return { format: "module", source: STUBS[url.slice(5)], shortCircuit: true };
      return next(url, ctx);
    },
  });
  inbound = await import("../src/inbound.js");
  runtimeStore = await import("../src/runtime.js");
}

// One admitted DM through handleTwistInbound; returns what dispatchReply received, what was
// posted to Twist, and what onDelivery heard.
async function runDm() {
  let captured;
  const posts = [];
  const deliveries = [];
  runtimeStore.setTwistRuntime({
    channel: {
      reply: { finalizeInboundContext: (c) => c, dispatchReplyWithBufferedBlockDispatcher: () => {} },
      inbound: { dispatchReply: (p) => { captured = p; } },
      session: { recordInboundSession: () => {} },
    },
  });
  await inbound.handleTwistInbound({
    message: { kind: "conv", conversationId: 77, peerId: "u1", peerKind: "direct", isGroup: false, text: "hello" },
    account: { accountId: "default", botUserId: 1 },
    cfg: { session: {} },
    runtime: { log() {}, error() {} },
    client: { addConversationMessage: async (id, body) => { posts.push([id, body]); return { id: 1 }; } },
    verdict: { admit: true, commandAuthorized: false },
    onDelivery: (o) => deliveries.push(o),
  });
  return { captured, posts, deliveries };
}

test("the dispatchReply delivery sets observeMessageSent: true", { skip }, async () => {
  const { captured } = await runDm();
  assert.equal(captured.delivery.observeMessageSent, true);
});

test("the delivery object gains only observeMessageSent", { skip }, async () => {
  // No onDelivered, preparePayload, durable or provider-send variant: those would change delivery.
  const { captured } = await runDm();
  assert.deepEqual(Object.keys(captured.delivery).sort(), ["deliver", "observeMessageSent", "onError"]);
});

test("a DM reply is still posted once, as written", { skip }, async () => {
  const { captured, posts, deliveries } = await runDm();
  await captured.delivery.deliver({ text: "hi there" });
  assert.deepEqual(posts, [[77, "hi there"]]);
  assert.deepEqual(deliveries, [{ delivered: true }]);
});
