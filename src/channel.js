// The Twist ChannelPlugin object, shared by the runtime entry (index.js) and the
// lightweight setup entry (setup-entry.js).
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { createChatChannelPlugin } from "openclaw/plugin-sdk/channel-core";
import {
  runStoppablePassiveMonitor,
  resolveLoggerBackedRuntime,
} from "openclaw/plugin-sdk/extension-shared";
import { createAccountStatusSink } from "openclaw/plugin-sdk/channel-lifecycle";
import { getTwistRuntime } from "./runtime.js";
import { twistConfigAdapter, resolveTwistAccount, resolveRequireMention } from "./config.js";
import { twistOutbound } from "./outbound.js";
import { monitorTwistProvider } from "./monitor.js";
import { createCursorStore } from "./state.js";

const PLUGIN_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const STATE_FILE = join(PLUGIN_ROOT, ".state", "cursors.json");

export const meta = {
  id: "twist",
  label: "Twist",
  selectionLabel: "Twist (Workspace + Token)",
  detailLabel: "Twist",
  docsPath: "/channels/twist",
  blurb: "Twist channel — poll-based DM and @mention routing for a bot account.",
  markdownCapable: true,
  quickstartAllowFrom: true,
};

async function startTwistAccount(ctx) {
  const core = getTwistRuntime();
  const account = resolveTwistAccount(ctx.cfg);
  if (!account.configured) {
    throw new Error(
      `Twist is not configured for "${ctx.accountId}" — set channels.twist.token + workspaceId + botUserId ` +
        "(or TWIST_TOKEN / TWIST_WORKSPACE_ID / TWIST_BOT_USER_ID env vars).",
    );
  }
  const logger = core.logging.getChildLogger({ channel: "twist", accountId: ctx.accountId });
  const runtime = resolveLoggerBackedRuntime(ctx.runtime, logger);
  const statusSink = ctx.setStatus
    ? createAccountStatusSink({ accountId: ctx.accountId, setStatus: ctx.setStatus })
    : undefined;
  const cursors = createCursorStore(STATE_FILE);
  await cursors.load();
  ctx.log?.info?.(`[${ctx.accountId}] starting Twist poller (workspace ${account.workspaceId})`);
  await runStoppablePassiveMonitor({
    abortSignal: ctx.abortSignal,
    start: async () =>
      monitorTwistProvider({
        accountId: account.accountId,
        config: ctx.cfg,
        runtime,
        abortSignal: ctx.abortSignal,
        statusSink,
        cursors,
      }),
  });
}

export const twistPlugin = createChatChannelPlugin({
  base: {
    id: "twist",
    meta,
    capabilities: { chatTypes: ["direct", "group"], media: false, blockStreaming: true },
    reload: { configPrefixes: ["channels.twist"] },
    config: {
      ...twistConfigAdapter,
      isConfigured: (account) => account.configured,
      hasConfiguredState: ({ cfg }) => resolveTwistAccount(cfg).configured,
      describeAccount: (account) => ({
        configured: account.configured,
        extra: { workspaceId: account.workspaceId, botUserId: account.botUserId },
      }),
    },
    groups: {
      resolveRequireMention: ({ cfg, groupId }) =>
        resolveRequireMention(resolveTwistAccount(cfg), groupId),
    },
    gateway: { startAccount: startTwistAccount },
  },
  security: {
    dm: {
      channelKey: "twist",
      resolvePolicy: (account) => account.config.dmPolicy,
      resolveAllowFrom: (account) => account.config.allowFrom,
      defaultPolicy: "open",
    },
  },
  outbound: twistOutbound,
});
