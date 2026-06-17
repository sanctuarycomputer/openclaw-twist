// Account/config resolution for the single-account Twist channel.
import { readFileSync } from "node:fs";
import { createTopLevelChannelConfigAdapter } from "openclaw/plugin-sdk/channel-config-helpers";

export const SECTION_KEY = "twist";
export const DEFAULT_ACCOUNT_ID = "default";
export const DEFAULT_POLL_INTERVAL_MS = 15000;

/** @typedef {{accountId:string, config:any, token?:string, workspaceId?:number|string, botUserId?:number|string, pollIntervalMs:number, configured:boolean}} TwistAccount */

const firstNonEmpty = (...vals) => {
  for (const v of vals) {
    if (v === undefined || v === null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return undefined;
};

/** Resolve a secret from an inline value, a *File path, or an env var (in that order). */
function resolveSecret({ value, file, envName }) {
  const inline = firstNonEmpty(value);
  if (inline) return inline;
  if (file) {
    try {
      const fromFile = readFileSync(file, "utf8").trim();
      if (fromFile) return fromFile;
    } catch {
      /* fall through to env */
    }
  }
  return firstNonEmpty(process.env[envName]);
}

/**
 * Resolve the Twist account from `channels.twist`, with env-var / tokenFile
 * fallbacks so no secret needs to live in committed config:
 *   token       ← config.token | config.tokenFile | $TWIST_TOKEN
 *   workspaceId ← config.workspaceId | $TWIST_WORKSPACE_ID
 *   botUserId   ← config.botUserId | $TWIST_BOT_USER_ID
 */
export function resolveTwistAccount(cfg) {
  const section = cfg?.channels?.[SECTION_KEY] ?? {};
  const token = resolveSecret({ value: section.token, file: section.tokenFile, envName: "TWIST_TOKEN" });
  const workspaceId = firstNonEmpty(section.workspaceId, process.env.TWIST_WORKSPACE_ID);
  const botUserId = firstNonEmpty(section.botUserId, process.env.TWIST_BOT_USER_ID);
  const pollIntervalMs =
    Number(section.pollIntervalMs ?? process.env.TWIST_POLL_INTERVAL_MS) || DEFAULT_POLL_INTERVAL_MS;
  return {
    accountId: DEFAULT_ACCOUNT_ID,
    config: section,
    token,
    workspaceId,
    botUserId,
    pollIntervalMs,
    configured: Boolean(token && workspaceId && botUserId),
  };
}

export const twistConfigAdapter = createTopLevelChannelConfigAdapter({
  sectionKey: SECTION_KEY,
  resolveAccount: resolveTwistAccount,
  clearBaseFields: [
    "token",
    "workspaceId",
    "botUserId",
    "pollIntervalMs",
    "allowFrom",
    "groupAllowFrom",
    "groups",
    "mentionPatterns",
    "dmPolicy",
    "groupPolicy",
  ],
  resolveAllowFrom: (account) => account.config.allowFrom,
  formatAllowFrom: (allowFrom) => (allowFrom ?? []).map((e) => String(e)),
});

/** Whether a channel group/thread requires an @mention (default: true). */
export function resolveRequireMention(account, groupId) {
  const groups = account.config.groups ?? {};
  const exact = groupId != null ? groups[String(groupId)] : undefined;
  const wildcard = groups["*"];
  const v = exact?.requireMention ?? wildcard?.requireMention;
  return v === undefined ? true : Boolean(v);
}
