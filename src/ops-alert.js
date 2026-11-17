// ops-alert.js — the ops alert: post an alert text to the ops target, holding back an incident alert the bot
// already posted there in the last 6h. Moved out of monitor.js (which imports the SDK and can't load under
// node --test) so the dedupe is testable; monitor.js wires it to the Twist client.
import { ALERT_HEADER, isRepeatIncident } from "./consumer.js";

/**
 * `target()` → `{ kind, id }`; `readRecent(kind, id)` → promise of that target's recent comments;
 * `post({ kind, id, text })` → promise. The returned `alert(text)` resolves to whether it tried to post:
 * true once `post` was called (whether it then resolved or rejected), false for a repeat held back, or when
 * `target()` or `readRecent` threw synchronously before any post. It never rejects.
 */
export function createOpsAlert({ target, readRecent, post, botUserId, log, now = Date.now }) {
  return async (text) => {
    let tried = false;
    try {
      const { kind, id } = target();
      // An incident alert already in the thread within 6h is not posted again (restarts reset in-process flags).
      // A failed read never blocks the alert.
      const recent = String(text).startsWith(ALERT_HEADER) ? await readRecent(kind, id).catch(() => []) : [];
      if (isRepeatIncident(recent, text, { botUserId, nowMs: now() })) {
        log(`alert not repeated (already posted within 6h): ${String(text).split("\n")[1] ?? ""}`.slice(0, 200));
        return false;
      }
      tried = true;
      await post({ kind, id, text });
    } catch (err) {
      log(`alert delivery failed: ${String(err)}`);
    }
    return tried;
  };
}
