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
  const settlePromises = new Set(); // test-only visibility into in-flight settle() work; see idle()
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
    // Test-only: await all settle() work currently tracked. NOT for production use —
    // if a claimed item's runTurn never resolves (e.g. hung upstream call), this hangs
    // forever. tick()'s own claim loop never awaits this.
    async idle() { await Promise.allSettled([...settlePromises]); },
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
        const p = settle(claimed).finally(() => settlePromises.delete(p)); // fire-and-forget; settle() clears inFlight
        settlePromises.add(p);
      }
    },
  };
}
