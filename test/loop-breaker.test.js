import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createMuteList, RECHECK_MS } from "../src/mutes.js";
import { createLoopBreaker, resolveLoopGuardConfig, loopAlertText, LOOP_BREAKER_BY, DEFAULT_CRITICAL_TO } from "../src/loop-breaker.js";
import { ALERT_HEADER } from "../src/consumer.js";

const NOW = Date.parse("2026-11-11T12:00:00Z");
const CONFIG = { enabled: true, maxTurns: 40, windowMinutes: 30, muteHours: 6 };
const item = (peerId, over = {}) => ({ id: `x:${Math.random()}`, kind: "conv", peerId, senderId: 99, senderName: "Help Desk", ...over });

function setup({ config = CONFIG, file } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistlb-"));
  const path = join(dir, "twist-muted.json");
  if (file) writeFileSync(path, JSON.stringify(file));
  const clock = { t: NOW };
  const logs = [];
  const alerts = [];
  const mutes = createMuteList({ path, log: (m) => logs.push(m), now: () => clock.t });
  const breaker = createLoopBreaker({ mutes, notify: async (t) => { alerts.push(t); }, config, log: (m) => logs.push(m), now: () => clock.t });
  return { path, clock, logs, alerts, mutes, breaker };
}
// `n` turns for one peer, `stepMs` apart; returns each admit() verdict.
async function run(breaker, clock, peerId, n, stepMs, over) {
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push(await breaker.admit(item(peerId, over), { kind: "dm" }));
    clock.t += stepMs;
  }
  return out;
}

test("resolveLoopGuardConfig: defaults, overrides, and junk numbers fall back", () => {
  assert.deepEqual(resolveLoopGuardConfig({}), {
    neverMute: ["conv:1367817", "thread:7882650"],
    criticalTo: DEFAULT_CRITICAL_TO,
    loopBreaker: { enabled: true, maxTurns: 40, windowMinutes: 30, muteHours: 6 },
  });
  const custom = resolveLoopGuardConfig({ neverMute: ["conv:5"], criticalTo: "thread:1", loopBreaker: { enabled: false, maxTurns: 10, windowMinutes: -1, muteHours: "x" } });
  assert.deepEqual(custom.neverMute, ["conv:5"]);
  assert.equal(custom.criticalTo, "thread:1");
  assert.deepEqual(custom.loopBreaker, { enabled: false, maxTurns: 10, windowMinutes: 30, muteHours: 6 });
});

test("trips at maxTurns+1 inside the window: that turn is refused, the peer is written to the file, one alert", async () => {
  const { breaker, clock, path, alerts, mutes } = setup({ file: { version: 1, conversations: { 5: { kind: "dm", until: null, by: "spend-guard" } }, threads: {} } });
  const verdicts = await run(breaker, clock, "conv:1349360", 41, 30_000); // 41 turns in 20 minutes
  assert.deepEqual(verdicts.slice(0, 40), Array(40).fill(true));
  assert.equal(verdicts[40], false);
  const file = JSON.parse(readFileSync(path, "utf8"));
  assert.deepEqual(file.conversations["5"], { kind: "dm", until: null, by: "spend-guard" }, "the other writer's entry is untouched");
  const e = file.conversations["1349360"];
  assert.equal(e.by, LOOP_BREAKER_BY);
  assert.equal(e.kind, "dm");
  assert.equal(e.until, new Date(NOW + 40 * 30_000 + 6 * 3600_000).toISOString());
  assert.match(e.reason, /41 turns in 20 min \(limit 40 per 30 min\); last sender Help Desk/);
  assert.equal(mutes.isMuted("conv:1349360"), true);
  assert.equal(alerts.length, 1);
  assert.ok(alerts[0].startsWith(`${ALERT_HEADER}\nCritical: my DM with Help Desk (conversation 1349360) looks like a loop. I answered 41 messages there in 20 minutes`));
  assert.match(alerts[0], /stopped answering it until 2026-11-11 18:20 UTC/);
  assert.match(alerts[0], /node scripts\/twist-mute\.mjs unmute 1349360/);
});

test("a slower peer never trips: 40 turns per 30 minutes, sustained, stays under the limit", async () => {
  const { breaker, clock, alerts, path } = setup();
  const verdicts = await run(breaker, clock, "conv:77", 200, 46_000); // ~39 per 30 min
  assert.ok(verdicts.every(Boolean));
  assert.equal(alerts.length, 0);
  assert.throws(() => readFileSync(path), /ENOENT/, "nothing written");
});

test("peers are counted separately", async () => {
  const { breaker, clock, alerts } = setup();
  for (let i = 0; i < 40; i++) {
    assert.equal(await breaker.admit(item("conv:1"), { kind: "dm" }), true);
    assert.equal(await breaker.admit(item("thread:2", { kind: "thread" }), { kind: "thread" }), true);
    clock.t += 10_000;
  }
  assert.equal(alerts.length, 0);
});

test("a protected peer (Hugh's DM) trips: alert only, it keeps being answered, and the alert is not repeated every window", async () => {
  const { breaker, clock, alerts, path, mutes } = setup();
  const verdicts = await run(breaker, clock, "conv:1367817", 41 * 3, 10_000, { senderName: "Hugh" });
  assert.ok(verdicts.every(Boolean), "never refused");
  assert.equal(mutes.isMuted("conv:1367817"), false);
  assert.throws(() => readFileSync(path), /ENOENT/, "never written to the file");
  assert.equal(alerts.length, 1, "one alert per muteHours, not one per trip");
  assert.match(alerts[0], /still answering it because it is on the never-mute list/);
});

test("disabled: never trips", async () => {
  const { breaker, clock, alerts } = setup({ config: { ...CONFIG, enabled: false } });
  assert.ok((await run(breaker, clock, "conv:1", 100, 1000)).every(Boolean));
  assert.equal(alerts.length, 0);
});

test("a person's unmute holds: the next trip within muteHours alerts but does not re-mute", async () => {
  const { breaker, clock, alerts, path } = setup();
  await run(breaker, clock, "conv:9", 41, 1000);
  assert.equal(alerts.length, 1);
  // Hugh unmutes it (scripts/twist-mute.mjs sets until = now).
  const file = JSON.parse(readFileSync(path, "utf8"));
  file.conversations["9"].until = new Date(clock.t).toISOString();
  writeFileSync(path, JSON.stringify(file));
  clock.t += RECHECK_MS;
  const verdicts = await run(breaker, clock, "conv:9", 41, 1000);
  assert.ok(verdicts.every(Boolean));
  assert.equal(alerts.length, 2);
  assert.match(alerts[1], /still answering it because someone unmuted it/);
});

test("when the file cannot be written the peer is still muted here, and the alert says a restart undoes it", async () => {
  const { breaker, clock, alerts, path, mutes } = setup();
  writeFileSync(path, "{ half-edited");
  await run(breaker, clock, "conv:9", 41, 1000);
  assert.equal(mutes.isMuted("conv:9"), true);
  assert.equal(readFileSync(path, "utf8"), "{ half-edited");
  assert.match(alerts[0], /or the next restart.*couldn't record it in twist-muted\.json/s);
});

test("a fault inside the breaker lets the turn through", async () => {
  const mutes = { isProtected: () => { throw new Error("boom"); } };
  const logs = [];
  const breaker = createLoopBreaker({ mutes, notify: async () => {}, config: { ...CONFIG, maxTurns: 1 }, log: (m) => logs.push(m), now: () => NOW });
  assert.equal(await breaker.admit(item("conv:1")), true);
  assert.equal(await breaker.admit(item("conv:1")), true);
  assert.ok(logs.some((l) => /loop breaker failed/.test(l)));
});

test("loopAlertText names a thread plainly", () => {
  const t = loopAlertText({ peerId: "thread:42", kind: "thread", senderName: "Zapier", turns: 41, minutes: 12, outcome: "muted", untilMs: NOW });
  assert.match(t, /Critical: thread 42 looks like a loop\. I answered 41 messages there in 12 minutes, faster than any person writes\. The latest was from Zapier\./);
  assert.match(t, /unmute 42/);
});
