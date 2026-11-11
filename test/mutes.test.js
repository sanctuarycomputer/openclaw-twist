import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createMuteList, muteFilePath, isLive, RECHECK_MS, DEFAULT_NEVER_MUTE } from "../src/mutes.js";

const NOW = Date.parse("2026-11-11T12:00:00Z");
const entry = (over = {}) => ({ kind: "dm", reason: "spend", mutedAt: "2026-11-10T00:00:00Z", until: null, by: "spend-guard", ...over });
function setup(content, { clock = { t: NOW }, neverMute } = {}) {
  const dir = mkdtempSync(join(tmpdir(), "twistmute-"));
  const path = join(dir, "twist-muted.json");
  if (content !== undefined) writeFileSync(path, typeof content === "string" ? content : JSON.stringify(content));
  const logs = [];
  const mutes = createMuteList({ path, log: (m) => logs.push(m), now: () => clock.t, ...(neverMute ? { neverMute } : {}) });
  return { dir, path, logs, mutes, clock };
}

test("muteFilePath: $OPENCLAW_STATE_DIR/twist-muted.json, or null (nothing muted) without it", () => {
  assert.equal(muteFilePath({ OPENCLAW_STATE_DIR: "/data" }), "/data/twist-muted.json");
  assert.equal(muteFilePath({}), null);
  const mutes = createMuteList({ path: null, log: () => {} });
  assert.equal(mutes.isMuted("conv:9"), false);
});

test("a live entry mutes its conversation or thread; an expired one is ignored", () => {
  const { mutes } = setup({
    version: 1,
    conversations: { 9: entry(), 10: entry({ until: "2026-11-11T11:59:00Z" }), 11: entry({ until: "2026-11-11T13:00:00Z" }) },
    threads: { 7: entry({ kind: "thread" }) },
  });
  assert.equal(mutes.isMuted("conv:9"), true);
  assert.equal(mutes.isMuted("conv:10"), false, "until in the past = unmuted");
  assert.equal(mutes.isMuted("conv:11"), true);
  assert.equal(mutes.isMuted("thread:7"), true);
  assert.equal(mutes.isMuted("thread:9"), false, "buckets are separate: thread 9 is not conversation 9");
  assert.equal(isLive(entry({ until: "not a date" }), NOW), false, "an unparseable until mutes nothing");
});

test("a mute expires on its own as time passes, without a file change", () => {
  const { mutes, clock } = setup({ version: 1, conversations: { 9: entry({ until: "2026-11-11T12:05:00Z" }) } });
  assert.equal(mutes.isMuted("conv:9"), true);
  clock.t += 6 * 60_000;
  assert.equal(mutes.isMuted("conv:9"), false);
});

test("missing or corrupt file: nothing muted, logged once (not once per poll)", () => {
  const missing = setup(undefined);
  for (let i = 0; i < 3; i++) { missing.clock.t += RECHECK_MS; assert.equal(missing.mutes.isMuted("conv:9"), false); }
  assert.equal(missing.logs.length, 1);
  assert.match(missing.logs[0], /not present: nothing muted/);

  const corrupt = setup("{ not json");
  for (let i = 0; i < 3; i++) { corrupt.clock.t += RECHECK_MS; assert.equal(corrupt.mutes.isMuted("conv:9"), false); }
  assert.equal(corrupt.logs.length, 1);
  assert.match(corrupt.logs[0], /corrupt \(nothing muted/);

  const wrongShape = setup("[1,2,3]");
  assert.equal(wrongShape.mutes.isMuted("conv:9"), false);
  assert.match(wrongShape.logs[0], /corrupt/);
});

test("the file is re-read when it changes (at most one stat per RECHECK_MS)", () => {
  const { path, mutes, clock } = setup({ version: 1, conversations: {} });
  assert.equal(mutes.isMuted("conv:9"), false);
  writeFileSync(path, JSON.stringify({ version: 1, conversations: { 9: entry() } }));
  assert.equal(mutes.isMuted("conv:9"), false, "not re-checked inside RECHECK_MS");
  clock.t += RECHECK_MS;
  assert.equal(mutes.isMuted("conv:9"), true);
  // An unmute (until = now) is obeyed the same way.
  writeFileSync(path, JSON.stringify({ version: 1, conversations: { 9: entry({ until: new Date(clock.t).toISOString() }) } }));
  clock.t += RECHECK_MS;
  assert.equal(mutes.isMuted("conv:9"), false);
});

test("Hugh's DM and the ops thread are never muted: the entry is ignored with one warning", () => {
  assert.deepEqual([...DEFAULT_NEVER_MUTE], ["conv:1367817", "thread:7882650"]);
  const { mutes, logs } = setup({ version: 1, conversations: { 1367817: entry() }, threads: { 7882650: entry() } });
  assert.equal(mutes.isMuted("conv:1367817"), false);
  assert.equal(mutes.isMuted("conv:1367817"), false);
  assert.equal(mutes.isMuted("thread:7882650"), false);
  const warns = logs.filter((l) => l.startsWith("WARNING"));
  assert.equal(warns.length, 2, "one warning per ignored entry");
  assert.match(warns[0], /ignoring mute of conv:1367817/);
  assert.equal(mutes.mute("conv:1367817", entry()).written, false);
  assert.equal(mutes.isMuted("conv:1367817"), false);
});

test("neverMute is configurable", () => {
  const { mutes } = setup({ version: 1, conversations: { 1367817: entry(), 5: entry() } }, { neverMute: ["conv:5"] });
  assert.equal(mutes.isMuted("conv:1367817"), true);
  assert.equal(mutes.isMuted("conv:5"), false);
});

test("mute(): merges into the file atomically, keeping every other entry and unknown field", () => {
  const theirs = entry();
  const { path, dir, mutes } = setup({ version: 1, note: "keep me", conversations: { 9: theirs }, threads: {} });
  const mine = entry({ by: "twist-loop-breaker", until: "2026-11-11T18:00:00Z" });
  assert.deepEqual(mutes.mute("thread:7", mine), { written: true });
  const file = JSON.parse(readFileSync(path, "utf8"));
  assert.deepEqual(file.conversations["9"], theirs);
  assert.deepEqual(file.threads["7"], mine);
  assert.equal(file.note, "keep me");
  assert.equal(mutes.isMuted("thread:7"), true);
  assert.deepEqual(readdirSync(dir), ["twist-muted.json"], "no temp file left behind");
});

test("mute(): never rewrites another writer's entry, and never overwrites a corrupt file; the peer is still muted in memory", () => {
  const theirs = entry({ until: "2026-11-11T11:00:00Z" }); // expired, owned by spend-guard
  const owned = setup({ version: 1, conversations: { 9: theirs } });
  const res = owned.mutes.mute("conv:9", entry({ by: "twist-loop-breaker", until: "2026-11-11T18:00:00Z" }));
  assert.equal(res.written, false);
  assert.match(res.why, /spend-guard owns that entry/);
  assert.deepEqual(JSON.parse(readFileSync(owned.path, "utf8")).conversations["9"], theirs);
  assert.equal(owned.mutes.isMuted("conv:9"), true, "muted in this process anyway");

  const corrupt = setup("{ half-edited");
  const r2 = corrupt.mutes.mute("conv:9", entry({ by: "twist-loop-breaker", until: "2026-11-11T18:00:00Z" }));
  assert.equal(r2.written, false);
  assert.equal(readFileSync(corrupt.path, "utf8"), "{ half-edited");
  assert.equal(corrupt.mutes.isMuted("conv:9"), true);
});

test("mute(): a written mute lives in the file only, so a person's unmute there wins", () => {
  const { path, mutes, clock } = setup(undefined);
  assert.equal(mutes.mute("conv:9", entry({ by: "twist-loop-breaker", until: "2026-11-11T18:00:00Z" })).written, true);
  const file = JSON.parse(readFileSync(path, "utf8"));
  file.conversations["9"].until = new Date(clock.t).toISOString();
  writeFileSync(path, JSON.stringify(file));
  clock.t += RECHECK_MS;
  assert.equal(mutes.isMuted("conv:9"), false);
});
