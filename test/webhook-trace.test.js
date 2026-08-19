import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, existsSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createWebhookTrace, DEFAULT_TRACE_MAX_BYTES } from "../src/webhook-trace.js";

const tracePath = (name = "webhook-trace.jsonl") => join(mkdtempSync(join(tmpdir(), "twist-trace-")), name);
const lines = (p) => readFileSync(p, "utf8").trim().split("\n").filter(Boolean);

test("trace: appends one JSON line per accepted hint", async () => {
  const p = tracePath();
  const trace = createWebhookTrace(p);

  await trace.record({ t: 1000, k: "thread:7882650", e: 999 });
  await trace.record({ t: 2000, k: "conversation:34567", e: null });
  await trace.idle();

  assert.deepEqual(lines(p).map((l) => JSON.parse(l)), [
    { t: 1000, k: "thread:7882650", e: 999 },
    { t: 2000, k: "conversation:34567" },
  ]);
});

test("trace: an absent eventTs is simply omitted, never written as null/NaN", async () => {
  const p = tracePath();
  const trace = createWebhookTrace(p);
  await trace.record({ t: 5, k: "thread:1" });
  await trace.idle();
  assert.equal(lines(p)[0], '{"t":5,"k":"thread:1"}');
});

test("trace: creates its directory on first write (fresh install)", async () => {
  const p = join(mkdtempSync(join(tmpdir(), "twist-trace-")), "nested", "deeper", "trace.jsonl");
  const trace = createWebhookTrace(p);
  await trace.record({ t: 1, k: "thread:1" });
  await trace.idle();
  assert.equal(lines(p).length, 1);
});

test("trace: writes are serialized, so concurrent records never interleave", async () => {
  const p = tracePath();
  const trace = createWebhookTrace(p);

  // Fire without awaiting — the chain must still produce whole, ordered records.
  for (let i = 0; i < 50; i++) trace.record({ t: i, k: `thread:${i}` });
  await trace.idle();

  const parsed = lines(p).map((l) => JSON.parse(l));
  assert.equal(parsed.length, 50);
  assert.deepEqual(parsed.map((r) => r.t), [...Array(50).keys()]);
});

test("trace: rotates at the cap instead of growing without bound", async () => {
  const p = tracePath();
  const trace = createWebhookTrace(p, { maxBytes: 200 });

  for (let i = 0; i < 40; i++) await trace.record({ t: i, k: `thread:${i}` });
  await trace.idle();

  const live = readFileSync(p, "utf8");
  assert.ok(live.length <= 200, `live file should stay under the cap, got ${live.length}`);
  assert.ok(existsSync(`${p}.1`), "one previous generation is kept, so recent history survives");
  assert.ok(live.includes('"t":39'), "the newest record is in the live file");
});

test("trace: total footprint stays bounded at ~2x the cap across many rotations", async () => {
  const p = tracePath();
  const trace = createWebhookTrace(p, { maxBytes: 200 });
  for (let i = 0; i < 500; i++) await trace.record({ t: i, k: `thread:${i}` });
  await trace.idle();

  const total = readFileSync(p, "utf8").length + readFileSync(`${p}.1`, "utf8").length;
  assert.ok(total <= 200 * 2 + 64, `expected ~2x cap, got ${total}`);
});

test("trace: picks up the size of a pre-existing file rather than starting from zero", async () => {
  const p = tracePath();
  writeFileSync(p, "x".repeat(190));
  const trace = createWebhookTrace(p, { maxBytes: 200 });

  await trace.record({ t: 1, k: "thread:1" });
  await trace.idle();

  assert.ok(existsSync(`${p}.1`), "the pre-existing bulk was rotated, not appended past the cap");
  assert.ok(readFileSync(p, "utf8").length < 200);
});

test("trace: a write failure is contained, logged once, and never throws at the caller", async () => {
  // A directory where the file should be: every append fails.
  const dir = mkdtempSync(join(tmpdir(), "twist-trace-"));
  const logs = [];
  const trace = createWebhookTrace(dir, { log: (m) => logs.push(m) });

  await trace.record({ t: 1, k: "thread:1" });
  await trace.record({ t: 2, k: "thread:2" });
  await trace.record({ t: 3, k: "thread:3" });
  await trace.idle();

  assert.equal(logs.length, 1, "logged once, not once per delivery");
});

test("trace: the default cap is ~1MB", () => {
  assert.equal(DEFAULT_TRACE_MAX_BYTES, 1_000_000);
});
