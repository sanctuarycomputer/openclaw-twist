// test/state.test.js — cursor store durability. The cursor is "only" a refetch bound, but
// a corrupt file that throws on load boot-loops the account, and a torn write can hand the
// producer a cursor that never existed.
import test from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCursorStore } from "../src/state.js";

const T0 = 1_785_900_000_000;
const freshPath = () => join(mkdtempSync(join(tmpdir(), "twists-")), "cursors.json");

test("corrupt cursor file is quarantined, not fatal: store loads empty and stays usable", async () => {
  const path = freshPath();
  writeFileSync(path, '{"threads": {"7": 4'); // half-written before a hard crash
  const logs = [];
  const store = createCursorStore(path, { log: (m) => logs.push(m) });

  await store.load(T0); // must NOT throw — a throw here boot-loops the account forever

  assert.equal(store.isFirstSight("threads", 7), true); // started empty
  assert.equal(existsSync(`${path}.corrupt-${T0}`), true);
  assert.equal(existsSync(path), false); // moved aside, not left to poison the next load
  assert.match(logs.join("\n"), /corrupt/i);

  // still writable afterwards
  await store.setCursor("threads", 7, 12);
  assert.equal(store.getCursor("threads", 7), 12);
});

test("valid-JSON-but-wrong-shape cursor file is also quarantined", async () => {
  const path = freshPath();
  writeFileSync(path, "[1,2,3]");
  const store = createCursorStore(path);
  await store.load(T0);
  assert.equal(store.isFirstSight("conversations", 9), true);
  assert.equal(existsSync(`${path}.corrupt-${T0}`), true);
});

test("persist/load round-trip: cursors survive a restart and the file is valid JSON at rest", async () => {
  const path = freshPath();
  const store = createCursorStore(path);
  await store.load(T0);
  await store.setCursor("threads", 7, 810);
  await store.setCursor("conversations", 9, 3);
  await store.setCursor("threads", 7, 800); // lower value never regresses the cursor
  JSON.parse(readFileSync(path, "utf8")); // atomic rename ⇒ never a partial file at rest

  const reloaded = createCursorStore(path);
  await reloaded.load(T0);
  assert.equal(reloaded.getCursor("threads", 7), 810);
  assert.equal(reloaded.getCursor("conversations", 9), 3);
  assert.equal(reloaded.isFirstSight("threads", 7), false);
  assert.equal(reloaded.isFirstSight("threads", 8), true);
});

test("a missing cursor file is not an error (fresh install)", async () => {
  const store = createCursorStore(freshPath());
  await store.load(T0);
  assert.equal(store.isFirstSight("threads", 1), true);
  assert.equal(store.getCursor("threads", 1), -Infinity);
});
