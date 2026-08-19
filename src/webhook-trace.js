// Latency breadcrumbs for webhook ingress. Diagnostics only — nothing in this file feeds a
// decision, and nothing downstream reads it back.
//
// The point is to split the end-to-end "someone typed" → "the bot reacted" number into
// TWIST DELIVERY (their side) and OUR PROCESSING (ours). We can only measure the second
// half from inside the process, so each accepted hint records the moment we received it
// alongside the event's own `posted_ts` when the payload carried one. Subtracting the two
// in the ledger gives the delivery leg.
//
// SECURITY NOTE on `eventTs`: it comes from an UNSIGNED, attacker-controllable payload. It
// is written to a local diagnostics file and used for nothing else — never as a cursor,
// never as a freshness/backlog input, never as a dedup key. A forged value can only corrupt
// a latency statistic. (Contrast the real `posted_ts` the pipeline uses, which comes from
// our own authenticated fetch.) It is deliberately kept out of the hint object so it cannot
// travel into the sweep path by accident.
import { appendFile, mkdir, rename, stat } from "node:fs/promises";
import { dirname } from "node:path";

/** Truncation threshold — roughly 1MB of JSONL. */
export const DEFAULT_TRACE_MAX_BYTES = 1_000_000;

/**
 * Append-only JSONL trace with a bounded footprint.
 *
 * Rotation keeps ONE previous generation (`<path>.1`) rather than truncating outright, so
 * crossing the cap never destroys the entire recent history — the window we would actually
 * want when investigating a latency regression. Disk use is bounded at ~2x the cap.
 *
 * Every write is best-effort and chained: a failure is logged once and swallowed, because a
 * diagnostics file must never be able to fail an ingest. Writes are serialized through a
 * promise chain so lines can never interleave into a corrupt record.
 *
 * @param {string} filePath
 * @param {object} [opts]
 * @param {number} [opts.maxBytes]
 * @param {(msg:string)=>void} [opts.log]
 */
export function createWebhookTrace(filePath, { maxBytes = DEFAULT_TRACE_MAX_BYTES, log } = {}) {
  let chain = Promise.resolve();
  let bytes = null; // lazily seeded from the file on first write
  let warned = false;

  async function currentSize() {
    try {
      return (await stat(filePath)).size;
    } catch {
      return 0; // missing file (fresh install) is not an error
    }
  }

  async function rotate() {
    try {
      await rename(filePath, `${filePath}.1`);
    } catch (err) {
      // Nothing to rotate, or we cannot — either way the append below still bounds growth
      // on the next pass, so this must not stop us recording.
      log?.(`webhook trace rotation failed: ${String(err)}`);
    }
  }

  return {
    /**
     * Record one accepted hint. Never throws; callers `void` the result.
     *
     * @param {{t:number, k:string, e?:number|null}} entry
     * @returns {Promise<void>} settled write, for tests
     */
    record({ t, k, e }) {
      const line = `${JSON.stringify(e == null ? { t, k } : { t, k, e })}\n`;
      chain = chain
        .then(async () => {
          if (bytes === null) {
            await mkdir(dirname(filePath), { recursive: true });
            bytes = await currentSize();
          }
          if (bytes + line.length > maxBytes) {
            await rotate();
            bytes = 0;
          }
          await appendFile(filePath, line);
          bytes += line.length;
        })
        .catch((err) => {
          // Once per process: a broken trace file must not spam the log on every delivery.
          if (!warned) {
            warned = true;
            log?.(`webhook trace write failed (further failures suppressed): ${String(err)}`);
          }
          bytes = null; // re-stat next time in case the situation resolves
        });
      return chain;
    },
    /** Test-only: await all pending writes. */
    idle: () => chain,
  };
}
