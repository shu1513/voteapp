/**
 * Hard wall-clock guard for manual-wrapper validation phases.
 *
 * A profile write once hung forever inside source validation (a redirected
 * host that neither responded nor tripped the per-fetch abort) and the
 * wrapper produced no output and no repair report — indistinguishable from a
 * wrapper that was never started. Per-fetch timeouts bound each request, but
 * nothing bounded the phase as a whole. This wraps a validation promise in a
 * wall-clock ceiling that always surfaces a structured, labeled error, so a
 * hang becomes a loud failure instead of silence. (ERR-346)
 */

export const VALIDATION_WALL_CLOCK_TIMEOUT_MS = 300_000;

export async function withWallClockTimeout<T>(
  promise: Promise<T>,
  label: string,
  timeoutMs: number = VALIDATION_WALL_CLOCK_TIMEOUT_MS
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new Error(
          `${label} exceeded the ${timeoutMs}ms wall-clock ceiling and was aborted. A per-fetch timeout should have tripped first, so this usually means a host is hanging in a way the fetch abort does not cover — retry, and if it persists, isolate the offending URL.`
        )
      );
    }, timeoutMs);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}
