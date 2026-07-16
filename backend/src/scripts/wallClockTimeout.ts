/**
 * Hard wall-clock guard for manual-wrapper validation phases.
 *
 * A profile write once hung forever inside source validation (a redirected
 * host that neither responded nor tripped the per-fetch abort) and the
 * wrapper produced no output and no repair report — indistinguishable from a
 * wrapper that was never started. Per-fetch timeouts bound each request, but
 * nothing bounded the phase as a whole. This puts a wall-clock ceiling on a
 * validation promise so a hang becomes a loud, labeled failure instead of
 * silence. (ERR-346)
 *
 * The race ABANDONS the underlying work — it cannot cancel in-flight
 * fetches, and an open network handle keeps the Node event loop (and so the
 * process) alive even after the entrypoint sets a nonzero exit code. Callers
 * that need the process to actually die pass `forceExitAfterMs`: when the
 * ceiling fires, a grace timer lets the entrypoint print its structured
 * failure, then the process hard-exits with code 1.
 */

export const VALIDATION_WALL_CLOCK_TIMEOUT_MS = 300_000;

export const WALL_CLOCK_FORCE_EXIT_GRACE_MS = 2_000;

export async function withWallClockTimeout<T>(
  promise: Promise<T>,
  label: string,
  options: { timeoutMs?: number; forceExitAfterMs?: number | null } = {}
): Promise<T> {
  const { timeoutMs = VALIDATION_WALL_CLOCK_TIMEOUT_MS, forceExitAfterMs = null } = options;

  // If the ceiling fires first and the abandoned promise later rejects, that
  // rejection has no consumer (the race already settled) and would crash the
  // process as an unhandledRejection — attach a no-op handler up front. When
  // the promise wins the race, the race result still propagates normally.
  promise.catch(() => {});

  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      if (forceExitAfterMs !== null) {
        setTimeout(() => {
          process.exit(1);
        }, forceExitAfterMs);
      }
      reject(
        new Error(
          `${label} exceeded the ${timeoutMs}ms wall-clock ceiling and was abandoned (in-flight fetches cannot be cancelled${forceExitAfterMs !== null ? `; the process will force-exit in ${forceExitAfterMs}ms` : ""}). A per-fetch timeout should have tripped first, so this usually means a host is hanging in a way the fetch abort does not cover — retry, and if it persists, isolate the offending URL.`
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
