import type { Pool, PoolConfig } from "pg";

/**
 * Connection-pool settings for the request-serving API process.
 *
 * The pg defaults are built for scripts: wait forever for a free client
 * (connectionTimeoutMillis 0) and never cancel a statement. In the API that
 * means one stuck Postgres pins every request until the process is killed.
 * The API pool therefore always carries:
 *
 * - connectionTimeoutMillis — bounded wait for a free client when the pool
 *   is exhausted; the request fails fast instead of queueing forever.
 * - statement_timeout — a SERVER-side deadline (sent as a session parameter
 *   on connect), so Postgres cancels the statement itself and the client
 *   stays reusable.
 * - query_timeout — pg's client-side read timeout, kept LARGER than the
 *   server deadline. It only errors the query locally (no cancel is sent),
 *   so it must never fire first; it is a backstop for a connection that
 *   stopped answering altogether.
 */
export const API_DB_QUERY_TIMEOUT_MARGIN_MS = 5_000;

/**
 * Default deadline for the /api/healthz database probe. Render's HTTP probe
 * gives up after 5 s, and the pool's own limits (10 s to acquire a client,
 * 30 s per statement) are far past that, so the probe needs its own shorter
 * clock or a slow database reads as a dead instance.
 */
export const API_DB_HEALTH_DEADLINE_MS = 3_000;

export type ApiDbPoolConfig = Pick<
  PoolConfig,
  "connectionString" | "connectionTimeoutMillis" | "statement_timeout" | "query_timeout"
> & {
  connectionString: string;
  connectionTimeoutMillis: number;
  statement_timeout: number;
  query_timeout: number;
};

export function buildApiDbPoolConfig(input: {
  connectionString: string;
  connectionTimeoutMs: number;
  statementTimeoutMs: number;
}): ApiDbPoolConfig {
  for (const [name, value] of [
    ["connectionTimeoutMs", input.connectionTimeoutMs],
    ["statementTimeoutMs", input.statementTimeoutMs],
  ] as const) {
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error(`API DB pool ${name} must be a positive integer`);
    }
  }
  return {
    connectionString: input.connectionString,
    connectionTimeoutMillis: input.connectionTimeoutMs,
    statement_timeout: input.statementTimeoutMs,
    query_timeout: input.statementTimeoutMs + API_DB_QUERY_TIMEOUT_MARGIN_MS,
  };
}

/**
 * Builds the /api/healthz probe: `SELECT 1` on the pool, rejecting if it has
 * not answered within `deadlineMs`. Concurrent probes share one in-flight
 * query, and the slot stays taken until that query settles, even after every
 * caller's deadline has passed. The path skips the rate limiter, so without
 * this each probe in a burst would queue its own pool checkout ahead of real
 * requests. The timer only bounds the caller; the query itself runs on to
 * completion or the pool's own statement_timeout.
 */
export function createApiDbPoolHealthCheck(
  pool: Pick<Pool, "query">,
  deadlineMs = API_DB_HEALTH_DEADLINE_MS
): () => Promise<void> {
  let inFlight: Promise<unknown> | undefined;
  const clear = () => {
    inFlight = undefined;
  };
  return async () => {
    if (!inFlight) {
      inFlight = pool.query("SELECT 1");
      inFlight.then(clear, clear);
    }
    let timer: NodeJS.Timeout | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`database health check exceeded ${deadlineMs}ms`)), deadlineMs);
    });
    try {
      await Promise.race([inFlight, deadline]);
    } finally {
      clearTimeout(timer);
    }
  };
}

/**
 * pg-pool emits "error" when an IDLE client's connection fails (server
 * restart, network drop). Without a listener that is an unhandled
 * EventEmitter error → uncaughtException → the process exits, even though
 * the pool has already discarded the broken client and the next checkout
 * simply opens a new connection. Log and capture it; keep serving.
 */

export function attachApiDbPoolErrorHandler(
  pool: Pick<Pool, "on">,
  capture: (error: unknown, tags: Record<string, string>) => void,
  log: (message: string, error: unknown) => void = (message, error) => console.error(message, error)
): void {
  pool.on("error", (error) => {
    try {
      log("address API server idle database client error (client discarded, pool still serving):", error);
      capture(error, { source: "pg_pool_idle_client" });
    } catch {
      // Monitoring is best-effort; a failure here must not become a crash.
    }
  });
}
