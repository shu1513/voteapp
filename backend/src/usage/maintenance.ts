// Usage-events retention (docs/plans/usage-analytics.md): raw rows are
// deleted after 90 days. Same mechanics as the chatbot question retention
// (chatbot/maintenance.ts) — the API triggers it at boot plus hourly, a
// Redis SET NX elects one real run per UTC day, and a DB failure releases
// the day so the next tick retries. Runs whether or not collection is
// enabled: the promise attaches to rows already stored.

import type { Pool } from "pg";

import { utcDay } from "../chatbot/limits.js";
import type { MaintenanceRedis, RetentionOutcome } from "../chatbot/maintenance.js";

const PURGE_GUARD_TTL_SECONDS = 26 * 3600;

export async function maybeRunUsageRetention(
  db: Pool,
  redis: MaintenanceRedis | null,
  now: Date = new Date()
): Promise<RetentionOutcome> {
  if (!redis) {
    return "skipped_no_redis";
  }
  const guardKey = `usage:purge:${utcDay(now)}`;
  let elected = false;
  try {
    const electionResult = await redis.set(guardKey, "1", { NX: true, EX: PURGE_GUARD_TTL_SECONDS });
    if (electionResult === null) {
      return "skipped_already_ran";
    }
    elected = true;
    const result = await db.query<{ purge_events: number }>(`SELECT usage.purge_events()`);
    console.log(`usage events retention: ${result.rows[0]?.purge_events ?? 0} rows purged`);
    return "ran";
  } catch (error) {
    if (elected) {
      await redis.del(guardKey).catch(() => undefined);
    }
    console.warn("usage events retention failed:", error instanceof Error ? error.message : String(error));
    return "failed";
  }
}
