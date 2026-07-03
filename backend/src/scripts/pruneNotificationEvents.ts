import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";

// Retention for user_candidate_follow_notification_events. Events are
// deduplicated per (user, record) / (user, candidate, election) by unique
// partial indexes, so the table grows with followers x content and nothing
// deletes rows. Until a delivery consumer exists — and after one does — an
// undelivered event older than the retention window is a stale notification
// nobody should receive, so pruning is safe. Deleting a row re-arms the
// ON CONFLICT DO NOTHING dedupe for that pair, which only re-notifies if the
// same record or election fires a new event later; that is intended.

export const DEFAULT_NOTIFICATION_EVENT_RETENTION_DAYS = 90;
export const DEFAULT_NOTIFICATION_EVENT_PRUNE_BATCH_SIZE = 10_000;

export type PruneNotificationEventsOptions = {
  olderThanDays: number;
  live: boolean;
  batchSize: number;
};


export function parsePruneNotificationEventsArgs(argv: readonly string[]): PruneNotificationEventsOptions {
  return {
    live: argv.includes("--live"),
    olderThanDays: readPositiveIntegerFlag(argv, "--older-than-days", DEFAULT_NOTIFICATION_EVENT_RETENTION_DAYS),
    batchSize: readPositiveIntegerFlag(argv, "--batch-size", DEFAULT_NOTIFICATION_EVENT_PRUNE_BATCH_SIZE),
  };
}

export async function pruneNotificationEvents(
  db: Pick<Pool, "query">,
  options: PruneNotificationEventsOptions
): Promise<{ matchedCount: number; deletedCount: number }> {
  if (!options.live) {
    const counted = await db.query<{ matched: string }>(
      `
        SELECT count(*)::text AS matched
        FROM public.user_candidate_follow_notification_events
        WHERE created_at < now() - make_interval(days => $1::int)
      `,
      [options.olderThanDays]
    );
    return { matchedCount: Number(counted.rows[0]?.matched ?? 0), deletedCount: 0 };
  }

  // Batched delete: one giant statement would hold a single long transaction
  // over a seq scan (there is no plain created_at index), stalling vacuum and
  // replication for the first prune of a long-neglected table. Chunks by
  // primary key keep each transaction short; each batch re-evaluates the
  // cutoff, which only moves forward.
  let deletedCount = 0;
  for (;;) {
    const deleted = await db.query(
      `
        DELETE FROM public.user_candidate_follow_notification_events
        WHERE id IN (
          SELECT id
          FROM public.user_candidate_follow_notification_events
          WHERE created_at < now() - make_interval(days => $1::int)
          LIMIT $2::int
        )
      `,
      [options.olderThanDays, options.batchSize]
    );
    const batchDeleted = deleted.rowCount ?? 0;
    deletedCount += batchDeleted;
    if (batchDeleted < options.batchSize) {
      break;
    }
  }
  return { matchedCount: deletedCount, deletedCount };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parsePruneNotificationEventsArgs(process.argv.slice(2));
  const connectionString = process.env.DATABASE_URL?.trim();
  if (!connectionString) {
    throw new Error("DATABASE_URL is required to prune notification events");
  }

  const pool = new Pool({ connectionString });
  try {
    const result = await pruneNotificationEvents(pool, options);
    console.log(
      JSON.stringify(
        {
          dryRun: !options.live,
          olderThanDays: options.olderThanDays,
          matchedCount: result.matchedCount,
          deletedCount: result.deletedCount,
          ...(options.live ? {} : { next: "re-run with --live to delete" }),
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("notification event prune failed:", message);
    process.exitCode = 1;
  });
}
