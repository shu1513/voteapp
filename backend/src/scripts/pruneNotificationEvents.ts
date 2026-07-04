import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";

// Retention for the notification event tables (candidate-follow and
// district new-election). Events are deduplicated by unique indexes, so the
// tables grow with audience x content and nothing deletes rows. Deleting a
// row re-arms the ON CONFLICT DO NOTHING dedupe for that pair, which only
// re-notifies if the same record or election fires a new event later; that
// is intended.
//
// Candidate-follow events prune by age alone: an unsent record-update or
// ballot digest item older than the window is stale news nobody should
// receive. District new-election events additionally require notified_at —
// a pending alert for a still-future election stays deliverable (e.g. a user
// who verifies their email late), and every row is eventually stamped anyway
// (delivered, or orphan-resolved when the user opts out, moves away, or the
// election date passes), so the lifecycle stays bounded.

export const NOTIFICATION_EVENT_TABLES = [
  { table: "user_candidate_follow_notification_events", requireNotified: false },
  { table: "user_district_notification_events", requireNotified: true },
] as const;

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

async function pruneNotificationEventTable(
  db: Pick<Pool, "query">,
  { table, requireNotified }: (typeof NOTIFICATION_EVENT_TABLES)[number],
  options: PruneNotificationEventsOptions
): Promise<{ matchedCount: number; deletedCount: number }> {
  const ageCondition = `created_at < now() - make_interval(days => $1::int)${
    requireNotified ? " AND notified_at IS NOT NULL" : ""
  }`;
  if (!options.live) {
    const counted = await db.query<{ matched: string }>(
      `
        SELECT count(*)::text AS matched
        FROM public.${table}
        WHERE ${ageCondition}
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
        DELETE FROM public.${table}
        WHERE id IN (
          SELECT id
          FROM public.${table}
          WHERE ${ageCondition}
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

export async function pruneNotificationEvents(
  db: Pick<Pool, "query">,
  options: PruneNotificationEventsOptions
): Promise<{ matchedCount: number; deletedCount: number }> {
  let matchedCount = 0;
  let deletedCount = 0;
  for (const table of NOTIFICATION_EVENT_TABLES) {
    const result = await pruneNotificationEventTable(db, table, options);
    matchedCount += result.matchedCount;
    deletedCount += result.deletedCount;
  }
  return { matchedCount, deletedCount };
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
