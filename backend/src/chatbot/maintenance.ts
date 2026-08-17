// In-API retention maintenance — docs/plans/chatbot-improvements-2026-08.md
// PR 1. The privacy policy promises question text is deleted after 90 days;
// with no cron on the free plan, the API triggers retention itself (boot +
// hourly timer in runAddressApiServer): at most one real run per UTC day,
// elected via Redis SET NX. Fire-and-forget from the caller's perspective —
// a maintenance problem must never delay or fail an answer.
//
// The single SQL function chatbot.roll_up_and_purge_questions() aggregates
// frequent questions into durable stats BEFORE nulling old text (same
// statement, same snapshot), so the automated purge can never destroy text
// that was not yet aggregated. `chatbot:report` calls the same function.

import type { Pool } from "pg";

import { utcDay } from "./limits.js";

/** Minimal structural slice of node-redis v4: SET NX returns "OK" when this
 * process won today's election, null when another already did. NX is typed
 * `true` (not boolean) to stay assignable from node-redis's SetOptions. */
export type MaintenanceRedis = {
  set: (key: string, value: string, options?: { NX?: true; EX?: number }) => Promise<string | null>;
  del: (key: string) => Promise<unknown>;
};

// Day-scoped key + slack, same GC pattern as the user-cap keys: the day in
// the key is the real boundary, the TTL just cleans up.
const PURGE_GUARD_TTL_SECONDS = 26 * 3600;

export type RetentionOutcome = "skipped_no_redis" | "skipped_already_ran" | "ran" | "failed";

/**
 * Runs the daily question-log retention (rollup + 90-day purge) if nobody
 * has today. Never throws. A DB failure AFTER winning the election releases
 * the day's guard key (best effort) so the next hourly tick can retry —
 * without that, one transient failure would skip retention for the whole
 * UTC day. Without Redis (chatbot on, but no auth/cache/LLM wiring) it stays
 * skipped: every real deployment has Redis, and the manual `chatbot:report`
 * run still covers retention.
 */
export async function maybeRunQuestionRetention(
  db: Pool,
  redis: MaintenanceRedis | null,
  now: Date = new Date()
): Promise<RetentionOutcome> {
  if (!redis) {
    return "skipped_no_redis";
  }
  const guardKey = `chatbot:purge:${utcDay(now)}`;
  let elected = false;
  try {
    const electionResult = await redis.set(guardKey, "1", { NX: true, EX: PURGE_GUARD_TTL_SECONDS });
    if (electionResult === null) {
      return "skipped_already_ran";
    }
    elected = true;
    const result = await db.query<{ stats_rows_written: number; purged_question_texts: number }>(
      `SELECT stats_rows_written, purged_question_texts FROM chatbot.roll_up_and_purge_questions()`
    );
    const row = result.rows[0];
    console.log(
      `chatbot question retention: ${row?.stats_rows_written ?? 0} stat rows written, ${row?.purged_question_texts ?? 0} question texts purged`
    );
    return "ran";
  } catch (error) {
    if (elected) {
      // Give the day back so a later tick can retry; if Redis is the thing
      // that is down, the guard's TTL is the fallback boundary.
      await redis.del(guardKey).catch(() => undefined);
    }
    console.warn(
      "chatbot question retention failed; the report script's run still covers it:",
      error instanceof Error ? error.message : String(error)
    );
    return "failed";
  }
}
