// In-API retention maintenance — docs/plans/chatbot-improvements-2026-08.md
// PR 1. The privacy policy promises question text is deleted after 90 days;
// with no cron on the free plan, the purge piggybacks on ask traffic: at most
// one real run per UTC day, elected via Redis SET NX. Fire-and-forget from
// the caller's perspective — a maintenance problem must never delay or fail
// an answer. `chatbot:report` runs the same chatbot.purge_question_text()
// function (as the owner role), so both paths stay one implementation.

import type { Pool } from "pg";

import { utcDay } from "./limits.js";

/** Minimal structural slice of node-redis v4: SET NX returns "OK" when this
 * process won today's election, null when another already did. NX is typed
 * `true` (not boolean) to stay assignable from node-redis's SetOptions. */
export type MaintenanceRedis = {
  set: (key: string, value: string, options?: { NX?: true; EX?: number }) => Promise<string | null>;
};

// Day-scoped key + slack, same GC pattern as the user-cap keys: the day in
// the key is the real boundary, the TTL just cleans up.
const PURGE_GUARD_TTL_SECONDS = 26 * 3600;

export type PurgeOutcome = "skipped_no_redis" | "skipped_already_ran" | "purged" | "failed";

/**
 * Runs the daily question-text purge if nobody has today. Never throws; a
 * Redis or DB failure logs a warning and leaves the day unmarked or marked —
 * either way the manual `chatbot:report` purge still covers retention.
 * Without Redis (chatbot on, but no auth/cache/LLM wiring) it stays skipped:
 * an unguarded purge-per-ask would be harmless but noisy, and every real
 * deployment has Redis.
 */
export async function maybePurgeQuestionText(
  db: Pool,
  redis: MaintenanceRedis | null,
  now: Date = new Date()
): Promise<PurgeOutcome> {
  if (!redis) {
    return "skipped_no_redis";
  }
  try {
    const elected = await redis.set(`chatbot:purge:${utcDay(now)}`, "1", { NX: true, EX: PURGE_GUARD_TTL_SECONDS });
    if (elected === null) {
      return "skipped_already_ran";
    }
    const result = await db.query<{ purged: number }>(`SELECT chatbot.purge_question_text() AS purged`);
    console.log(`chatbot question-text purge: ${result.rows[0]?.purged ?? 0} rows cleared`);
    return "purged";
  } catch (error) {
    console.warn(
      "chatbot question-text purge failed; the report script's purge still covers retention:",
      error instanceof Error ? error.message : String(error)
    );
    return "failed";
  }
}
