// npm run chatbot:report — question-log rollup + retention
// (docs/plans/chatbot-rag.md component 9):
//   1. Rolls chatbot.questions up into chatbot.question_stats with WRITE-TIME
//      SUPPRESSION: a (week, question_norm) aggregate is only written when
//      that question's weekly total is >= 5, so rare/unique question text
//      never persists past the purge.
//   2. Deletes question_norm older than 90 days (outcome/latency columns
//      stay for the operational counters).
//   3. Prints the last 7 days: outcome mix, top questions, top refusals —
//      the candidate list for intent promotion and content gaps.

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";

const SUPPRESSION_MIN_WEEKLY_COUNT = 5;

async function main(): Promise<void> {
  loadProjectEnv();
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp",
  });
  try {
    // 1. Rollup with write-time suppression. Re-running is idempotent: counts
    // are recomputed from the raw rows still inside the retention window.
    const rollup = await pool.query(
      `
        INSERT INTO chatbot.question_stats (week, question_norm, outcome, count)
        SELECT
          date_trunc('week', q.asked_at)::date AS week,
          q.question_norm,
          q.answered_by AS outcome,
          count(*) AS count
        FROM chatbot.questions AS q
        JOIN (
          SELECT date_trunc('week', asked_at)::date AS week, question_norm
          FROM chatbot.questions
          WHERE question_norm IS NOT NULL
          GROUP BY 1, 2
          HAVING count(*) >= $1
        ) AS frequent
          ON frequent.week = date_trunc('week', q.asked_at)::date
         AND frequent.question_norm = q.question_norm
        WHERE q.question_norm IS NOT NULL
        GROUP BY 1, 2, 3
        ON CONFLICT (week, question_norm, outcome)
          -- GREATEST, not overwrite: once the 90-day purge nulls part of a
          -- week's rows, that week recomputes SMALLER from the survivors — a
          -- plain overwrite would permanently shrink the durable aggregate
          -- the purge exists to preserve. Counts only grow while rows live,
          -- so the stored peak is the true weekly count.
          DO UPDATE SET count = GREATEST(chatbot.question_stats.count, EXCLUDED.count)
      `,
      [SUPPRESSION_MIN_WEEKLY_COUNT]
    );

    // 2. 90-day purge of raw question text.
    const purge = await pool.query(
      `
        UPDATE chatbot.questions
        SET question_norm = NULL
        WHERE asked_at < now() - interval '90 days'
          AND question_norm IS NOT NULL
      `
    );

    // 3. Last-7-days report.
    const outcomes = await pool.query<{ answered_by: string; count: string; avg_latency_ms: string | null }>(
      `
        SELECT answered_by, count(*) AS count, round(avg(latency_ms)) AS avg_latency_ms
        FROM chatbot.questions
        WHERE asked_at >= now() - interval '7 days'
        GROUP BY answered_by
        ORDER BY count DESC
      `
    );
    const topQuestions = await pool.query<{ question_norm: string; count: string }>(
      `
        SELECT question_norm, count(*) AS count
        FROM chatbot.questions
        WHERE asked_at >= now() - interval '7 days'
          AND question_norm IS NOT NULL
        GROUP BY question_norm
        ORDER BY count DESC, question_norm ASC
        LIMIT 20
      `
    );
    const topRefusals = await pool.query<{ question_norm: string; count: string }>(
      `
        SELECT question_norm, count(*) AS count
        FROM chatbot.questions
        WHERE asked_at >= now() - interval '7 days'
          AND question_norm IS NOT NULL
          AND answered_by IN ('refused', 'refused_policy')
        GROUP BY question_norm
        ORDER BY count DESC, question_norm ASC
        LIMIT 20
      `
    );

    console.log(
      JSON.stringify(
        {
          ok: true,
          stats_rows_written: rollup.rowCount ?? 0,
          purged_question_texts: purge.rowCount ?? 0,
          last_7_days: {
            outcomes: outcomes.rows,
            top_questions: topQuestions.rows,
            top_refusals: topRefusals.rows,
          },
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("chatbot report failed:", error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
