// npm run chatbot:report — question-log rollup + retention
// (docs/plans/chatbot-rag.md component 9):
//   1. Rolls chatbot.questions up into chatbot.question_stats with WRITE-TIME
//      SUPPRESSION: a (week, question_norm) aggregate is only written when
//      that question's weekly total is >= 5, so rare/unique question text
//      never persists past the purge.
//   2. Deletes question_norm older than 90 days (outcome/latency columns
//      stay for the operational counters) via chatbot.purge_question_text()
//      — the same function the API triggers daily (maintenance.ts).
//   3. Prints the last 7 days: outcome mix (with latency percentiles), token
//      spend, today's budget use, top questions, top refusals, and the active
//      generation's age — the canary numbers plus the candidate list for
//      intent promotion and content gaps.

import { Pool } from "pg";

import { DEFAULT_CHATBOT_DAILY_TOKEN_BUDGET } from "../chatbot/chatbotConfig.js";
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

    // 2. 90-day purge of raw question text (same function the API triggers
    // once per day; running it here too is an idempotent no-op overlap).
    const purge = await pool.query<{ purged: number }>(`SELECT chatbot.purge_question_text() AS purged`);

    // 3. Last-7-days report.
    const outcomes = await pool.query<{
      answered_by: string;
      count: string;
      avg_latency_ms: string | null;
      p50_latency_ms: string | null;
      p95_latency_ms: string | null;
    }>(
      `
        SELECT
          answered_by,
          count(*) AS count,
          round(avg(latency_ms)) AS avg_latency_ms,
          round(percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)) AS p50_latency_ms,
          round(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)) AS p95_latency_ms
        FROM chatbot.questions
        WHERE asked_at >= now() - interval '7 days'
        GROUP BY answered_by
        ORDER BY count DESC
      `
    );
    const tokens = await pool.query<{ tokens_in: string; tokens_out: string }>(
      `
        SELECT COALESCE(sum(tokens_in), 0) AS tokens_in, COALESCE(sum(tokens_out), 0) AS tokens_out
        FROM chatbot.questions
        WHERE asked_at >= now() - interval '7 days'
      `
    );
    // Same UTC-day boundary as limits.ts utcDay(); the cap mirrors the API's
    // env-or-default resolution (a malformed value falls back rather than
    // failing a read-only report).
    const budget = await pool.query<{ tokens_reserved: string }>(
      `SELECT tokens_reserved FROM chatbot.daily_budget WHERE day = (now() AT TIME ZONE 'utc')::date`
    );
    const budgetCapRaw = process.env.CHATBOT_DAILY_TOKEN_BUDGET?.trim();
    const budgetCap =
      budgetCapRaw && /^\d+$/.test(budgetCapRaw) ? Number(budgetCapRaw) : DEFAULT_CHATBOT_DAILY_TOKEN_BUDGET;
    const generation = await pool.query<{ id: string; activated_at: string; age_days: string }>(
      `
        SELECT
          id::text AS id,
          activated_at::text AS activated_at,
          round(EXTRACT(epoch FROM (now() - activated_at)) / 86400, 1) AS age_days
        FROM chatbot.index_generations
        WHERE status = 'active'
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

    const tokensReservedToday = Number(budget.rows[0]?.tokens_reserved ?? 0);
    console.log(
      JSON.stringify(
        {
          ok: true,
          stats_rows_written: rollup.rowCount ?? 0,
          purged_question_texts: purge.rows[0]?.purged ?? 0,
          active_generation: generation.rows[0] ?? null,
          budget_today: {
            tokens_reserved: tokensReservedToday,
            daily_token_budget: budgetCap,
            remaining: Math.max(0, budgetCap - tokensReservedToday),
          },
          last_7_days: {
            outcomes: outcomes.rows,
            tokens: tokens.rows[0],
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
