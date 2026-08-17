// npm run chatbot:report — question-log retention + canary report
// (docs/plans/chatbot-rag.md component 9):
//   1. Retention: chatbot.roll_up_and_purge_questions() — the SAME function
//      the API triggers daily (maintenance.ts). One statement does the
//      write-time-suppressed rollup into chatbot.question_stats (weekly
//      count >= 5 only) and THEN nulls question_norm older than 90 days, so
//      text is always aggregated before it is purged. Idempotent re-run.
//   2. Prints the last 7 days: outcome mix (with latency percentiles), token
//      spend, today's budget use, top questions, top refusals, and the active
//      generation's age — the canary numbers plus the candidate list for
//      intent promotion and content gaps.

import { Pool } from "pg";

import { DEFAULT_CHATBOT_DAILY_TOKEN_BUDGET } from "../chatbot/chatbotConfig.js";
import { loadProjectEnv } from "../config/env.js";

async function main(): Promise<void> {
  loadProjectEnv();
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp",
  });
  try {
    // 1. Retention (rollup then purge, one snapshot — migration 241).
    const retention = await pool.query<{ stats_rows_written: number; purged_question_texts: number }>(
      `SELECT stats_rows_written, purged_question_texts FROM chatbot.roll_up_and_purge_questions()`
    );

    // 2. Last-7-days report.
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
          stats_rows_written: retention.rows[0]?.stats_rows_written ?? 0,
          purged_question_texts: retention.rows[0]?.purged_question_texts ?? 0,
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
