-- Chatbot question-log retention maintenance as a callable function.
--
-- The privacy policy promises stored question text is deleted after 90 days.
-- Until now retention ran only inside the manual `chatbot:report` script; the
-- API now triggers it daily (Redis-elected timer — no cron on the free plan).
-- The function does BOTH halves of the retention rule in one statement so an
-- automated purge can never destroy text before it is aggregated:
--   1. roll frequent questions up into chatbot.question_stats (write-time
--      suppression: only (week, question_norm) pairs with weekly count >= 5;
--      GREATEST keeps the stored peak as rows age out of the window), then
--   2. null question_norm older than 90 days.
-- Both CTEs read the same snapshot, so the rollup always sees the rows the
-- purge is about to clear.
--
-- SECURITY DEFINER: the API role deliberately cannot read or update
-- chatbot.questions / question_stats (insert-only on questions, see migration
-- 234) and keeps it — voteapp_api may EXECUTE this fixed statement and still
-- cannot touch the log any other way.
CREATE FUNCTION chatbot.roll_up_and_purge_questions()
RETURNS TABLE (stats_rows_written integer, purged_question_texts integer)
LANGUAGE sql
SECURITY DEFINER
-- Pin resolution for the function body: with an empty search_path only the
-- schema-qualified names inside resolve, so a malicious object earlier on the
-- caller's search_path can never be substituted into a definer-rights body.
SET search_path = ''
AS $$
  WITH rolled AS (
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
      HAVING count(*) >= 5
    ) AS frequent
      ON frequent.week = date_trunc('week', q.asked_at)::date
     AND frequent.question_norm = q.question_norm
    WHERE q.question_norm IS NOT NULL
    GROUP BY 1, 2, 3
    ON CONFLICT (week, question_norm, outcome)
      -- GREATEST, not overwrite: once the purge nulls part of a week's rows,
      -- that week recomputes SMALLER from the survivors — a plain overwrite
      -- would permanently shrink the durable aggregate the purge exists to
      -- preserve. Counts only grow while rows live, so the stored peak is the
      -- true weekly count.
      DO UPDATE SET count = GREATEST(chatbot.question_stats.count, EXCLUDED.count)
    RETURNING 1
  ),
  purged AS (
    UPDATE chatbot.questions
    SET question_norm = NULL
    WHERE asked_at < now() - interval '90 days'
      AND question_norm IS NOT NULL
    RETURNING 1
  )
  SELECT
    (SELECT count(*) FROM rolled)::integer AS stats_rows_written,
    (SELECT count(*) FROM purged)::integer AS purged_question_texts;
$$;

-- Functions are EXECUTE-able by PUBLIC by default; this one must be callable
-- only by roles we name (it bypasses the question-log privilege wall).
REVOKE ALL ON FUNCTION chatbot.roll_up_and_purge_questions() FROM PUBLIC;

-- Guarded like migration 234: the API role does not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT EXECUTE ON FUNCTION chatbot.roll_up_and_purge_questions() TO voteapp_api;
  END IF;
END $$;
