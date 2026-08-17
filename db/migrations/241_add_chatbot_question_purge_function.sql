-- Chatbot question-text retention purge as a callable function.
--
-- The privacy policy promises stored question text is deleted after 90 days.
-- Until now the purge only ran inside the manual `chatbot:report` script; the
-- API now also triggers it (once per UTC day, piggybacked on ask traffic —
-- no cron exists on the free plan). The API role deliberately cannot read or
-- update chatbot.questions (insert-only, see migration 234), so the purge is
-- SECURITY DEFINER: voteapp_api may EXECUTE the fixed statement but still
-- cannot see or touch the log any other way.
CREATE FUNCTION chatbot.purge_question_text() RETURNS integer
LANGUAGE sql
SECURITY DEFINER
-- Pin resolution for the function body: with an empty search_path only the
-- schema-qualified names inside resolve, so a malicious object earlier on the
-- caller's search_path can never be substituted into a definer-rights body.
SET search_path = ''
AS $$
  WITH purged AS (
    UPDATE chatbot.questions
    SET question_norm = NULL
    WHERE asked_at < now() - interval '90 days'
      AND question_norm IS NOT NULL
    RETURNING 1
  )
  SELECT count(*)::integer FROM purged;
$$;

-- Functions are EXECUTE-able by PUBLIC by default; this one must be callable
-- only by roles we name (it bypasses the question-log privilege wall).
REVOKE ALL ON FUNCTION chatbot.purge_question_text() FROM PUBLIC;

-- Guarded like migration 234: the API role does not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT EXECUTE ON FUNCTION chatbot.purge_question_text() TO voteapp_api;
  END IF;
END $$;
