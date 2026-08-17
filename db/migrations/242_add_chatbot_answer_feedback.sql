-- Chatbot answer feedback (docs/plans/chatbot-improvements-2026-08.md PR 2):
-- anonymous 👍/👎 per answer. A table, not a column on chatbot.questions —
-- the API role is deliberately INSERT-only on the question log, and a vote
-- arriving later would need UPDATE there. No user identifier and no FK to
-- chatbot.questions (question logging is fire-and-forget with no returned id;
-- the token the API mints instead carries answered_by, which is all the
-- report needs). token_nonce is UNIQUE: one vote per issued token, enforced
-- server-side (the widget is one-shot too, but the DB is the boundary).
--
-- No text columns → nothing here falls under the 90-day question-text purge.

CREATE TABLE chatbot.answer_feedback (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  -- Same vocabulary as chatbot.questions.answered_by ('intent:<name>' |
  -- 'retrieval' | 'clarify' | 'refused' | ... | 'llm' | 'cache'): the report
  -- computes downvote rate per answer path.
  answered_by text NOT NULL,
  verdict text NOT NULL CHECK (verdict IN ('up', 'down')),
  token_nonce text NOT NULL UNIQUE
);

CREATE INDEX idx_chatbot_answer_feedback_created_at ON chatbot.answer_feedback (created_at);

-- Least-privilege grant, same pattern as migration 234: the API only ever
-- INSERTs (ON CONFLICT DO NOTHING needs no SELECT); the report reads as the
-- owner role. Guarded because the role does not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT INSERT ON chatbot.answer_feedback TO voteapp_api;
    -- Migration 234's sequence grant predates this table's sequence.
    GRANT USAGE ON SEQUENCE chatbot.answer_feedback_id_seq TO voteapp_api;
  END IF;
END $$;
