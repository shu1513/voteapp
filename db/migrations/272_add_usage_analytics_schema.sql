-- First-party usage analytics — docs/plans/usage-analytics.md.
--
-- Own schema, no FKs in either direction: teardown = DROP SCHEMA usage
-- CASCADE. One append-only table; sessions are reconstructed in SQL from
-- their session_start event. Privacy rules the schema enforces by absence:
-- no user id, no email, no address, no district/candidate/election ids —
-- events carry a route id (never a path), a catalog name, and small
-- allowlisted props validated by the API before insert.

CREATE SCHEMA usage;

CREATE TABLE usage.events (
  event_id uuid PRIMARY KEY,               -- client-minted; retries dedupe on it
  session_id uuid NOT NULL,                -- navigation session (30-min idle rotation), not a visitor
  page_view_id uuid,                       -- the page view the event belongs to
  name text NOT NULL,                      -- catalog name (backend/src/usage/events.ts)
  route text NOT NULL,                     -- route id such as 'home' / 'election' / 'not_found'
  received_at timestamptz NOT NULL DEFAULT now(),
  client_offset_ms integer NOT NULL,       -- ms since the client's session start; orders events within a session
  v smallint NOT NULL DEFAULT 1,           -- payload schema version
  props jsonb
);
CREATE INDEX idx_usage_events_received_at ON usage.events (received_at);
CREATE INDEX idx_usage_events_session ON usage.events (session_id, client_offset_ms);

-- Retention: raw events live 90 days by server receipt time. SECURITY
-- DEFINER + empty search_path + REVOKE PUBLIC — the same shape as
-- chatbot.roll_up_and_purge_questions (migration 241): the API role may run
-- exactly this statement and still cannot read or delete the table any
-- other way.
CREATE FUNCTION usage.purge_events()
RETURNS integer
LANGUAGE sql
SECURITY DEFINER
SET search_path = ''
AS $$
  WITH purged AS (
    DELETE FROM usage.events
    WHERE received_at < now() - interval '90 days'
    RETURNING 1
  )
  SELECT count(*)::integer FROM purged;
$$;

REVOKE ALL ON FUNCTION usage.purge_events() FROM PUBLIC;

-- Least privilege for the API role (guarded: the role does not exist in
-- local dev). INSERT only — no SELECT, no UPDATE, no sequence (the primary
-- key is client-minted). Reports run as the owner role.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT USAGE ON SCHEMA usage TO voteapp_api;
    GRANT INSERT ON usage.events TO voteapp_api;
    GRANT EXECUTE ON FUNCTION usage.purge_events() TO voteapp_api;
  END IF;
END $$;
