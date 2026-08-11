-- Chatbot ("Ask" feature) storage — docs/plans/chatbot-rag.md.
--
-- Everything lives in its own schema so the feature is removable without
-- touching core tables: teardown = DROP SCHEMA chatbot CASCADE (plus the two
-- extensions if nothing else uses them). No FKs from core tables into this
-- schema, and none from here into core tables (source ids are plain uuid
-- columns — a chunk outliving its source row is harmless because retrieval
-- only reads the active generation and reindexing drops stale chunks).

CREATE EXTENSION IF NOT EXISTS vector;    -- Render offers 0.8.1, local 0.8.2 (verified 2026-08-11)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE SCHEMA chatbot;

-- Snapshot index generations: the indexer builds a full new corpus under a
-- 'building' row, validates it, then atomically flips it to 'active' and the
-- old one to 'retired' in a single transaction. Retrieval and cache keys
-- carry the active generation id, so a flip invalidates everything at once
-- and there is no partial-reindex state.
CREATE TABLE chatbot.index_generations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  status text NOT NULL CHECK (status IN ('building', 'active', 'retired')),
  embedding_model text NOT NULL,          -- 'bge-small-en-v1.5'
  chunker_version int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz
);

-- At most one active generation at a time; the flip transaction relies on it.
CREATE UNIQUE INDEX uq_chatbot_index_generations_active
  ON chatbot.index_generations ((TRUE))
  WHERE status = 'active';

CREATE TABLE chatbot.chunks (
  id bigserial PRIMARY KEY,
  generation_id uuid NOT NULL REFERENCES chatbot.index_generations(id) ON DELETE CASCADE,
  source_type text NOT NULL CHECK (source_type IN
    ('candidate_profile', 'candidate_record', 'finance_summary', 'election', 'ballot_measure')),
  -- Candidate id for candidate_profile/candidate_record/finance_summary
  -- chunks, election id for election/ballot_measure chunks: whatever id the
  -- server needs to build the chunk's public page URL. Plain column, no FK.
  source_id uuid,
  -- 'profile:<candidate_uuid>', 'record:<record_uuid>', 'finance:<candidate_uuid>:<election_uuid>',
  -- 'election:<election_uuid>', 'measure:<measure_uuid>' — many chunks per source.
  chunk_key text NOT NULL,
  election_id uuid,
  district_id uuid,
  state text,
  title text NOT NULL,
  content text NOT NULL,                  -- ~150–350 tokens (bge-small caps at 512; TEI truncates longer)
  evidence_urls jsonb,                    -- upstream source URLs (candidate_records.source_url etc.)
  source_updated_at timestamptz,
  content_hash text NOT NULL,             -- sha256(source data + chunker_version + embedding model)
  content_tsv tsvector GENERATED ALWAYS AS
    (setweight(to_tsvector('english', title), 'A') || setweight(to_tsvector('english', content), 'B')) STORED,
  embedding halfvec(384),
  UNIQUE (generation_id, source_type, chunk_key)
);
-- NO HNSW index — deliberate (plan "Risks"). A shared index across
-- generations is mutated by every generation build (inserts) and retirement
-- (deletes + vacuum), exactly the paths the pgvector 0.8.2–0.8.4 corruption
-- fixes cover, and Render is on 0.8.1. Exact scan is accurate by definition
-- and fast at tens of thousands of chunks with halfvec. Add
-- `CREATE INDEX ... USING hnsw (embedding halfvec_cosine_ops)` only when the
-- corpus outgrows exact scan AND Render ships pgvector >= 0.8.4.
CREATE INDEX idx_chatbot_chunks_content_tsv ON chatbot.chunks USING gin (content_tsv);
CREATE INDEX idx_chatbot_chunks_title_trgm ON chatbot.chunks USING gin (title gin_trgm_ops);
CREATE INDEX idx_chatbot_chunks_generation_id ON chatbot.chunks (generation_id);
CREATE INDEX idx_chatbot_chunks_election_id ON chatbot.chunks (election_id);

-- Anonymous question log. NO user identifier column at all (privacy: nothing
-- links questions to accounts; account deletion is unaffected). question_norm
-- is redacted (emails/phones/street addresses/long digit runs stripped) and
-- normalized BEFORE insert, and is nullable because the report script deletes
-- it after 90 days while keeping the outcome/latency columns.
CREATE TABLE chatbot.questions (
  id bigserial PRIMARY KEY,
  asked_at timestamptz NOT NULL DEFAULT now(),
  question_norm text,
  answered_by text NOT NULL,              -- 'intent:<name>' | 'retrieval' | 'clarify' | 'refused' | 'refused_policy' | 'cache' | 'llm' | 'rate_limited'
  scope_key text,
  matched_chunk_ids bigint[],
  latency_ms int,
  tokens_in int,
  tokens_out int
);
CREATE INDEX idx_chatbot_questions_asked_at ON chatbot.questions (asked_at);

-- Durable weekly aggregates that survive the 90-day purge. The report script
-- only ever writes a (week, question_norm) row when its weekly count is >= 5
-- (write-time suppression), so rare/unique question text never persists.
CREATE TABLE chatbot.question_stats (
  week date NOT NULL,
  question_norm text NOT NULL,
  outcome text NOT NULL,
  count int NOT NULL,
  PRIMARY KEY (week, question_norm, outcome)
);

-- Durable atomic daily token budget for the (Phase 2) LLM path. One row per
-- day; reservations are `INSERT ... ON CONFLICT (day) DO NOTHING` then a
-- conditional UPDATE ... RETURNING, so concurrent requests cannot overshoot
-- and the counter survives restarts (the free Key Value store does not).
CREATE TABLE chatbot.daily_budget (
  day date PRIMARY KEY,
  tokens_reserved bigint NOT NULL DEFAULT 0
);

-- Grants for the least-privilege API role. Verified 2026-08-11:
-- docs/postgres-api-role.md grants and ALTER DEFAULT PRIVILEGES are scoped to
-- schema public ONLY, so a new schema gets nothing automatically — the API
-- role cannot even see `chatbot` without these. Guarded because the role does
-- not exist in local dev.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voteapp_api') THEN
    GRANT USAGE ON SCHEMA chatbot TO voteapp_api;
    -- Least privilege, per table. Deliberately NOT "SELECT ON ALL TABLES" and
    -- no ALTER DEFAULT PRIVILEGES: the API must not read the question log
    -- (privacy: insert-only from the request path) or question_stats, and any
    -- future table gets an explicit grant in its own migration.
    GRANT SELECT ON chatbot.index_generations, chatbot.chunks TO voteapp_api;
    GRANT INSERT ON chatbot.questions TO voteapp_api;
    GRANT SELECT, INSERT, UPDATE ON chatbot.daily_budget TO voteapp_api;
    GRANT USAGE ON ALL SEQUENCES IN SCHEMA chatbot TO voteapp_api;  -- bigserial inserts
    -- Indexer + reporting scripts run as the owner role, not voteapp_api.
  END IF;
END $$;
