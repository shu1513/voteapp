BEGIN;

-- Ledger of in-place candidate_record identity re-keys.
--
-- record_identity_key hashes (description, source_url, event_date), so every
-- sanctioned in-place edit — the plain-language rewrite, the event-date and
-- source-URL repairs, the ingest writer's similar-description update — gives
-- the row a NEW key while the row itself (id, tags, notification history)
-- stays put. Anything that mirrors rows across databases by natural key
-- (research:promote) then sees the edited row as brand new and the mirror's
-- old-key row as an unmatched orphan: the 2026-08-02 promotion duplicated
-- rewritten records on production exactly this way, because the 2026-08-01
-- rewrite left no machine-readable trace connecting old key to new.
--
-- Every writer that re-keys in place records the transition here, in the
-- same transaction as the edit. Promotion follows the chain (old -> ... ->
-- current) to update mirrored rows in place instead of inserting duplicates,
-- and the duplicate cleanup (research:promote:dedupe) uses it to identify
-- stale old-key rows with provenance instead of similarity guesses.
--
-- Rows are append-only history. ON DELETE CASCADE on candidate_id only: a
-- record's deletion does not erase the fact that its keys once transitioned,
-- because a mirror may still hold the old key.
CREATE TABLE public.candidate_record_identity_transitions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id uuid NOT NULL REFERENCES public.candidates(id) ON DELETE CASCADE,
    old_record_identity_key text NOT NULL,
    new_record_identity_key text NOT NULL,
    -- Which writer re-keyed: 'plain_language_rewrite', 'event_date_repair',
    -- 'source_url_repair', 'research_refresh', 'backfill' (historical rows
    -- reconstructed from plain_language_rewrites after the fact).
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- Same edit re-applied (idempotent backfills, re-runs) must not grow the
    -- ledger; a *different* edit of the same old key is a new pair and rides
    -- the chain.
    CONSTRAINT uq_candidate_record_identity_transitions
        UNIQUE (candidate_id, old_record_identity_key, new_record_identity_key),
    CONSTRAINT ck_record_identity_transition_keys_differ
        CHECK (old_record_identity_key <> new_record_identity_key)
);

-- Promotion resolves chains forward: "which new key did this old key become?"
CREATE INDEX idx_record_identity_transitions_old_key
    ON public.candidate_record_identity_transitions (candidate_id, old_record_identity_key);

COMMIT;
