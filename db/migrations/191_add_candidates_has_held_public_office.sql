BEGIN;

-- Candidate-record discovery routes its question list on one research-derived
-- fact: has this candidate EVER held public office (current or former)?
-- Nothing persisted that answer — current_office is NULL for former
-- officeholders and is_incumbent-style flags proved stale — so every record
-- sweep re-derived it from scratch, and the 2026-07-15 bulk runs defaulted
-- ~3,100 first-time candidates onto the officeholder question list (no career
-- question was ever asked) before falsely confirming them record-less.
-- NULL means "not yet determined" — NOT "never held office": existing rows
-- were never asked this question, so defaulting them to false would assert a
-- fact nobody verified.
ALTER TABLE public.candidates
    ADD COLUMN has_held_public_office boolean;

COMMIT;
