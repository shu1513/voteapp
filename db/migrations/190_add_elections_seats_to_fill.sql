BEGIN;

-- Multi-seat contests ("vote for up to 3" at-large councils and school
-- boards) import as one election row with no way to record how many seats
-- the contest fills, so the ballot UI cannot distinguish a 6-way race for 3
-- seats from a 6-way race for 1. NULL means "not recorded" — NOT "one
-- seat": existing rows were never checked for seat count, so defaulting
-- them to 1 would assert a fact nobody verified. Display treats NULL and 1
-- identically (no instruction shown).
ALTER TABLE public.elections
    ADD COLUMN seats_to_fill integer;

ALTER TABLE public.elections
    ADD CONSTRAINT chk_elections_seats_to_fill
    CHECK (seats_to_fill IS NULL OR seats_to_fill >= 1);

COMMIT;
