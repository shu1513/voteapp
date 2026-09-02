BEGIN;

-- Reviewed official-date override for roll calls whose source stamps the
-- LEGISLATIVE day instead of the calendar day (an overnight sine-die session:
-- LegiScan holds IL S.B. 3777's House vote as 2026-05-31 while the ILGA
-- record says 6/1/2026 — see
-- backend/evidence/rollcall/legiscan-il-2176/CODE-FINDINGS.md §1).
--
-- vote_date stays what the source file says — every evidence-vs-row check
-- pins to it — and official_vote_date, written only through rollcall:judge
-- with the official record cited in the judgment file, is what the fan-out
-- uses for the records' event_date when set. The approved-row freeze trigger
-- (migration 251) covers the new column automatically, so changing an
-- override goes through pending + re-approve like any other judgment edit.
ALTER TABLE public.legislative_votes
  ADD COLUMN official_vote_date date;

COMMIT;
