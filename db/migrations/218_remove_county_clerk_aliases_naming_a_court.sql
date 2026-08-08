-- A county's "Clerk of Circuit Court" is the Clerk of Court office, never the
-- County Clerk: they are two separately elected officials, and the catalog
-- already seeds every "clerk of <court>" title onto Clerk of Court.
--
-- The office matcher learned the opposite. Wisconsin's official ballot title is
-- "<County> County Clerk of Circuit Court" (myvote.wi.gov sample ballot, Racine
-- County, Aug 11 2026 partisan primary), and the same shape is live in NC, NE,
-- IN and KS. The jurisdiction strip removes the county's proper noun but keeps
-- the generic civic word, leaving the key "county clerk of circuit court" — so
-- the token scorer, which only ever compared canonical office NAMES and never
-- aliases, saw "County Clerk" (0.79) beat "Clerk of Court" (0.67) and then
-- persisted its own wrong answer as a learned alias. Every later county with
-- that title then matched the wrong office at alias_exact confidence 1.00, and
-- a wrong office_id silently attaches the wrong office_research_areas.
--
-- The matcher fix that ships with this migration stops the scorer from minting
-- these, but it cannot out-rank one that already exists: an alias covering the
-- whole matcher key always wins the exact lookup. The learned rows have to go.
--
-- Only LEARNED rows are in scope. No migration or seed has ever authored a
-- "clerk of <court>" alias onto County Clerk (seedOffices.ts puts these titles
-- on Clerk of Court), and upsertOfficeAlias refuses to remap an existing alias
-- to a different office, so the seed layer cannot reintroduce them. Titles that
-- merely CONTAIN "clerk" without naming a court ("county clerk", "county clerk
-- and recorder", "county clerk register of deeds") are untouched — those are
-- the County Clerk's own ballot titles.
--
-- Deleting is the whole repair: the fixed matcher re-resolves these titles
-- through the catalog's own Clerk of Court aliases and re-learns the key
-- pointing at the right office on the next injection.

BEGIN;

DELETE FROM public.office_title_aliases alias
USING public.offices office
WHERE alias.office_id = office.id
  AND alias.scope = 'county'
  AND office.scope = 'county'
  -- Both county records-clerk offices; a title naming a court belongs to
  -- neither of them.
  AND office.canonical_name IN ('County Clerk', 'County Clerk and Recorder')
  -- normalized_alias is lowercase, single-spaced and free of punctuation, so a
  -- plain word-boundary pattern is enough: "clerk of [the] [<qualifier> ...]
  -- court[s]" anywhere in the key. Matches the six shapes seen live —
  -- "county clerk of circuit court", "county clerk of the circuit court",
  -- "county clerk of superior court", "county clerk of the superior court",
  -- "county clerk of the district court", "county clerk of courts".
  AND alias.normalized_alias ~ '(^| )clerk of (the )?([a-z]+ )*courts?( |$)';

COMMIT;
