-- Repair contests that a matcher defect attached to the wrong clerk office.
--
-- "<County> Clerk of the District Court" (Nebraska) and "<County> Clerk of
-- Circuit Court" / "Clerk of Courts" (Wisconsin) elect the clerk of court, a
-- distinct office from the county's own clerk. Both states put the county name
-- first, so the office matcher's jurisdiction strip left "county clerk of ...
-- court": the generic "county clerk" key sat in that verbatim and took the
-- phrase-containment boost, beating the specific "clerk of court" key that the
-- interposed court name had split apart. The mis-match then persisted itself as
-- a learned alias, so later runs returned the wrong office at confidence 1.00
-- and its research areas followed onto the contest.
--
-- The matcher fix ships alongside this migration, but it only governs FUTURE
-- resolutions. Rows already written keep their wrong office_id: the elections
-- upsert only overwrites office_id when a payload is re-injected, and the
-- office-id repair script deliberately revisits only rows where office_id IS
-- NULL. This migration is therefore the repair path for rows already on disk,
-- and it is what carries the fix to production.
--
-- Both statements are idempotent: after a successful run neither predicate
-- matches anything, so a replay is a no-op.

BEGIN;

-- Repoint county contests whose ballot title names a court's clerk but whose
-- office is a clerk office naming no court of its own (County Clerk, County
-- Clerk and Recorder). Mirrors the matcher's own predicates, so the SQL and the
-- code agree on what a court-clerk title is. Nebraska's actual county-clerk
-- title, "Clerk Register of Deeds", names no court and is untouched.
UPDATE public.elections e
SET office_id = target.id,
    updated_at = now()
FROM public.districts d,
     public.offices current_office,
     public.offices target
WHERE d.id = e.district_id
  AND current_office.id = e.office_id
  AND target.scope = 'county'
  AND target.canonical_name = 'Clerk of Court'
  AND d.district_type = 'county'
  AND (
    e.official_ballot_title ~* '\mclerk of (the )?([a-z]+ )?courts?\M'
    OR e.official_ballot_title ~* '\mcourts? clerk\M'
  )
  AND current_office.canonical_name ~* '\mclerk\M'
  AND current_office.canonical_name !~* '\mcourts?\M';

-- Drop the learned aliases that cemented the mis-match. A trigger blocks
-- reassigning an alias's office_id, and the matcher now resolves these titles
-- deterministically without an alias, so deletion is the whole repair.
DELETE FROM public.office_title_aliases a
USING public.offices o
WHERE o.id = a.office_id
  AND a.scope = 'county'
  AND (
    a.normalized_alias ~ '\mclerk of (the )?([a-z]+ )?courts?\M'
    OR a.normalized_alias ~ '\mcourts? clerk\M'
  )
  AND o.canonical_name ~* '\mclerk\M'
  AND o.canonical_name !~* '\mcourts?\M';

COMMIT;
