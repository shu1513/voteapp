-- Georgia's county Solicitor-General is a separate elected office from the
-- District Attorney, not a state-specific name for the same job. The DA
-- prosecutes felonies in superior court for a multi-county judicial circuit;
-- the solicitor-general prosecutes misdemeanors in the county's State Court.
-- Both are elected countywide, in the same cycle, so one county can carry BOTH
-- contests on the same ballot.
--
-- Migration 184 mapped the alias "County Solicitor General" onto District
-- Attorney alongside the genuine DA synonyms (State's Attorney, County
-- Attorney). That collapses the two contests onto one catalog office, and it
-- hands solicitor-general candidates the DA research-area set
-- (corporate_accountability and womens_reproductive_rights cover felony-only
-- conduct the office cannot charge). Found live on "Hall County Solicitor
-- General" (Nov 3 2026); Georgia has 70+ State Courts, so it recurs.
--
-- This migration adds the office, moves the alias to it, and links the curated
-- research areas. The alias MOVE is not optional: seedOffices.ts refuses to
-- remap an alias whose stored office disagrees with the seed source
-- ("Office alias collision ... refused to remap"), so leaving the alias on
-- District Attorney would make db:seed:offices throw on every existing
-- database once the seed source names the new office.
--
-- Existing shells ARE rehomed here. The obvious alternative,
-- manual:elections:repair-office-ids, cannot do it: that script selects and
-- writes `office_id IS NULL` only, so a shell holding a wrong-but-present
-- office_id is invisible to it. Rehoming is therefore this migration's job,
-- and it is safe to do here because the migration knows exactly which rows the
-- moved alias produced — county shells still pointing at District Attorney
-- whose normalized title carries the phrase "solicitor general". South
-- Carolina's bare circuit "Solicitor" is a genuine District Attorney and does
-- not contain that phrase, so it is left alone.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES
  (
    'county',
    'Solicitor General',
    E'Prosecuting misdemeanor cases in the county''s state court, such as DUI, theft, and family-violence charges\nDeciding who gets charged with those crimes and which cases are diverted or dropped\nDeciding plea deals and what sentences to ask for\nRepresenting the state at misdemeanor trials and appeals'
  )
ON CONFLICT (scope, canonical_name) DO NOTHING;

-- Move the Georgia alias off District Attorney. Migration 087 installed
-- trg_prevent_office_title_alias_reassignment precisely to stop an alias from
-- drifting between offices, because a silent re-point silently rehomes every
-- election that matched through it. Splitting one office into two is the case
-- that rule is meant to make deliberate, not the case it is meant to forbid,
-- so the guard is suspended for this one scoped statement and restored before
-- COMMIT — and if anything below fails, the rollback restores it anyway.
-- Delete-and-reinsert would dodge the same trigger without saying so.
ALTER TABLE public.office_title_aliases
  DISABLE TRIGGER trg_prevent_office_title_alias_reassignment;

-- Scoped to the row that is actually mis-pointed, so re-running is a no-op and
-- a database that already has it right is untouched.
UPDATE public.office_title_aliases alias
SET office_id = solicitor.id,
    updated_at = now()
FROM public.offices solicitor,
     public.offices district_attorney
WHERE solicitor.scope = 'county'
  AND solicitor.canonical_name = 'Solicitor General'
  AND district_attorney.scope = 'county'
  AND district_attorney.canonical_name = 'District Attorney'
  AND alias.scope = 'county'
  AND alias.normalized_alias = 'county solicitor general'
  AND alias.office_id = district_attorney.id;

ALTER TABLE public.office_title_aliases
  ENABLE TRIGGER trg_prevent_office_title_alias_reassignment;

-- Belt and braces: a future edit that moves the ENABLE above the UPDATE, or
-- drops it, would leave production without the guard. Fail the migration
-- rather than commit a database whose alias table can drift silently.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'public.office_title_aliases'::regclass
      AND tgname = 'trg_prevent_office_title_alias_reassignment'
      AND tgenabled <> 'D'
  ) THEN
    RAISE EXCEPTION
      'migration 219 would leave trg_prevent_office_title_alias_reassignment disabled';
  END IF;
END
$$;

-- "<X> County Solicitor General" keeps the generic civic word after the
-- jurisdiction strip, which is why the 184 alias carries the leading "county";
-- ballots that title the office without it ("Solicitor General", "Solicitor-
-- General" — punctuation normalizes to a space) reduce to the bare form.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('County Solicitor General', 'county solicitor general'),
        ('Solicitor General', 'solicitor general')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Solicitor General'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Curated core set, copied from db/seeds/office_research_areas_v1.sql so the
-- two layers converge. It is the District Attorney set MINUS the two slugs
-- that track felony-only charging discretion: in Georgia, abortion-law
-- violations (womens_reproductive_rights) and white-collar fraud
-- (corporate_accountability) are felonies the solicitor-general cannot bring.
-- gun_control stays because Georgia's weapons misdemeanors — carrying in a
-- prohibited location, possession by a minor, pointing a firearm at another —
-- are State Court cases.
--
-- On a fresh migrations-only database research_areas is still empty
-- (DB_DEPLOYMENT.md runs db:seed:research-areas AFTER db:migrate), the join
-- below inserts zero rows by design, and the seed layer fills the links
-- afterward. An exception here would brick every fresh install, so the
-- missing-areas case is a NOTICE, not an error (same pattern as migration 206).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.research_areas) THEN
    RAISE NOTICE 'migration 219: research_areas is empty (fresh install); Solicitor General research areas will come from the seed layer';
  END IF;
END
$$;

INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'anti_corruption',
       'civil_rights',
       'gun_control',
       'public_safety_and_crime_control'
     ]::text[])
WHERE o.scope = 'county'
  AND o.canonical_name = 'Solicitor General'
ON CONFLICT DO NOTHING;

-- Rehome the shells the moved alias had already filed under District Attorney.
-- official_ballot_title_key is normalized lowercase with single spaces, so the
-- phrase test is exact rather than fuzzy.
UPDATE public.elections e
SET office_id = solicitor.id,
    updated_at = now()
FROM public.offices solicitor,
     public.offices district_attorney
WHERE solicitor.scope = 'county'
  AND solicitor.canonical_name = 'Solicitor General'
  AND district_attorney.scope = 'county'
  AND district_attorney.canonical_name = 'District Attorney'
  AND e.office_id = district_attorney.id
  AND e.official_ballot_title_key LIKE '%solicitor general%';

COMMIT;
