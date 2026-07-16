-- Round-24 office-catalog batch: every alias below is a live-hit official
-- ballot title (or its jurisdiction/seat-stripped matcher key) that stranded
-- a NULL-office election shell, hard-blocking the candidate-records stage
-- for the linked candidates. Three genuinely new elected offices (Constable,
-- County Surveyor, Soil and Water Conservation District Supervisor) are
-- added; everything else maps a state-specific title onto the existing
-- catalog office that does that job:
--
--   County Supervisor    <- County Council (+ Member of County Council,
--                           Council Member, County Board Chair) — the
--                           catalog's existing county-council mapping
--   County Executive     <- County Chair (Multnomah OR)
--   District Attorney    <- State's Attorney (MD/IL), County Solicitor
--                           General (GA), County Attorney (MN/KY)
--   County Recorder      <- (County) Register of Deeds (NC/TN)
--   Clerk of Court       <- Register of Wills (MD), Register of Probate
--                           forms (MA), County Surrogate (NJ), Tennessee
--                           elected court clerks
--   County Treasurer     <- County Trustee (TN)
--   County Assessor      <- Property Valuation Administrator (KY)
--   County Level Judge   <- Chancellor (TN chancery court)
--   Comptroller          <- Tax Commissioner (ND, statewide)
--   City Council Member  <- City Commission(er) (FL), (City) Board of
--                           Supervisors (Carson City NV), Metro Council
--                           Member (Louisville KY)
--
-- The seed layer (seedOffices.ts + office_research_areas_v1.sql) carries the
-- same offices, aliases, and curated research areas for fresh installs; on a
-- fresh migrations-only database the alias SELECTs insert zero rows by design
-- and the seed layer fills them in afterward (same pattern as migrations 158,
-- 164, and 169).
--
-- Alias insertion does not repair elections written before the alias existed;
-- the manual:elections:repair-office-ids wrapper re-runs the matcher over
-- stranded shells and backfills office_id.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES
  (
    'county',
    'Constable',
    'Serves civil process such as evictions, subpoenas, and court orders, and provides limited law-enforcement support for local justice courts.'
  ),
  (
    'county',
    'County Surveyor',
    'Maintains official land surveys, boundary records, and plats for the county.'
  ),
  (
    'county',
    'Soil and Water Conservation District Supervisor',
    'Directs local soil and water conservation programs, guiding land-use, drainage, and watershed-protection practices.'
  )
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('County Council', 'county council'),
        ('Member of County Council', 'member of county council'),
        ('Council Member', 'council member'),
        ('County Board Chair', 'county board chair')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Supervisor'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('County Chair', 'county chair')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Executive'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('State''s Attorney', 'state s attorney'),
        ('County Solicitor General', 'county solicitor general'),
        ('County Attorney', 'county attorney')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'District Attorney'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Register of Deeds', 'register of deeds'),
        ('County Register of Deeds', 'county register of deeds')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Recorder'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- "Register of Probate County" is the matcher-key residue of
-- "Register of Probate, <Name> County": the jurisdiction strip removes the
-- county's proper-noun core but keeps the generic civic word, leaving it
-- trailing (Middlesex County MA, live).
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Register of Wills', 'register of wills'),
        ('Register of Probate', 'register of probate'),
        ('County Register of Probate', 'county register of probate'),
        ('Register of Probate County', 'register of probate county'),
        ('County Surrogate', 'county surrogate'),
        ('County Circuit Court Clerk', 'county circuit court clerk'),
        ('County Criminal Court Clerk', 'county criminal court clerk'),
        ('County Juvenile Court Clerk', 'county juvenile court clerk'),
        ('County Probate Court Clerk', 'county probate court clerk')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Clerk of Court'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('County Trustee', 'county trustee')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Treasurer'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Property Valuation Administrator', 'property valuation administrator'),
        ('County Property Valuation Administrator', 'county property valuation administrator')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Assessor'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Chancellor', 'chancellor')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Level Judge'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'statewide', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Tax Commissioner', 'tax commissioner')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'statewide'
  AND o.canonical_name = 'Comptroller'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'place', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('City Commission', 'city commission'),
        ('City Commissioner', 'city commissioner'),
        ('City Board of Supervisors', 'city board of supervisors'),
        ('Board of Supervisors', 'board of supervisors'),
        ('Metro Council Member', 'metro council member'),
        -- The jurisdiction strip cannot remove "Louisville" from the title:
        -- the district row is the consolidated government name
        -- ("Louisville/Jefferson County metro government"), whose proper-noun
        -- core never reduces to the bare city word.
        ('Louisville Metro Council Member', 'louisville metro council member')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'place'
  AND o.canonical_name = 'City Council Member'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Self-aliases for the new offices (seed-layer convention), so the exact
-- canonical form matches without relying on the token scorer.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o
JOIN (VALUES
        ('Constable', 'Constable', 'constable'),
        ('County Surveyor', 'County Surveyor', 'county surveyor'),
        (
          'Soil and Water Conservation District Supervisor',
          'Soil and Water Conservation District Supervisor',
          'soil and water conservation district supervisor'
        ),
        (
          'Soil and Water Conservation District Supervisor',
          'County Soil and Water Conservation District Supervisor',
          'county soil and water conservation district supervisor'
        )
     ) AS v(canonical_name, alias_text, normalized_alias)
  ON v.canonical_name = o.canonical_name
WHERE o.scope = 'county'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Research areas for the new offices. Fresh migration-only databases have no
-- research areas yet; the seed layer fills these links after research-area
-- seeding. Existing databases receive the same curated sets immediately.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT office.id, area.id
FROM public.offices office
JOIN public.research_areas area
  ON area.slug = ANY (
    CASE office.canonical_name
      WHEN 'Constable'
        THEN ARRAY['civil_rights', 'housing_affordability', 'public_safety_and_crime_control']
      WHEN 'County Surveyor'
        THEN ARRAY['government_efficiency', 'housing_affordability', 'public_infrastructure']
      WHEN 'Soil and Water Conservation District Supervisor'
        THEN ARRAY['environment_and_public_health', 'government_efficiency', 'public_infrastructure']
    END::text[]
  )
WHERE office.scope = 'county'
  AND office.canonical_name IN (
    'Constable',
    'County Surveyor',
    'Soil and Water Conservation District Supervisor'
  )
ON CONFLICT (office_id, research_area_id) DO NOTHING;

COMMIT;
