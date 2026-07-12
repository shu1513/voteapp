-- Los Angeles City Controller campaign-finance support needs a place-scoped
-- controller office. The existing Comptroller is statewide and must not be
-- reused for a municipal contest. Seed files carry the same office, aliases,
-- and curated research areas for fresh installs.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'place',
  'Municipal Controller',
  'Oversees municipal accounting, audits, financial reporting, and fiscal controls for city government.'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (
  office_id,
  scope,
  alias_text,
  normalized_alias
)
SELECT office.id, 'place', alias.alias_text, alias.normalized_alias
FROM public.offices office
CROSS JOIN (
  VALUES
    ('Municipal Controller', 'municipal controller'),
    ('City Controller', 'city controller')
) AS alias(alias_text, normalized_alias)
WHERE office.scope = 'place'
  AND office.canonical_name = 'Municipal Controller'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Alias insertion does not repair elections written before the alias existed.
-- Backfill only exact place-office titles whose office is still unresolved.
UPDATE public.elections election
SET office_id = office.id,
    updated_at = now()
FROM public.offices office,
     public.districts district
WHERE election.office_id IS NULL
  AND election.race_type = 'office'
  AND district.id = election.district_id
  AND district.district_type = 'place'
  AND office.scope = 'place'
  AND office.canonical_name = 'Municipal Controller'
  AND election.official_ballot_title_key IN (
    'municipal controller',
    'city controller'
  );

-- Fresh migration-only databases have no research areas yet; the seed layer
-- fills these links after research-area seeding. Existing databases receive
-- the same curated set immediately.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT office.id, area.id
FROM public.offices office
JOIN public.research_areas area
  ON area.slug = ANY (ARRAY[
    'anti_corruption',
    'corporate_accountability',
    'government_efficiency',
    'government_spending_reduction'
  ]::text[])
WHERE office.scope = 'place'
  AND office.canonical_name = 'Municipal Controller'
ON CONFLICT (office_id, research_area_id) DO NOTHING;

COMMIT;
