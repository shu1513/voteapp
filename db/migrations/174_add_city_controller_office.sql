-- Houston elects a City Controller, but the place-office catalog previously
-- had only City Treasurer. They are not equivalent: Houston's Controller is
-- the independently elected chief financial officer and auditor. Add the
-- proper office so future Houston elections can link to finance safely.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'place',
  'City Controller',
  'Oversees municipal accounting, financial reporting, payments, investments, debt administration, and independent audits where the office is elected.'
)
ON CONFLICT (scope, canonical_name) DO UPDATE SET
  summary = EXCLUDED.summary,
  updated_at = now();

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT office.id, 'place', alias.alias_text, alias.normalized_alias
FROM public.offices office
CROSS JOIN (VALUES
  ('Controller', 'controller'),
  ('City Controller', 'city controller'),
  ('Municipal Controller', 'municipal controller')
) AS alias(alias_text, normalized_alias)
WHERE office.scope = 'place'
  AND office.canonical_name = 'City Controller'
ON CONFLICT (scope, normalized_alias) DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

UPDATE public.elections election
SET office_id = office.id,
    updated_at = now()
FROM public.offices office
JOIN public.districts district ON district.district_type = 'place'
WHERE election.district_id = district.id
  AND election.office_id IS NULL
  AND office.scope = 'place'
  AND office.canonical_name = 'City Controller'
  AND lower(trim(regexp_replace(election.official_ballot_title, '[^a-zA-Z0-9]+', ' ', 'g')))
      IN ('controller', 'city controller', 'municipal controller');

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
  AND office.canonical_name = 'City Controller'
ON CONFLICT DO NOTHING;

COMMIT;

