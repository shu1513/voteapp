BEGIN;

INSERT INTO public.research_areas (slug, name, description)
VALUES
  (
    'legal_competence',
    'Legal Competence',
    'Evaluate legal reasoning, courtroom performance, case handling, rulings, legal writing, and professional command of the law.'
  ),
  (
    'integrity',
    'Integrity',
    'Evaluate honesty, ethical conduct, transparency, accountability, and reliability in public or professional duties.'
  ),
  (
    'impartiality',
    'Impartiality',
    'Evaluate fairness, neutrality, evenhanded treatment, and independence from improper bias or influence.'
  )
ON CONFLICT (slug)
DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  updated_at = now();

WITH target_offices(scope, canonical_name) AS (
  VALUES
    ('statewide', 'State Supreme Court Justice'),
    ('statewide', 'State Court of Appeals Judge'),
    ('county', 'Superior Court Judge'),
    ('county', 'Probate Judge'),
    ('place', 'Municipal Judge')
),
target_areas(slug) AS (
  VALUES
    ('legal_competence'),
    ('integrity'),
    ('impartiality')
)
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT office.id, research_area.id
FROM target_offices target_office
JOIN public.offices office
  ON office.scope = target_office.scope
 AND office.canonical_name = target_office.canonical_name
JOIN target_areas target_area
  ON true
JOIN public.research_areas research_area
  ON research_area.slug = target_area.slug
ON CONFLICT (office_id, research_area_id) DO NOTHING;

COMMIT;
