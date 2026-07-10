-- Clark County NV's 2026 offices-up list includes an elected Public
-- Administrator (administers estates of decedents with no qualified executor;
-- several states elect this county office). The catalog had no row or alias
-- for it, so the election could not be imported end-to-end: a NULL office_id
-- hard-blocks the candidate-records stage. This migration seeds a county-scope
-- Public Administrator office, mirrors County Treasurer's research areas (the
-- nearest fiduciary asset-handling county office), and maps its ballot titles.

BEGIN;

-- Research-area LINKS are owned by the seed layer (db/seeds/
-- office_research_areas_v1.sql), which carries its own Public Administrator
-- block. The copy below only covers already-seeded databases so the office is
-- usable immediately without re-running seeds; on a fresh migrations-only
-- database County Treasurer does not exist yet (offices are created by
-- elections:offices:seed, which DB_DEPLOYMENT.md runs AFTER db:migrate), the
-- mirror inserts zero rows by design, and the seed layer fills the areas
-- afterward. An exception here would brick every fresh install, so the
-- missing-Treasurer case is a NOTICE, not an error.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM public.offices
    WHERE scope = 'county' AND canonical_name = 'County Treasurer'
  ) THEN
    RAISE NOTICE 'migration 158: County Treasurer office not present (fresh install); Public Administrator research areas will come from the seed layer';
  END IF;
END
$$;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'Public Administrator',
  'An elected county officer who administers the estates of people who die without a will or without a qualified executor, acting as a court-supervised fiduciary over decedent assets. In some counties the same officer also serves as Public Guardian for incapacitated adults.'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

-- Best-effort copy for already-seeded databases: same set as County Treasurer.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT pa.id, ora.research_area_id
FROM public.offices pa
JOIN public.offices ct
  ON ct.scope = 'county' AND ct.canonical_name = 'County Treasurer'
JOIN public.office_research_areas ora
  ON ora.office_id = ct.id
WHERE pa.scope = 'county'
  AND pa.canonical_name = 'Public Administrator'
ON CONFLICT DO NOTHING;

-- Ballot-title aliases. The matcher normalizes titles by lowercasing and
-- stripping punctuation, so "Public Administrator/Public Guardian" normalizes
-- to the slashless form below.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Public Administrator', 'public administrator'),
        ('Public Administrator/Public Guardian', 'public administrator public guardian'),
        ('Public Administrator/Guardian', 'public administrator guardian')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'Public Administrator'
ON CONFLICT DO NOTHING;

COMMIT;
