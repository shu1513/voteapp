-- Texas County Judges (and Kentucky County Judge/Executives) are the county's
-- chief EXECUTIVE, not judicial officers. The office matcher previously scored
-- the bare title "County Judge" into the judicial "County Level Judge" office
-- and memorized that mapping as an alias, attaching judicial research areas
-- (legal_competence, impartiality) to an executive contest. This migration
-- seeds a proper County Executive office, mirrors County Supervisor's research
-- areas, maps the bare executive titles to it, and removes any learned alias
-- that pointed "county judge" at the judicial office.

BEGIN;

-- Research-area LINKS are owned by the seed layer (db/seeds/
-- office_research_areas_v1.sql), which runs after migrations and carries its
-- own County Executive block. The copy below only covers already-seeded
-- databases so County Executive is usable immediately without re-running
-- seeds; on a fresh migrations-only database it inserts zero rows by design
-- and the seed layer fills the areas afterward. The guard still fails loudly
-- if the County Supervisor office row itself is missing.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM public.offices
    WHERE scope = 'county' AND canonical_name = 'County Supervisor'
  ) THEN
    RAISE EXCEPTION 'migration 145: County Supervisor office not found; cannot seed County Executive research areas';
  END IF;
END
$$;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'county',
  'County Executive',
  'The elected chief executive of a county government, such as a County Executive, or the presiding officer of a county commissioners court, such as a Texas County Judge or Kentucky County Judge/Executive. Responsible for county administration, budgets, and executive leadership; not a judicial office.'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

-- Best-effort copy for already-seeded databases: same set as County Supervisor.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT ce.id, ora.research_area_id
FROM public.offices ce
JOIN public.offices cs
  ON cs.scope = 'county' AND cs.canonical_name = 'County Supervisor'
JOIN public.office_research_areas ora
  ON ora.office_id = cs.id
WHERE ce.scope = 'county'
  AND ce.canonical_name = 'County Executive'
ON CONFLICT DO NOTHING;

-- Remove any learned alias that routed the executive title to the judicial office.
DELETE FROM public.office_title_aliases a
USING public.offices o
WHERE a.office_id = o.id
  AND o.scope = 'county'
  AND o.canonical_name = 'County Level Judge'
  AND a.normalized_alias IN ('county judge', 'county judge executive');

-- Deliberate aliases: the bare "County Judge" ballot title is used only by
-- states where the office is executive (TX, KY). Judicial county-court titles
-- are phrased "Judge of the County Court" / "County Court Judge" and do not
-- collide with these aliases.
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('County Judge', 'county judge'),
        ('County Judge/Executive', 'county judge executive'),
        ('County Executive', 'county executive')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Executive'
ON CONFLICT DO NOTHING;

COMMIT;
