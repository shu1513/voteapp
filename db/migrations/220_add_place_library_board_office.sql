-- Grand Rapids MI and many Michigan/Midwest cities elect the public library's
-- governing board citywide, on its own ballot heading ("Library Board 6 Year
-- Term" plus a partial-term seat, Kent County Nov 3 2026 live). No place-scope
-- library office existed, so the office matcher returned no match and the
-- elections writer aborted the whole Grand Rapids payload.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'place',
  'Library Board Member',
  E'Setting policy for the public library and its branches\nApproving the library''s budget and how its millage money is spent\nHiring and overseeing the library director\nDeciding library hours, services, and building projects'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'place', v.alias_text, v.normalized_alias
FROM public.offices o
CROSS JOIN (
  VALUES
    ('Library Board', 'library board'),
    ('Public Library Board', 'public library board'),
    ('Library Trustee', 'library trustee'),
    ('Library Board of Trustees', 'library board of trustees'),
    ('Board of Library Trustees', 'board of library trustees')
) AS v(alias_text, normalized_alias)
WHERE o.scope = 'place'
  AND o.canonical_name = 'Library Board Member'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

-- Curated research areas, mirrored byte-for-byte in
-- db/seeds/office_research_areas_v1.sql. Without these the office is the only
-- one in the catalog with no areas, and candidate-record labeling for library
-- races has no allowlist to draw on.
--
-- civil_rights covers the board's most contested lever: challenges to
-- materials and who may access them. data_privacy is the library-specific
-- one — patron borrowing records are confidential by statute in most states.
-- government_efficiency and government_spending_reduction are the operating
-- and levy decisions (Michigan library boards administer their own millage).
-- public_infrastructure is branches and capital projects.
-- public_education_quality covers the literacy, homework-help, and adult-
-- education programming a library board actually sets; it is shared with
-- School Board Member rather than exclusive to it, and omitting it would push
-- those records into neighboring areas.
--
-- No public_safety_and_crime_control: branch security is an administrative
-- matter for the director, not a lever the board is elected on.
--
-- On a fresh migrations-only database research_areas is still empty
-- (DB_DEPLOYMENT.md runs db:seed:research-areas AFTER db:migrate), so this
-- insert matches zero rows by design and the seed layer fills the links
-- afterward — same pattern as migrations 184 and 206.
INSERT INTO public.office_research_areas (office_id, research_area_id)
SELECT o.id, ra.id
FROM public.offices o
JOIN public.research_areas ra
  ON ra.slug = ANY (ARRAY[
       'civil_rights',
       'data_privacy',
       'government_efficiency',
       'government_spending_reduction',
       'public_education_quality',
       'public_infrastructure'
     ]::text[])
WHERE o.scope = 'place'
  AND o.canonical_name = 'Library Board Member'
ON CONFLICT DO NOTHING;

COMMIT;
