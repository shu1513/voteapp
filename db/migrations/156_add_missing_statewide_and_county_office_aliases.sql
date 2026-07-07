-- Manual 2026 research runs surfaced real ballot titles that match no office,
-- leaving elections with office_id NULL — which hard-blocks the candidate-
-- records stage (writeManualCandidateRecords requires an office for labeling):
--   * Florida "Chief Financial Officer": statewide cabinet office created by
--     merging the elected Comptroller and Treasurer/Insurance Commissioner;
--     mapped to the catalog's Comptroller (state fiscal-officer research areas).
--   * Iowa "Secretary of Agriculture": Iowa's title for the office the catalog
--     calls Commissioner of Agriculture.
--   * Cook County "President of the ... Board of Commissioners": the county's
--     chief executive; catalog office County Executive (see migration 145).
-- The office matcher strips the district's proper-noun prefix before alias
-- lookup, so the county aliases below are keyed without the county name.

BEGIN;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'statewide', v.alias_text, v.normalized_alias
FROM public.offices o
JOIN (VALUES
        ('Comptroller', 'Chief Financial Officer', 'chief financial officer'),
        ('Commissioner of Agriculture', 'Secretary of Agriculture', 'secretary of agriculture')
     ) AS v(canonical_name, alias_text, normalized_alias)
  ON o.canonical_name = v.canonical_name
WHERE o.scope = 'statewide'
ON CONFLICT DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('County Board President', 'county board president'),
        ('President of the County Board of Commissioners', 'president of the county board of commissioners'),
        ('President of the County Board', 'president of the county board')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Executive'
ON CONFLICT DO NOTHING;

COMMIT;
