BEGIN;

WITH lieutenant_governor_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Lieutenant Governor'
  LIMIT 1
)
INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT id, 'statewide', 'Lieutenant Governor', 'lieutenant governor'
FROM lieutenant_governor_office
ON CONFLICT (scope, normalized_alias)
DO UPDATE SET
  office_id = EXCLUDED.office_id,
  alias_text = EXCLUDED.alias_text,
  updated_at = now();

WITH lieutenant_governor_office AS (
  SELECT id
  FROM public.offices
  WHERE scope = 'statewide'
    AND canonical_name = 'Lieutenant Governor'
  LIMIT 1
)
UPDATE public.elections e
SET office_id = (SELECT id FROM lieutenant_governor_office),
    updated_at = now()
FROM public.districts d
WHERE e.race_type = 'office'
  AND d.id = e.district_id
  AND d.district_type = 'statewide'
  AND e.discovery_contest_family = 'non_judicial_office'
  AND e.official_ballot_title_key = 'lieutenant governor'
  AND e.office_id IN (
    SELECT id
    FROM public.offices
    WHERE scope = 'statewide'
      AND canonical_name = 'Governor'
  );

COMMIT;
