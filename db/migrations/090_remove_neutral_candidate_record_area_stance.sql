BEGIN;

UPDATE public.candidate_record_area_tags tag
SET stance = NULL
FROM public.research_areas area
WHERE area.id = tag.research_area_id
  AND area.slug IN ('general', 'integrity_and_ethics')
  AND tag.stance = 'neutral';

DELETE FROM public.candidate_record_area_tags tag
USING public.research_areas area
WHERE area.id = tag.research_area_id
  AND area.slug NOT IN ('general', 'integrity_and_ethics')
  AND tag.stance = 'neutral';

ALTER TABLE public.candidate_record_area_tags
DROP CONSTRAINT IF EXISTS chk_candidate_record_area_tags_stance;

ALTER TABLE public.candidate_record_area_tags
ADD CONSTRAINT chk_candidate_record_area_tags_stance
CHECK (stance IS NULL OR stance IN ('for', 'against'));

COMMIT;
