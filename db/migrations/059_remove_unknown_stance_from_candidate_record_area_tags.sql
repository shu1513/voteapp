BEGIN;

UPDATE public.candidate_record_area_tags
SET stance = 'neutral'
WHERE stance = 'unknown';

ALTER TABLE public.candidate_record_area_tags
DROP CONSTRAINT IF EXISTS chk_candidate_record_area_tags_stance;

ALTER TABLE public.candidate_record_area_tags
ADD CONSTRAINT chk_candidate_record_area_tags_stance
CHECK (stance IN ('for', 'against', 'neutral'));

COMMIT;
