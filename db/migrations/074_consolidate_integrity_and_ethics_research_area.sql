BEGIN;

DO $$
DECLARE
  target_id uuid;
  legal_ethics_id uuid;
  integrity_id uuid;
  old_ids uuid[] := ARRAY[]::uuid[];
BEGIN
  INSERT INTO public.research_areas (slug, name, description)
  VALUES (
    'integrity_and_ethics',
    'Integrity and Ethics',
    'Documented criminal convictions, official ethics findings, sanctions, disciplinary actions, court judgments, enforcement actions, or other verified public accountability records.'
  )
  ON CONFLICT (slug)
  DO UPDATE SET
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    updated_at = now()
  RETURNING id INTO target_id;

  SELECT id INTO legal_ethics_id
  FROM public.research_areas
  WHERE slug = 'legal_and_ethics_record';

  SELECT id INTO integrity_id
  FROM public.research_areas
  WHERE slug = 'integrity';

  IF legal_ethics_id IS NOT NULL THEN
    old_ids := array_append(old_ids, legal_ethics_id);
  END IF;

  IF integrity_id IS NOT NULL THEN
    old_ids := array_append(old_ids, integrity_id);
  END IF;

  IF COALESCE(array_length(old_ids, 1), 0) > 0 THEN
    INSERT INTO public.candidate_record_area_tags (
      candidate_record_id,
      research_area_id,
      stance
    )
    SELECT DISTINCT
      tag.candidate_record_id,
      target_id,
      NULL::text
    FROM public.candidate_record_area_tags tag
    WHERE tag.research_area_id = ANY (old_ids)
    ON CONFLICT (candidate_record_id, research_area_id)
    DO UPDATE SET
      stance = NULL,
      updated_at = now();

    DELETE FROM public.candidate_record_area_tags
    WHERE research_area_id = ANY (old_ids);

    DELETE FROM public.office_research_areas
    WHERE research_area_id = ANY (old_ids);

    DELETE FROM public.research_areas
    WHERE id = ANY (old_ids);
  END IF;
END
$$;

COMMIT;
