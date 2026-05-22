BEGIN;

ALTER TABLE public.candidates
ADD COLUMN IF NOT EXISTS display_name text;

UPDATE public.candidates
SET display_name = trim(
  regexp_replace(
    concat_ws(' ', first_name, last_name),
    '\s+',
    ' ',
    'g'
  )
)
WHERE display_name IS NULL;

COMMIT;
