BEGIN;

DELETE FROM public.office_title_aliases
WHERE scope = 'statewide'
  AND normalized_alias = 'lt govenor';

COMMIT;
