-- Several states elect the statewide K-12 schools chief under a title other
-- than the catalog's canonical "Superintendent of Public Instruction":
-- Georgia's official ballot title is "State School Superintendent" (hit live:
-- the election shell persisted with office_id = NULL, which hard-blocks the
-- candidate-records stage), South Carolina uses "State Superintendent of
-- Education", and Wisconsin/California ballots use the "State Superintendent
-- of Public Instruction" form. This migration maps those official titles to
-- the existing statewide office. The seed layer (seedOffices.ts) carries the
-- same aliases for fresh installs; on a fresh migrations-only database the
-- office does not exist yet, the SELECT inserts zero rows by design, and the
-- seed layer fills them in afterward (same pattern as migration 158).

BEGIN;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'statewide', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('State School Superintendent', 'state school superintendent'),
        ('State Superintendent of Public Instruction', 'state superintendent of public instruction'),
        ('State Superintendent of Education', 'state superintendent of education')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'statewide'
  AND o.canonical_name = 'Superintendent of Public Instruction'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
