-- Florida's county-level felony prosecutor is titled "State Attorney", elected
-- by judicial circuit ("State Attorney, 4th Judicial Circuit" covers Duval,
-- Clay and Nassau counties). It is the same office the catalog calls District
-- Attorney: it makes charging decisions and prosecutes crimes on behalf of the
-- public, and it is elected, not appointed.
--
-- The catalog already carries the neighboring state-specific names for that job
-- ("Prosecuting Attorney", "County Prosecutor", "County Attorney" in migration
-- 077, and the Maryland/Illinois possessive "State's Attorney" -> "state s
-- attorney" in migration 184), but not Florida's non-possessive form. The key
-- "state attorney" shares only the token "attorney" with "District Attorney"
-- and scores 0.500, under the 0.56 confidence floor, so fuzzy matching cannot
-- reach the office on its own: the name has to be in this table.
--
-- The alias is the catalog half of the fix. The ballot prints the circuit, and
-- stripping that numbered circuit as a seat designator is the matcher half,
-- landed separately in PR #598; with both in place the full ballot title
-- resolves alias_exact. Nothing needs backfilling: no stranded Florida
-- State Attorney election exists to repair (confirmed live, zero rows), because
-- until #598 the elections writer aborted the payload on an unresolved office
-- rather than persisting an office-less row.
--
-- "state s attorney" is a distinct normalized key, so this adds a row rather
-- than moving one; Florida's separately elected Public Defender already matches
-- through its own alias and is untouched.

BEGIN;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('State Attorney', 'state attorney')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'District Attorney'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
