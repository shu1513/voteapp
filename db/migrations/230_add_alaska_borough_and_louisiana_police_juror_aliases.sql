-- PR #625 taught the elections validator that Louisiana parishes and Alaska
-- boroughs/census areas are county-equivalents, so their contests now pass the
-- county-scope check on the first inject. The office catalog had no landing
-- spot for two of the offices those rows carry, so the writer produced
-- office-less shells (matcher method=none) and the roster handoff skipped
-- them — candidate research never queued:
--
--   * "Police Juror District 3" (LA): the Police Jury is the governing body of
--     most Louisiana parishes. The catalog holds only the "Parish Police
--     Juror" alias; the seat strip reduces the live title to the bare "police
--     juror", which matched nothing (probed method=none, 0.0).
--
--   * "Assembly Member, Fairbanks North Star Borough" / "Borough Mayor" (AK):
--     a borough assembly is the borough's legislative body — the analogue of a
--     county board — and a borough mayor is the county-equivalent executive,
--     the same office "County Mayor" already aliases to County Executive.
--     County scope had no assembly or mayor form at all (probed method=none).
--
-- "Assembly Member" is safe at county scope: a state-assembly title on a
-- county row is hard-rejected by the validator in every state with a real
-- state assembly (Alaska's legislature is a House and a Senate), and the
-- county corpus holds zero office-less assembly rows this alias could
-- backfill wrongly. A bare "Mayor" alias is deliberately NOT added — a
-- mis-scoped city mayor row must keep failing to resolve.

BEGIN;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Police Juror', 'police juror'),
        ('Borough Assembly Member', 'borough assembly member'),
        ('Assembly Member', 'assembly member')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Supervisor'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'county', v.alias_text, v.normalized_alias
FROM public.offices o,
     (VALUES
        ('Borough Mayor', 'borough mayor')
     ) AS v(alias_text, normalized_alias)
WHERE o.scope = 'county'
  AND o.canonical_name = 'County Executive'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
