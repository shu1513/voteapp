-- Grand Rapids MI and many Michigan/Midwest cities elect the public library's
-- governing board citywide, on its own ballot heading ("Library Board 6 Year
-- Term" plus a partial-term seat, Kent County Nov 3 2026 live). No place-scope
-- library office existed, so the office matcher returned no match and the
-- elections writer aborted the whole Grand Rapids payload.

BEGIN;

INSERT INTO public.offices (scope, canonical_name, summary)
VALUES (
  'place',
  'Library Board Member',
  E'Setting policy for the public library and its branches\nApproving the library''s budget and how its millage money is spent\nHiring and overseeing the library director\nDeciding library hours, services, and building projects'
)
ON CONFLICT (scope, canonical_name) DO NOTHING;

INSERT INTO public.office_title_aliases (office_id, scope, alias_text, normalized_alias)
SELECT o.id, 'place', v.alias_text, v.normalized_alias
FROM public.offices o
CROSS JOIN (
  VALUES
    ('Library Board', 'library board'),
    ('Public Library Board', 'public library board'),
    ('Library Trustee', 'library trustee'),
    ('Library Board of Trustees', 'library board of trustees'),
    ('Board of Library Trustees', 'board of library trustees')
) AS v(alias_text, normalized_alias)
WHERE o.scope = 'place'
  AND o.canonical_name = 'Library Board Member'
ON CONFLICT (scope, normalized_alias) DO NOTHING;

COMMIT;
