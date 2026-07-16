BEGIN;

-- Converges public.normalize_finance_label (migration 185) with the
-- TypeScript normalizeFinanceLabel it must stay in lockstep with. The 185
-- body was a verbatim copy of the long-inlined SQL expression, which had
-- known divergences from TypeScript — all silent-miss bugs, since the
-- classification join finds nothing when the two sides disagree:
--
--   * No Unicode folding: TypeScript applies NFKD and strips combining
--     marks U+0300..U+036F ("Café" -> CAFE, "Nguyễn" -> NGUYEN, decomposed
--     input included) while the SQL dropped every non-ASCII letter.
--   * No all-suffix fallback: TypeScript falls back to the unstripped label
--     when suffix-stripping empties it ("LLC" -> LLC) while the SQL
--     returned an empty string.
--
-- The body now mirrors the TypeScript pipeline step for step:
--   normalize(NFKD) -> strip combining marks U+0300..U+036F -> '&' to
--   ' AND ' -> non-alphanumeric runs to a space -> upper() -> collapse and
--   trim -> business-suffix strip with fallback to the unstripped form.
-- Postgres normalize() requires a UTF8 database (all environments here) and
-- is IMMUTABLE PARALLEL SAFE, matching this function's own labels. Because
-- the non-alphanumeric cleanup runs before upper(), upper() only ever sees
-- ASCII, so the result does not depend on the database locale (no ICU
-- upper() turning 'ß' into 'SS').
--
-- Lockstep contract unchanged: change this function only together with
-- tests/pipeline/finance/normalizeFinanceLabelParity.test.ts, which feeds a
-- corpus through both implementations and asserts identical output. The
-- function still implements only the non-occupation TypeScript branch;
-- every evidence query filters to 'donor'/'employer' before joining
-- through it.
CREATE OR REPLACE FUNCTION public.normalize_finance_label(raw_label text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS $$
  WITH cleaned AS (
    SELECT btrim(
      regexp_replace(
        upper(
          regexp_replace(
            replace(
              regexp_replace(normalize(raw_label, NFKD), '[\u0300-\u036f]', '', 'g'),
              '&',
              ' AND '
            ),
            '[^A-Za-z0-9]+',
            ' ',
            'g'
          )
        ),
        '\s+',
        ' ',
        'g'
      )
    ) AS base
  )
  SELECT CASE WHEN stripped.without_suffixes = '' THEN stripped.base ELSE stripped.without_suffixes END
  FROM (
    SELECT
      cleaned.base,
      btrim(
        regexp_replace(
          regexp_replace(
            cleaned.base,
            '\m(INC|INCORPORATED|LLC|L L C|LP|L P|LLP|L L P|LTD|LIMITED|CO|COMPANY|CORP|CORPORATION|PLC)\M',
            ' ',
            'g'
          ),
          '\s+',
          ' ',
          'g'
        )
      ) AS without_suffixes
    FROM cleaned
  ) AS stripped
$$;

COMMIT;
