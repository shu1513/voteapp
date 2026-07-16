BEGIN;

-- Converges public.normalize_finance_label (migration 185) with the
-- TypeScript normalizeFinanceLabel it must stay in lockstep with. The 185
-- body was a verbatim copy of the long-inlined SQL expression, which had two
-- known divergences from TypeScript — both silent-miss bugs, since the
-- classification join finds nothing when the two sides disagree:
--
--   * No diacritic folding: TypeScript applies NFKD and strips combining
--     marks ("Café" -> CAFE) while the SQL dropped the accented letter
--     ("Café" -> CAF).
--   * No all-suffix fallback: TypeScript falls back to the unstripped label
--     when suffix-stripping empties it ("LLC" -> LLC) while the SQL
--     returned an empty string.
--
-- The translate() pair below is generated from the TypeScript pipeline
-- itself: for every character in Latin-1 Supplement + Latin Extended-A
-- (U+00C0..U+017F), apply NFKD and strip combining marks U+0300..U+036F; a
-- single surviving ASCII letter becomes the mapping, anything else becomes
-- a space (TypeScript's [^A-Za-z0-9] cleanup spaces those characters too).
-- Pre-mapping the whole block also keeps upper() away from non-ASCII input,
-- so the result does not depend on the database locale (e.g. ICU upper()
-- turning 'ß' into 'SS').
--
-- Remaining known divergences, pinned by the parity test: characters whose
-- NFKD form is multi-character (ligatures Ĳ ĳ Ŀ ŀ ŉ, and compatibility
-- forms outside this block such as ﬁ or fullwidth letters) fold to letters
-- in TypeScript but to a space here — translate() is strictly one-to-one
-- and such characters do not occur in campaign-finance labels.
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
        regexp_replace(
          upper(
            translate(
              replace(raw_label, '&', ' AND '),
              'ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿĀāĂăĄąĆćĈĉĊċČčĎďĐđĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħĨĩĪīĬĭĮįİıĲĳĴĵĶķĸĹĺĻļĽľĿŀŁłŃńŅņŇňŉŊŋŌōŎŏŐőŒœŔŕŖŗŘřŚśŜŝŞşŠšŢţŤťŦŧŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽžſ',
              'AAAAAA CEEEEIIII NOOOOO  UUUUY  aaaaaa ceeeeiiii nooooo  uuuuy yAaAaAaCcCcCcCcDd  EeEeEeEeEeGgGgGgGgHh  IiIiIiIiI   JjKk LlLlLl    NnNnNn   OoOoOo  RrRrRrSsSsSsSsTtTt  UuUuUuUuUuUuWwYyYZzZzZzs'
            )
          ),
          '[^A-Z0-9]+',
          ' ',
          'g'
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
