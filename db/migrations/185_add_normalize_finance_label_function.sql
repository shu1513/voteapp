BEGIN;

-- Single shared implementation of the finance-label normalization that ~22
-- ballot-lookup evidence queries previously inlined as identical nested
-- regexp_replace expressions. The body below is a verbatim copy of that
-- inline expression: this migration is a pure refactor and must not change
-- query behavior.
--
-- Lockstep contract: finance_label_classifications.normalized_label is
-- written by the TypeScript normalizeFinanceLabel (financeLabelClassifier.ts)
-- and evidence queries recompute the key with this function at read time. If
-- the two ever disagree, the classification join silently matches nothing.
-- tests/pipeline/finance/normalizeFinanceLabelParity.test.ts feeds a corpus
-- through both and asserts identical output — change either side only
-- together with that test.
--
-- This function implements only the non-occupation branch of the TypeScript
-- normalizeFinanceLabel: for label_type 'occupation' TypeScript skips the
-- business-suffix stripping, while this function always strips. Every
-- evidence query filters to 'donor'/'employer' before joining through this
-- function, so occupation labels never reach it — do not point it at
-- occupation-typed labels without adding a label-type parameter first.
--
-- Known pre-existing divergences preserved by this verbatim copy (the parity
-- test documents them; fixing them is a deliberate follow-up, not part of
-- this refactor):
--   * TypeScript folds diacritics via NFKD ("Café" -> CAFE) while SQL drops
--     non-ASCII letters ("Café" -> CAF).
--   * TypeScript falls back to the unstripped label when suffix-stripping
--     empties it ("LLC" -> LLC) while SQL returns an empty string.
CREATE FUNCTION public.normalize_finance_label(raw_label text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
RETURNS NULL ON NULL INPUT
AS $$
  SELECT btrim(
    regexp_replace(
      regexp_replace(
        btrim(
          regexp_replace(
            regexp_replace(
              regexp_replace(upper(replace(raw_label, '&', ' AND ')), '[^A-Z0-9]+', ' ', 'g'),
              '\m(INC|INCORPORATED|LLC|L L C|LP|L P|LLP|L L P|LTD|LIMITED|CO|COMPANY|CORP|CORPORATION|PLC)\M',
              ' ',
              'g'
            ),
            '\s+',
            ' ',
            'g'
          )
        ),
        '\s+',
        ' ',
        'g'
      ),
      '^\s+|\s+$',
      '',
      'g'
    )
  )
$$;

COMMIT;
