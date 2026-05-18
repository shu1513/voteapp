BEGIN;

-- Correct likely false positives from migration 026's substring-based pattern.
-- Keep scope narrow: only rows currently marked ballot_measure that do not
-- contain any ballot-measure keyword as a standalone word.
UPDATE elections
SET race_type = 'office'
WHERE race_type = 'ballot_measure'
  AND official_ballot_title !~* '\m(proposition|measure|amendment|referendum|initiative|bond|question)\M';

COMMIT;
