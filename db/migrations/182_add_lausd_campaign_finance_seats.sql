BEGIN;

ALTER TABLE public.lacity_candidate_finance_links
  DROP CONSTRAINT lacity_candidate_finance_links_seat_number_check,
  ADD CONSTRAINT lacity_candidate_finance_links_seat_number_check
    CHECK (
      (
        office_name = 'City Council Member'
        AND seat_number IS NOT NULL
        AND seat_number BETWEEN 1 AND 15
      )
      OR
      (
        office_name = 'School Board Member'
        AND seat_number IS NOT NULL
        AND seat_number BETWEEN 1 AND 7
      )
      OR
      (
        office_name NOT IN ('City Council Member', 'School Board Member')
        AND seat_number IS NULL
      )
    ) NOT VALID;

COMMIT;
