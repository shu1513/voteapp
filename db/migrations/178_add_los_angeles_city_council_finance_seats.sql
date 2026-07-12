BEGIN;

ALTER TABLE public.lacity_candidate_finance_links
  ADD COLUMN seat_number smallint,
  ADD CONSTRAINT lacity_candidate_finance_links_seat_number_check
    CHECK (
      (
        office_name = 'City Council Member'
        AND seat_number IS NOT NULL
        AND seat_number BETWEEN 1 AND 15
      )
      OR
      (office_name <> 'City Council Member' AND seat_number IS NULL)
    ) NOT VALID;

COMMIT;
