BEGIN;

ALTER TABLE public.lacity_candidate_finance_links
  VALIDATE CONSTRAINT lacity_candidate_finance_links_seat_number_check;

COMMIT;
