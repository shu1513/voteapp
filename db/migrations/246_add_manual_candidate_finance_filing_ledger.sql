BEGIN;

CREATE TABLE IF NOT EXISTS public.manual_candidate_finance_filings (
  filing_id uuid PRIMARY KEY,
  schema_version text NOT NULL,
  state text NOT NULL,
  filing_type text NOT NULL,
  amends_filing_id uuid,
  report_date date NOT NULL,
  source_url text NOT NULL,
  coverage_note text NOT NULL,
  researched_at timestamptz NOT NULL,
  payload jsonb NOT NULL,
  payload_sha256 text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT manual_candidate_finance_filings_schema_check
    CHECK (schema_version = 'manual_candidate_finance.v1'),
  CONSTRAINT manual_candidate_finance_filings_state_check
    CHECK (state = 'MS'),
  CONSTRAINT manual_candidate_finance_filings_type_check
    CHECK (filing_type IN ('candidate_report', 'independent_expenditure')),
  CONSTRAINT manual_candidate_finance_filings_amendment_check
    CHECK (amends_filing_id IS NULL OR amends_filing_id <> filing_id),
  CONSTRAINT manual_candidate_finance_filings_source_url_check
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT manual_candidate_finance_filings_coverage_note_check
    CHECK (btrim(coverage_note) <> ''),
  CONSTRAINT manual_candidate_finance_filings_payload_hash_check
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT manual_candidate_finance_filings_payload_check
    CHECK (
      jsonb_typeof(payload) = 'object'
      AND payload ->> 'schema_version' IS NOT DISTINCT FROM schema_version
      AND payload ->> 'state' IS NOT DISTINCT FROM state
      AND payload ->> 'filing_type' IS NOT DISTINCT FROM filing_type
      AND payload ->> 'filing_id' IS NOT DISTINCT FROM filing_id::text
      AND payload ->> 'report_date' IS NOT DISTINCT FROM report_date::text
      AND payload ->> 'source_url' IS NOT DISTINCT FROM source_url
      AND payload ->> 'coverage_note' IS NOT DISTINCT FROM coverage_note
      AND (payload ->> 'researched_at')::timestamptz IS NOT DISTINCT FROM researched_at
      AND payload ? 'amends_filing_id'
      AND (
        (amends_filing_id IS NULL AND payload -> 'amends_filing_id' = 'null'::jsonb)
        OR payload ->> 'amends_filing_id' IS NOT DISTINCT FROM amends_filing_id::text
      )
    ),
  CONSTRAINT manual_candidate_finance_filings_amends_fk
    FOREIGN KEY (amends_filing_id)
    REFERENCES public.manual_candidate_finance_filings(filing_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX IF NOT EXISTS manual_candidate_finance_filings_one_amendment_idx
  ON public.manual_candidate_finance_filings (amends_filing_id)
  WHERE amends_filing_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS manual_candidate_finance_filings_report_date_idx
  ON public.manual_candidate_finance_filings (report_date DESC, filing_id);

CREATE TABLE IF NOT EXISTS public.manual_candidate_finance_filing_targets (
  filing_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  election_id uuid NOT NULL,
  candidate_name text NOT NULL,
  relationship text NOT NULL,
  amount numeric(16,2),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT manual_candidate_finance_filing_targets_pk
    PRIMARY KEY (filing_id, candidate_id, election_id, relationship),
  CONSTRAINT manual_candidate_finance_filing_targets_filing_fk
    FOREIGN KEY (filing_id)
    REFERENCES public.manual_candidate_finance_filings(filing_id)
    ON DELETE CASCADE,
  CONSTRAINT manual_candidate_finance_filing_targets_candidate_election_fk
    FOREIGN KEY (candidate_id, election_id)
    REFERENCES public.candidate_elections(candidate_id, election_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT,
  CONSTRAINT manual_candidate_finance_filing_targets_name_check
    CHECK (btrim(candidate_name) <> ''),
  CONSTRAINT manual_candidate_finance_filing_targets_relationship_check
    CHECK (relationship IN ('candidate_report', 'support', 'oppose')),
  CONSTRAINT manual_candidate_finance_filing_targets_amount_check
    CHECK (
      amount IS NULL OR amount >= 0
    ),
  CONSTRAINT manual_candidate_finance_targets_report_amount_check
    CHECK (relationship <> 'candidate_report' OR amount IS NULL)
);

CREATE INDEX IF NOT EXISTS manual_candidate_finance_filing_targets_lookup_idx
  ON public.manual_candidate_finance_filing_targets (candidate_id, election_id, filing_id)
  INCLUDE (relationship, amount);

COMMIT;
