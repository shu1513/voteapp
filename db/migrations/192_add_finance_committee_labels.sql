BEGIN;

-- Manually researched one-line descriptions of outside-spending committees
-- ("Streets for All Los Angeles PAC" tells a voter nothing about who is
-- behind the money). Keyed on (source, committee_id, cycle): committee ids
-- are only unique within one disclosing agency's namespace, and labels may
-- carry funder claims ("funded primarily by real-estate developers") that
-- can change between cycles — a label researched against one cycle's
-- disclosures must not be asserted on another cycle's summaries, and a new
-- cycle re-queues the committee for research instead of inheriting stale
-- claims. committee_name is a snapshot at research time for human
-- readability — the live name on the finance rows stays authoritative.
-- Labels are researched through the manual-research skill (no AI provider
-- calls) and must stay neutral, source-backed descriptions of the
-- committee's interest, never voting advice.
--
-- source_urls format (http/https) is deliberately validated only in the
-- application layer: the manual writer (manualFinanceCommitteeLabels.ts) is
-- the only sanctioned write path, and Postgres CHECKs cannot express a
-- per-element pattern over an array without a helper function. Direct SQL
-- writes bypass that validation and are out of contract.
CREATE TABLE public.finance_committee_labels (
    source text NOT NULL,
    committee_id text NOT NULL,
    cycle integer NOT NULL,
    committee_name text NOT NULL,
    label text NOT NULL,
    source_urls text[] NOT NULL,
    researched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, committee_id, cycle),
    CONSTRAINT finance_committee_labels_label_not_blank CHECK (btrim(label) <> ''),
    CONSTRAINT finance_committee_labels_sources_not_empty CHECK (cardinality(source_urls) > 0)
);

COMMIT;
