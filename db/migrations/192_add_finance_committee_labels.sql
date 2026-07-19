BEGIN;

-- Manually researched one-line descriptions of outside-spending committees
-- ("Streets for All Los Angeles PAC" tells a voter nothing about who is
-- behind the money). Keyed on (source, committee_id): committee ids are only
-- unique within one disclosing agency's namespace, and every outside-group
-- row the ballot lookup serves carries both. committee_name is a snapshot at
-- research time for human readability — the live name on the finance rows
-- stays authoritative. Labels are researched through the manual-research
-- skill (no AI provider calls) and must stay neutral, source-backed
-- descriptions of the committee's interest, never voting advice.
CREATE TABLE public.finance_committee_labels (
    source text NOT NULL,
    committee_id text NOT NULL,
    committee_name text NOT NULL,
    label text NOT NULL,
    source_urls text[] NOT NULL,
    researched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, committee_id),
    CONSTRAINT finance_committee_labels_label_not_blank CHECK (btrim(label) <> ''),
    CONSTRAINT finance_committee_labels_sources_not_empty CHECK (cardinality(source_urls) > 0)
);

COMMIT;
