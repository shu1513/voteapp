-- Michigan links can now come from the MiTN public-search resolver (the only
-- source of post-April-2025 filings). Widen the link_source allowlist.
ALTER TABLE public.mi_candidate_finance_links
  DROP CONSTRAINT IF EXISTS mi_candidate_finance_links_source_check;

ALTER TABLE public.mi_candidate_finance_links
  ADD CONSTRAINT mi_candidate_finance_links_source_check
    CHECK (link_source IN ('manual', 'mitn_legacy', 'mitn_public_search'));
