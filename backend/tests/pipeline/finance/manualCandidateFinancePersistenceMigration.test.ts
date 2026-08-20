import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const MIGRATION_SQL = readFileSync(
  new URL("../../../../db/migrations/246_add_manual_candidate_finance_filing_ledger.sql", import.meta.url),
  "utf8"
);

describe("manual candidate-finance persistence migration", () => {
  it("keeps the source payload and amendment lineage in a filing ledger", () => {
    expect(MIGRATION_SQL).toContain("CREATE TABLE IF NOT EXISTS public.manual_candidate_finance_filings");
    expect(MIGRATION_SQL).toContain("payload jsonb NOT NULL");
    expect(MIGRATION_SQL).toContain("payload_sha256 text NOT NULL");
    expect(MIGRATION_SQL).toContain("DEFERRABLE INITIALLY DEFERRED");
    expect(MIGRATION_SQL).toContain("manual_candidate_finance_filings_one_amendment_idx");
  });

  it("backs every target with the canonical candidate-election link and preserves null amounts", () => {
    expect(MIGRATION_SQL).toContain("CREATE TABLE IF NOT EXISTS public.manual_candidate_finance_filing_targets");
    expect(MIGRATION_SQL).toContain("REFERENCES public.candidate_elections(candidate_id, election_id)");
    expect(MIGRATION_SQL).toContain("ON UPDATE RESTRICT");
    expect(MIGRATION_SQL).toContain("amount numeric(16,2)");
    expect(MIGRATION_SQL).not.toContain("amount numeric(16,2) NOT NULL");
    expect(MIGRATION_SQL).toContain("relationship IN ('candidate_report', 'support', 'oppose')");
  });
});
