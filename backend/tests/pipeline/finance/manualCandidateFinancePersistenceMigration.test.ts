import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const LEDGER_MIGRATION_SQL = readFileSync(
  new URL("../../../../db/migrations/246_add_manual_candidate_finance_filing_ledger.sql", import.meta.url),
  "utf8"
);
const UPDATE_POLICY_MIGRATION_SQL = readFileSync(
  new URL("../../../../db/migrations/247_restrict_manual_candidate_finance_target_updates.sql", import.meta.url),
  "utf8"
);

describe("manual candidate-finance persistence migration", () => {
  it("keeps the source payload and amendment lineage in a filing ledger", () => {
    expect(LEDGER_MIGRATION_SQL).toContain("CREATE TABLE IF NOT EXISTS public.manual_candidate_finance_filings");
    expect(LEDGER_MIGRATION_SQL).toContain("payload jsonb NOT NULL");
    expect(LEDGER_MIGRATION_SQL).toContain("payload_sha256 text NOT NULL");
    expect(LEDGER_MIGRATION_SQL).toContain("DEFERRABLE INITIALLY DEFERRED");
    expect(LEDGER_MIGRATION_SQL).toContain("manual_candidate_finance_filings_one_amendment_idx");
  });

  it("backs every target with the canonical candidate-election link and preserves null amounts", () => {
    expect(LEDGER_MIGRATION_SQL).toContain("CREATE TABLE IF NOT EXISTS public.manual_candidate_finance_filing_targets");
    expect(LEDGER_MIGRATION_SQL).toContain("REFERENCES public.candidate_elections(candidate_id, election_id)");
    expect(LEDGER_MIGRATION_SQL).toContain("amount numeric(16,2)");
    expect(LEDGER_MIGRATION_SQL).not.toContain("amount numeric(16,2) NOT NULL");
    expect(LEDGER_MIGRATION_SQL).toContain("relationship IN ('candidate_report', 'support', 'oppose')");
  });

  it("adds the explicit target update policy in a follow-up migration", () => {
    expect(UPDATE_POLICY_MIGRATION_SQL).toContain(
      "DROP CONSTRAINT manual_candidate_finance_filing_targets_candidate_election_fk"
    );
    expect(UPDATE_POLICY_MIGRATION_SQL).toContain("ON UPDATE RESTRICT");
    expect(UPDATE_POLICY_MIGRATION_SQL).toContain("ON DELETE RESTRICT");
  });
});
