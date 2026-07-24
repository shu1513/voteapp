import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const MIGRATION_SQL = readFileSync(
  new URL("../../../../db/migrations/194_allow_signed_fec_receipts_cash_and_debt.sql", import.meta.url),
  "utf8"
);

describe("candidate finance signed-total migration", () => {
  it("allows signed receipts, cash, and debt while retaining every other amount guard", () => {
    for (const signedColumn of ["total_receipts", "cash_on_hand", "debts_owed"]) {
      expect(MIGRATION_SQL).not.toContain(`${signedColumn} IS NULL OR ${signedColumn} >= 0`);
    }

    for (const nonnegativeColumn of [
      "total_disbursements",
      "individual_itemized_total",
      "individual_unitemized_total",
      "other_committee_contributions",
      "transfers_from_affiliated_committees",
      "outside_support_total",
      "outside_oppose_total",
    ]) {
      expect(MIGRATION_SQL).toContain(`${nonnegativeColumn} IS NULL OR ${nonnegativeColumn} >= 0`);
    }
  });
});
