import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

const MIGRATION_SQL = readFileSync(
  new URL("../../../../db/migrations/194_allow_signed_fec_receipts_and_cash.sql", import.meta.url),
  "utf8"
);

describe("candidate finance signed-total migration", () => {
  it("allows signed receipts and cash while retaining the other amount guards", () => {
    expect(MIGRATION_SQL).not.toContain("total_receipts IS NULL OR total_receipts >= 0");
    expect(MIGRATION_SQL).not.toContain("cash_on_hand IS NULL OR cash_on_hand >= 0");
    expect(MIGRATION_SQL).toContain("total_disbursements IS NULL OR total_disbursements >= 0");
    expect(MIGRATION_SQL).toContain("debts_owed IS NULL OR debts_owed >= 0");
    expect(MIGRATION_SQL).toContain("outside_support_total IS NULL OR outside_support_total >= 0");
    expect(MIGRATION_SQL).toContain("outside_oppose_total IS NULL OR outside_oppose_total >= 0");
  });
});
