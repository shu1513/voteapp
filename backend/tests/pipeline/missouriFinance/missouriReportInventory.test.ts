import { describe, expect, it } from "vitest";

import { selectMissouriCanonicalReportRows } from "../../../src/pipeline/missouriFinance/missouriReportInventory.js";
import type { MissouriMecReportInventoryRow } from "../../../src/pipeline/missouriFinance/missouriMecParsers.js";

const inventory: MissouriMecReportInventoryRow[] = [
  { reportId: "1", report: "April Quarterly Report", dateFiled: "2026-04-13", isAmended: false, lineageKey: "APRIL QUARTERLY REPORT" },
  { reportId: "2", report: "AMENDED April Quarterly Report", dateFiled: "2026-04-15", isAmended: true, lineageKey: "APRIL QUARTERLY REPORT" },
];

type Row = { report: string; amount: number; identity: string };
const select = (rows: Row[]) => selectMissouriCanonicalReportRows({
  inventory,
  rows,
  reportName: (row) => row.report,
  amountCents: (row) => row.amount,
  safeFingerprint: (row) => `${row.identity}:${row.amount}`,
});

describe("selectMissouriCanonicalReportRows", () => {
  it("accepts the portal's sole current label even when inventory records an amendment", () => {
    expect(select([{ report: "April Quarterly Report", amount: 10000, identity: "A" }])).toMatchObject({
      rows: [{ amount: 10000 }], diagnostics: [],
    });
  });

  it("keeps one equivalent amendment copy", () => {
    const result = select([
      { report: "April Quarterly Report", amount: 10000, identity: "A" },
      { report: "AMENDED April Quarterly Report", amount: 10000, identity: "A" },
    ]);
    expect(result.rows).toEqual([{ report: "AMENDED April Quarterly Report", amount: 10000, identity: "A" }]);
    expect(result.diagnostics).toEqual([]);
  });

  it("quarantines a correction/delta lineage instead of guessing replacement arithmetic", () => {
    const result = select([
      { report: "April Quarterly Report", amount: 30000, identity: "A" },
      { report: "AMENDED April Quarterly Report", amount: 12500, identity: "A" },
    ]);
    expect(result.rows).toEqual([]);
    expect(result.diagnostics).toEqual([{
      lineageKey: "APRIL QUARTERLY REPORT", reason: "ambiguous_amendment", excludedRowCount: 2, excludedAmountCents: 42500,
    }]);
  });
});
