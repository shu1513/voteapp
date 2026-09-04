import { describe, expect, it, vi } from "vitest";

import {
  aggregateKansasOutsideSpending,
  createKansasOutsideRowLoader,
  kansasOutsideGroupCommitteeId,
  loadKansasOutsideRows,
  reconcileKansasOutsideStatements,
  type KansasOutsideRow,
} from "../../../src/pipeline/kansasFinance/kansasOutsideSpendingAggregator.js";

const TREE = "https://www.kansas.gov/ethics/CFAScanned/Others/2026ElecCycle";
const TARGET = "1::SAMPLE:ALEX";

// Synthetic filers and vendors; the amounts replay the live Kansas Comeback
// shape (cumulative totals inside 1/1-7/23, then a reset for 7/24-10/22).
function row(
  overrides: Partial<KansasOutsideRow> & Pick<KansasOutsideRow, "sourceFileName" | "rowIndex" | "amountCents" | "statementTotalCents">
): KansasOutsideRow {
  return {
    filerName: "Example Comeback Fund",
    sourceUrl: `${TREE}/202607/${overrides.sourceFileName}`,
    periodDueKey: "202607",
    rowDate: "2026-06-01",
    vendorName: "Example Media Inc.",
    targetCommitteeId: TARGET,
    targetAsFiled: "Alex Sample, Governor",
    supportOppose: "oppose",
    ...overrides,
  };
}

/** Four statements: 370,443.63 -> 378,943.63 -> 383,943.63 in 202607, then 138,270.00 opening 202610 (filed under the 2607 folder). */
function comeback(): KansasOutsideRow[] {
  return [
    row({ sourceFileName: "IE_EC1_2607.pdf", rowIndex: 1, amountCents: 35_963_300, statementTotalCents: 37_044_363 }),
    row({ sourceFileName: "IE_EC1_2607.pdf", rowIndex: 2, amountCents: 1_081_063, statementTotalCents: 37_044_363 }),
    row({ sourceFileName: "IE_EC2_2607.pdf", rowIndex: 1, amountCents: 850_000, statementTotalCents: 37_894_363 }),
    row({ sourceFileName: "IE_EC3_2607.pdf", rowIndex: 1, amountCents: 500_000, statementTotalCents: 38_394_363 }),
    row({ sourceFileName: "IE_EC4_2607.pdf", rowIndex: 1, amountCents: 13_827_000, statementTotalCents: 13_827_000, periodDueKey: "202610" }),
  ];
}

describe("reconcileKansasOutsideStatements", () => {
  it("accepts cumulative totals within a period and a reset at the period boundary, whatever the row order", () => {
    expect(reconcileKansasOutsideStatements(comeback())).toEqual(new Map());
    expect(reconcileKansasOutsideStatements([...comeback()].reverse())).toEqual(new Map());
  });

  it("fails a filer period when a row is missing, misread, or its statement rows disagree on the total", () => {
    const missing = comeback().filter((entry) => !(entry.sourceFileName === "IE_EC1_2607.pdf" && entry.rowIndex === 2));
    expect([...reconcileKansasOutsideStatements(missing).entries()]).toEqual([
      ["EXAMPLE COMEBACK FUND|202607", "Example Comeback Fund 202607: running total 35963300 != IE_EC1_2607.pdf Total this Period 37044363"],
    ]);
    const misread = comeback().map((entry) => (entry.sourceFileName === "IE_EC3_2607.pdf" ? { ...entry, amountCents: 500_001 } : entry));
    expect(reconcileKansasOutsideStatements(misread).get("EXAMPLE COMEBACK FUND|202607")).toBe(
      "Example Comeback Fund 202607: running total 38394364 != IE_EC3_2607.pdf Total this Period 38394363"
    );
    const disagree = comeback().map((entry) => (entry.sourceFileName === "IE_EC1_2607.pdf" && entry.rowIndex === 2 ? { ...entry, statementTotalCents: 1 } : entry));
    expect(reconcileKansasOutsideStatements(disagree).get("EXAMPLE COMEBACK FUND|202607")).toBe(
      "Example Comeback Fund 202607: IE_EC1_2607.pdf rows disagree on Total this Period"
    );
    // The 202610 period of the same filer is judged on its own.
    expect(reconcileKansasOutsideStatements(misread).has("EXAMPLE COMEBACK FUND|202610")).toBe(false);
  });
});

describe("aggregateKansasOutsideSpending", () => {
  it("sums a candidate's rows per filer and direction, oppose first, across both periods", () => {
    const rows = [
      ...comeback(),
      row({ sourceFileName: "IE_SF_2607.pdf", rowIndex: 1, amountCents: 1_527_384, statementTotalCents: 1_527_384, filerName: "Sample Freedom Fund", supportOppose: "support", targetCommitteeId: " 1::sample:alex " }),
    ];
    expect(aggregateKansasOutsideSpending({ rows, targetCommitteeId: TARGET })).toEqual({
      status: "ok",
      supportCents: 1_527_384,
      opposeCents: 52_221_363,
      groups: [
        { committeeId: "IE:EXAMPLE COMEBACK FUND", committeeName: "Example Comeback Fund", supportOppose: "oppose", amountCents: 52_221_363, sourceUrl: `${TREE}/202607/IE_EC1_2607.pdf` },
        { committeeId: "IE:SAMPLE FREEDOM FUND", committeeName: "Sample Freedom Fund", supportOppose: "support", amountCents: 1_527_384, sourceUrl: `${TREE}/202607/IE_SF_2607.pdf` },
      ],
      statementCount: 5,
    });
  });

  it("reports none found for a candidate no row names, and never $0", () => {
    expect(aggregateKansasOutsideSpending({ rows: comeback(), targetCommitteeId: "7:85:HOLLOWAY:MARGARET" })).toEqual({ status: "none_found" });
    expect(aggregateKansasOutsideSpending({ rows: [], targetCommitteeId: TARGET })).toEqual({ status: "none_found" });
  });

  it("counts an unallocated row toward the statement checksum but toward no candidate", () => {
    const rows = [
      row({ sourceFileName: "IE_AF_2607.pdf", rowIndex: 1, amountCents: 502_495, statementTotalCents: 2_331_370, filerName: "Sample Victory Fund", targetCommitteeId: "7:72:SAMPLE:ALEX", targetAsFiled: "KS HD 72 Alex Sample" }),
      // One printed row, two candidates, one amount: no per-candidate figure exists.
      row({ sourceFileName: "IE_AF_2607.pdf", rowIndex: 2, amountCents: 1_826_375, statementTotalCents: 2_331_370, filerName: "Sample Victory Fund", targetCommitteeId: null, supportOppose: null, targetAsFiled: "KS HD 5 Chris Example - Support; KS HD 72 Alex Sample - Support" }),
      row({ sourceFileName: "IE_AF_2607.pdf", rowIndex: 3, amountCents: 2_500, statementTotalCents: 2_331_370, filerName: "Sample Victory Fund", targetCommitteeId: "7:5:EXAMPLE:CHRIS", supportOppose: "support", targetAsFiled: "KS HD 5 Chris Example" }),
    ];
    expect(aggregateKansasOutsideSpending({ rows, targetCommitteeId: "7:72:SAMPLE:ALEX" })).toMatchObject({ status: "ok", supportCents: 0, opposeCents: 502_495, statementCount: 1 });
    expect(aggregateKansasOutsideSpending({ rows, targetCommitteeId: "7:5:EXAMPLE:CHRIS" })).toMatchObject({ status: "ok", supportCents: 2_500, opposeCents: 0 });
    // Drop the unallocated row and the checksum no longer holds for anyone the filer names.
    const withoutUnallocated = rows.filter((entry) => entry.rowIndex !== 2);
    expect(aggregateKansasOutsideSpending({ rows: withoutUnallocated, targetCommitteeId: "7:72:SAMPLE:ALEX" })).toEqual({
      status: "unpublishable",
      reasons: ["Sample Victory Fund 202607: running total 504995 != IE_AF_2607.pdf Total this Period 2331370"],
    });
  });

  it("quarantines only the candidates a failing filer period names", () => {
    const rows = [
      ...comeback().map((entry) => (entry.sourceFileName === "IE_EC2_2607.pdf" ? { ...entry, amountCents: 1 } : entry)),
      row({ sourceFileName: "IE_SF_2607.pdf", rowIndex: 1, amountCents: 100, statementTotalCents: 100, filerName: "Sample Freedom Fund", targetCommitteeId: "7:85:HOLLOWAY:MARGARET" }),
    ];
    expect(aggregateKansasOutsideSpending({ rows, targetCommitteeId: TARGET })).toMatchObject({ status: "unpublishable", reasons: [expect.stringContaining("IE_EC2_2607.pdf")] });
    expect(aggregateKansasOutsideSpending({ rows, targetCommitteeId: "7:85:HOLLOWAY:MARGARET" })).toMatchObject({ status: "ok", opposeCents: 100 });
  });

  it("builds the group id from the normalized filer name", () => {
    expect(kansasOutsideGroupCommitteeId("  Kansas  Comeback PAC, Inc. ")).toBe("IE:KANSAS COMEBACK PAC INC");
    expect(() => kansasOutsideGroupCommitteeId(" ")).toThrow("filer name is blank");
  });
});

describe("loadKansasOutsideRows", () => {
  const dbRow = {
    filer_name: "Example Comeback Fund",
    source_file_name: "IE_EC2_2607.pdf",
    source_url: `${TREE}/202607/IE_EC2_2607.pdf`,
    period_due_key: "202607",
    statement_total: "378943.63",
    row_index: 1,
    row_date: "2026-07-02",
    vendor_name: "Example Media Inc.",
    target_committee_id: TARGET,
    target_as_filed: "Alex Sample, Governor",
    support_oppose: "oppose",
    amount: "8500.00",
  };

  it("selects a cycle's rows with amounts cast to text and maps them to cents", async () => {
    const db = { query: vi.fn(async () => ({ rows: [dbRow, { ...dbRow, row_index: 2, target_committee_id: null, support_oppose: null, row_date: null, vendor_name: null }], rowCount: 2 })) };
    const rows = await loadKansasOutsideRows(db as never, 2026);
    const [sql, params] = db.query.mock.calls[0] as unknown as [string, unknown[]];
    expect(sql).toContain("FROM public.ks_candidate_finance_outside_rows");
    expect(sql).toContain("statement_total::text");
    expect(sql).toContain("amount::text");
    expect(params).toEqual([2026]);
    expect(rows[0]).toEqual({
      filerName: "Example Comeback Fund",
      sourceFileName: "IE_EC2_2607.pdf",
      sourceUrl: `${TREE}/202607/IE_EC2_2607.pdf`,
      periodDueKey: "202607",
      statementTotalCents: 37_894_363,
      rowIndex: 1,
      rowDate: "2026-07-02",
      vendorName: "Example Media Inc.",
      targetCommitteeId: TARGET,
      targetAsFiled: "Alex Sample, Governor",
      supportOppose: "oppose",
      amountCents: 850_000,
    });
    expect(rows[1]).toMatchObject({ rowIndex: 2, targetCommitteeId: null, supportOppose: null, rowDate: null, vendorName: null });
  });

  it("fails closed on a direction outside the vocabulary or an amount not handed back as text", async () => {
    const bad = { query: vi.fn(async () => ({ rows: [{ ...dbRow, support_oppose: "against" }], rowCount: 1 })) };
    await expect(loadKansasOutsideRows(bad as never, 2026)).rejects.toThrow('IE_EC2_2607.pdf row 1: unknown direction "against"');
    const rounded = { query: vi.fn(async () => ({ rows: [{ ...dbRow, amount: 8500 }], rowCount: 1 })) };
    await expect(loadKansasOutsideRows(rounded as never, 2026)).rejects.toThrow("IE_EC2_2607.pdf row 1 amount: expected a numeric(16,2) text value");
  });

  it("memoizes one read per cycle across a batch", async () => {
    const db = { query: vi.fn(async (_sql: string, params: unknown[]) => (params[0] === 2026 ? { rows: [dbRow], rowCount: 1 } : { rows: [], rowCount: 0 })) };
    const load = createKansasOutsideRowLoader(db as never);
    const [first, second, other] = await Promise.all([load(2026), load(2026), load(2028)]);
    expect(first).toBe(second);
    expect(other).toEqual([]);
    expect(db.query).toHaveBeenCalledTimes(2);
  });
});
