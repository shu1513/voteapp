import { describe, expect, it } from "vitest";

import {
  aggregateGeorgiaDirectContributions,
  GEORGIA_UNKNOWN_OCCUPATION_LABEL,
  type GeorgiaTaggedTransactionRow,
} from "../../../src/pipeline/georgiaFinance/georgiaDirectContributionAggregator.js";
import type { GeorgiaEthicsHost, GeorgiaTransactionRow } from "../../../src/pipeline/georgiaFinance/georgiaEthicsClient.js";

let nextTransactionId = 1;

function taggedRow(
  host: GeorgiaEthicsHost,
  overrides: Partial<GeorgiaTransactionRow> = {}
): GeorgiaTaggedTransactionRow {
  const transactionId = overrides.transactionId ?? (nextTransactionId += 1);
  const peachfile = host === "peachfile";
  return {
    host,
    row: {
      guid: `guid-${transactionId}`,
      transactionId,
      transactionAmount: 100,
      filerEntityId: peachfile ? 100035 : 757274,
      filerRegistrationGuid: "d973ab3b-54c2-416e-81ce-f5b1ee9a6f57",
      filerReportGuid: "7d5e2ef2-e1b5-4bc9-a555-13f6392f4199",
      timedFiledReportGuid: null,
      filerReportId: 37,
      filerReportVersionId: 1,
      transactionDate: peachfile ? "11/22/2024" : "2025-05-01",
      sourceName: "Jane Example",
      payeeOccupation: "Attorney",
      payeeEmployer: "Example LLP",
      transactionTypeCode: peachfile ? "TCON" : "CON",
      transactionSubTypeCode: peachfile ? "ITMY" : "MOI",
      transactionSubTypeDesc: peachfile ? "Itemized Contribution" : "Monetary Itemized",
      transactionSourceTypeCode: peachfile ? "TIND" : "IND",
      transactionStatusCode: peachfile ? "TFIL" : "F",
      reportName: "Campaign Contribution Disclosure Report",
      electionYear: 2026,
      ...overrides,
    },
  };
}

describe("aggregateGeorgiaDirectContributions", () => {
  it("buckets itemized individuals into occupation and size on both hosts", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        taggedRow("peachfile", { transactionAmount: 2800, sourceName: "A", payeeOccupation: "Attorney" }),
        taggedRow("efile_archive", { transactionAmount: 200, sourceName: "B", payeeOccupation: "Attorney" }),
        taggedRow("peachfile", { transactionAmount: 50, sourceName: "C", payeeOccupation: "Retired" }),
      ],
      sourceUrl: "https://ethics.ga.gov/records-search-all/",
    });

    const occupations = result.directBreakdowns.filter((b) => b.categoryType === "occupation");
    expect(occupations).toEqual([
      expect.objectContaining({ categoryName: "Attorney", amount: 3000, contributorCount: 2 }),
      expect.objectContaining({ categoryName: "Retired", amount: 50, contributorCount: 1 }),
    ]);
    const sizes = result.directBreakdowns.filter((b) => b.categoryType === "contribution_size");
    expect(sizes).toEqual([
      expect.objectContaining({ categoryName: "$1,000-$4,999", amount: 2800 }),
      expect.objectContaining({ categoryName: "$100-$249", amount: 200 }),
      expect.objectContaining({ categoryName: "$1-$99", amount: 50 }),
    ]);
    expect(result.syncedRowSum).toBe(3050);
    expect(result.occupationCoveredAmount).toBe(3050);
    expect(result.occupationUnknownAmount).toBe(0);
  });

  it("keys the individuals-only occupation gate on the per-host source code", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        // PeachFile business: size bucket only.
        taggedRow("peachfile", { transactionAmount: 1000, transactionSourceTypeCode: "TBSN", sourceName: "Corp A" }),
        // Archive individual (IND, not TIND): must still reach occupation.
        taggedRow("efile_archive", { transactionAmount: 500, sourceName: "D", payeeOccupation: "Teacher" }),
        // Archive committee: size bucket only.
        taggedRow("efile_archive", { transactionAmount: 250, transactionSourceTypeCode: "COM", sourceName: "PAC" }),
      ],
    });
    const occupations = result.directBreakdowns.filter((b) => b.categoryType === "occupation");
    expect(occupations).toEqual([expect.objectContaining({ categoryName: "Teacher", amount: 500 })]);
    expect(result.directBreakdowns.filter((b) => b.categoryType === "contribution_size")).toHaveLength(3);
  });

  it("collapses blank and placeholder occupations into the Unknown bucket", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        taggedRow("peachfile", { transactionAmount: 100, sourceName: "A", payeeOccupation: "Information Requested" }),
        taggedRow("peachfile", { transactionAmount: 200, sourceName: "B", payeeOccupation: null }),
        taggedRow("efile_archive", { transactionAmount: 300, sourceName: "C", payeeOccupation: "N/A" }),
        taggedRow("peachfile", { transactionAmount: 400, sourceName: "D", payeeOccupation: "Nurse" }),
      ],
    });
    const occupations = result.directBreakdowns.filter((b) => b.categoryType === "occupation");
    expect(occupations).toEqual([
      expect.objectContaining({ categoryName: GEORGIA_UNKNOWN_OCCUPATION_LABEL, amount: 600, contributorCount: 3 }),
      expect.objectContaining({ categoryName: "Nurse", amount: 400 }),
    ]);
    expect(result.occupationUnknownAmount).toBe(600);
    expect(result.occupationCoveredAmount).toBe(400);
  });

  it("keeps unitemized, in-kind, and anonymous dollars in the sum but out of every bucket", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        taggedRow("peachfile", { transactionAmount: 1000 }),
        taggedRow("peachfile", { transactionAmount: 90, transactionSubTypeCode: "NITMY", transactionSourceTypeCode: null }),
        taggedRow("peachfile", { transactionAmount: 80, transactionSubTypeCode: "INKIND" }),
        taggedRow("efile_archive", { transactionAmount: 70, transactionSubTypeCode: "NIM", transactionSourceTypeCode: null }),
        taggedRow("efile_archive", { transactionAmount: 60, transactionSubTypeCode: "IKD" }),
        taggedRow("efile_archive", { transactionAmount: 5, transactionSubTypeCode: "ANO", transactionSourceTypeCode: null }),
      ],
    });
    expect(result.syncedRowSum).toBe(1305);
    expect(result.unitemizedAmount).toBe(160);
    expect(result.inKindAmount).toBe(140);
    expect(result.anonymousAmount).toBe(5);
    expect(result.bucketedRowCount).toBe(1);
    expect(result.directBreakdowns.filter((b) => b.categoryType === "contribution_size")).toEqual([
      expect.objectContaining({ amount: 1000 }),
    ]);
  });

  it("keeps negative return rows in the sum and out of the buckets — the index nets returns itself", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        taggedRow("peachfile", { transactionAmount: 3300 }),
        taggedRow("peachfile", { transactionAmount: -3300, sourceName: "Refunded Donor" }),
      ],
    });
    expect(result.syncedRowSum).toBe(0);
    expect(result.returnedRowCount).toBe(1);
    expect(result.returnedAmount).toBe(-3300);
    expect(result.directBreakdowns.filter((b) => b.categoryType === "contribution_size")).toEqual([
      expect.objectContaining({ amount: 3300 }),
    ]);
  });

  it("keeps unpinned subtypes (loans, interest) in the sum and out of the buckets", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        taggedRow("peachfile", { transactionAmount: 100 }),
        taggedRow("peachfile", { transactionAmount: 50_000, transactionSubTypeCode: "LOAN" }),
        taggedRow("efile_archive", { transactionAmount: 12.34, transactionSubTypeCode: null }),
      ],
    });
    expect(result.syncedRowSum).toBe(50112.34);
    expect(result.unpinnedSubTypeRowCount).toBe(2);
    expect(result.unpinnedSubTypeAmount).toBe(50012.34);
    expect(result.bucketedRowCount).toBe(1);
  });

  it("excludes unrecognized statuses from everything, per host", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        taggedRow("peachfile", { transactionAmount: 100 }),
        // Archive status code on a PeachFile row is unrecognized.
        taggedRow("peachfile", { transactionAmount: 999, transactionStatusCode: "F" }),
        taggedRow("efile_archive", { transactionAmount: 200 }),
        taggedRow("efile_archive", { transactionAmount: 888, transactionStatusCode: "DEL" }),
      ],
    });
    expect(result.syncedRowSum).toBe(300);
    expect(result.unrecognizedStatusRowCount).toBe(2);
    expect(result.unrecognizedStatusAmount).toBe(1887);
  });

  it("includes timed-pending (TPEN/TPAMD) money — it is inside the official totals", () => {
    const result = aggregateGeorgiaDirectContributions({
      rows: [
        taggedRow("peachfile", { transactionAmount: 3300, transactionStatusCode: "TPEN" }),
        taggedRow("peachfile", { transactionAmount: 200, transactionStatusCode: "TPAMD" }),
        taggedRow("peachfile", { transactionAmount: 100, transactionStatusCode: "TAMD" }),
      ],
    });
    expect(result.syncedRowSum).toBe(3600);
    expect(result.bucketedRowCount).toBe(3);
  });

  it("caps occupation rows but never size buckets", () => {
    const rows: GeorgiaTaggedTransactionRow[] = [];
    for (let i = 0; i < 5; i += 1) {
      rows.push(
        taggedRow("peachfile", {
          transactionAmount: 100 + i,
          sourceName: `Donor ${i}`,
          payeeOccupation: `Occupation ${i}`,
        })
      );
    }
    const result = aggregateGeorgiaDirectContributions({ rows, maxBreakdownsPerCategory: 2 });
    expect(result.directBreakdowns.filter((b) => b.categoryType === "occupation")).toHaveLength(2);
    expect(result.directBreakdowns.filter((b) => b.categoryType === "contribution_size")).toHaveLength(1);
  });
});
