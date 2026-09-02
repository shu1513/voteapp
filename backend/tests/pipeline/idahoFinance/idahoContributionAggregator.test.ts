import { describe, expect, it } from "vitest";

import {
  aggregateIdahoContributions,
  IDAHO_UNITEMIZED_SIZE_BUCKET,
  mapIdahoContributorSourceType,
} from "../../../src/pipeline/idahoFinance/idahoContributionAggregator.js";
import { contribution, GUID_A, GUID_B, registration } from "./idahoTestFixtures.js";

const PROFILE_URL = `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=${GUID_A}&tabName=CAN&isLegacy=false`;

function row(transactionId: number, overrides: Parameters<typeof contribution>[0] = {}) {
  return contribution({ transactionId, guid: `33333333-3333-4333-8333-${String(transactionId).padStart(12, "0")}`, ...overrides });
}

describe("aggregateIdahoContributions", () => {
  it("takes totals from the grid and builds size and source-type breakdowns from the registration's rows", () => {
    const result = aggregateIdahoContributions({
      registration: registration({ registrationGuid: GUID_A, totalRaised: 1986.25, totalSpent: 50, balanceOfFunds: -12.5 }),
      contributionRows: [
        row(1, { transactionAmount: 1000 }),
        row(2, { transactionAmount: 250, sourceTypeCode: "TBSN" }),
        row(3, { transactionAmount: 75.5, transactionSubTypeCode: "INKIND" }),
        row(4, { transactionAmount: 20, transactionSubTypeCode: "NITMY", sourceName: "Unitemized" }),
        row(5, { transactionAmount: 50, transactionSubTypeCode: "NITMY", sourceTypeCode: "TBSN" }),
        row(6, { transactionAmount: 5, transactionSubTypeCode: "ANYMS", sourceTypeCode: null }),
        // Interest: in the grid total, not a contribution.
        row(7, { transactionAmount: 10.25, transactionSubTypeCode: "ITR", sourceTypeCode: null }),
        row(8, { transactionAmount: 500, sourceTypeCode: "TCENC" }),
        row(9, { transactionAmount: 100, sourceTypeCode: "TCAN" }),
        row(10, { transactionAmount: 0.5, sourceTypeCode: "TXYZ" }),
        // Non-positive rows count toward the row sum only.
        row(11, { transactionAmount: -25 }),
        // Another registration of the same filer name, and a non-contribution row.
        row(12, { transactionAmount: 999, filerRegistrationGuid: GUID_B }),
        row(13, { transactionAmount: 999, transactionTypeCode: "TEXP" }),
      ],
    });

    expect(result.summary).toEqual({
      totalReceipts: 1986.25,
      directContributionTotal: 2001,
      totalDisbursements: 50,
      cashOnHand: -12.5,
      sourceUrl: PROFILE_URL,
    });
    expect(result.directBreakdowns.map((b) => [b.categoryType, b.categoryName, b.amount])).toEqual([
      ["contribution_size", "$1,000-$4,999", 1000],
      ["contribution_size", "$500-$999", 500],
      ["contribution_size", "$250-$499", 250],
      ["contribution_size", "$100-$249", 100],
      ["contribution_size", "$1-$99", 75.5],
      ["contribution_size", IDAHO_UNITEMIZED_SIZE_BUCKET, 75],
      ["contribution_size", "$0.01-$0.99", 0.5],
      ["contributor_source_type", "individuals", 1095.5],
      ["contributor_source_type", "party_committee", 500],
      ["contributor_source_type", "business_nonprofit_entities", 300],
      ["contributor_source_type", "candidate_self", 100],
      ["contributor_source_type", "other", 5.5],
    ]);
    expect(result.directBreakdowns.every((b) => b.contributorCount === null && b.sourceUrl === PROFILE_URL)).toBe(true);
    expect(result).toMatchObject({
      sourceRowCount: 13,
      registrationRowCount: 11,
      directContributionRowCount: 9,
      itemizedRowCount: 6,
      unitemizedRowCount: 3,
      nonDirectReceiptRowCount: 1,
      nonPositiveRowCount: 1,
      rowTotal: 1986.25,
      gridTotalRaised: 1986.25,
      rowCoverage: "exact",
    });
  });

  it("reports row coverage against the grid without changing the grid totals", () => {
    const rows = [row(1, { transactionAmount: 100 })];
    const returned = aggregateIdahoContributions({
      registration: registration({ registrationGuid: GUID_A, totalRaised: 90 }),
      contributionRows: rows,
      sourceUrl: " https://example.test/filing ",
    });
    expect(returned).toMatchObject({ rowCoverage: "rows_exceed_grid", rowTotal: 100, gridTotalRaised: 90 });
    expect(returned.summary.totalReceipts).toBe(90);
    expect(returned.summary.directContributionTotal).toBe(100);
    expect(returned.summary.sourceUrl).toBe("https://example.test/filing");
    expect(returned.directBreakdowns[0]?.sourceUrl).toBe("https://example.test/filing");

    const missing = aggregateIdahoContributions({
      registration: registration({ registrationGuid: GUID_A, totalRaised: 110 }),
      contributionRows: rows,
    });
    expect(missing).toMatchObject({ rowCoverage: "rows_below_grid", rowTotal: 100, gridTotalRaised: 110 });

    const empty = aggregateIdahoContributions({
      registration: registration({ registrationGuid: GUID_A, totalRaised: 0, totalSpent: 0, balanceOfFunds: 0 }),
      contributionRows: [],
    });
    expect(empty.summary).toMatchObject({ totalReceipts: 0, directContributionTotal: 0 });
    expect(empty.directBreakdowns).toEqual([]);
    expect(empty.rowCoverage).toBe("exact");
  });

  it("fails closed on rows that cannot belong to the registration or codes it does not know", () => {
    const aggregate = (rows: ReturnType<typeof row>[], reg = registration({ registrationGuid: GUID_A })) =>
      () => aggregateIdahoContributions({ registration: reg, contributionRows: rows });

    expect(aggregate([row(1, { transactionSubTypeCode: "LOAN" })])).toThrow('Unknown Idaho contribution subtype "LOAN"');
    expect(aggregate([row(1, { filerEntityId: 258 })])).toThrow("row for entity 258 (expected 257)");
    expect(aggregate([row(1, { electionYear: 2024 })])).toThrow("received a 2024 row (expected 2026)");
    expect(aggregate([row(1), row(1, { guid: "33333333-3333-4333-8333-999999999999" })])).toThrow(
      "repeated transaction 1"
    );
    expect(aggregate([row(1), row(2, { guid: row(1).guid })])).toThrow("repeated row");
    expect(aggregate([], registration({ registrationGuid: "not-a-guid" }))).toThrow("Invalid Idaho registration guid");
    expect(aggregate([], registration({ registrationGuid: GUID_A, totalRaised: -1 }))).toThrow("Invalid Idaho grid totalRaised");
    expect(aggregate([], registration({ registrationGuid: GUID_A, totalSpent: Number.NaN }))).toThrow("Invalid Idaho amount");
  });

  it("maps Sunshine source-type codes onto the shared contributor source types", () => {
    expect(mapIdahoContributorSourceType("TIND")).toBe("individuals");
    expect(mapIdahoContributorSourceType(" tbsn ")).toBe("business_nonprofit_entities");
    expect(mapIdahoContributorSourceType("TPAC")).toBe("pac_independent");
    expect(mapIdahoContributorSourceType("TCENC")).toBe("party_committee");
    expect(mapIdahoContributorSourceType("TCAN")).toBe("candidate_self");
    expect(mapIdahoContributorSourceType("TSELF")).toBe("candidate_self");
    expect(mapIdahoContributorSourceType("TXYZ")).toBe("other");
    expect(mapIdahoContributorSourceType(null)).toBe("other");
  });
});
