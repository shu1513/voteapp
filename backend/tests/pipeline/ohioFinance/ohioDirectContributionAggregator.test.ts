import { describe, expect, it } from "vitest";

import {
  createOhioDirectContributionAccumulator,
} from "../../../src/pipeline/ohioFinance/ohioDirectContributionAggregator.js";
import type {
  OhioSosContributionRow,
  OhioSosCoverPageRow,
} from "../../../src/pipeline/ohioFinance/ohioSosBulkFiles.js";

function contributionRow(overrides: Partial<OhioSosContributionRow> = {}): OhioSosContributionRow {
  return {
    committeeName: "FRIENDS OF JANE DOE",
    masterKey: "16171",
    reportYear: 2026,
    reportKey: "500000001",
    reportDescription: "PRE-PRIMARY",
    shortDescription: "31-A Stmt of Contribution",
    contributorFirstName: "PAT",
    contributorMiddleName: null,
    contributorLastName: "SMITH",
    contributorSuffix: null,
    nonIndividual: null,
    pacRegNo: null,
    address: "1 MAIN ST",
    city: "COLUMBUS",
    state: "OH",
    zip: "43215",
    fileDateIso: "2026-03-01",
    amountCents: 5000,
    eventDateIso: null,
    empOccupation: null,
    inkindDescription: null,
    otherIncomeType: null,
    rcvEvent: null,
    candidateFirstName: "JANE",
    candidateLastName: "DOE",
    office: "GOVERNOR",
    district: "0",
    party: "DEMOCRAT",
    ...overrides,
  };
}

function coverRow(overrides: Partial<OhioSosCoverPageRow> = {}): OhioSosCoverPageRow {
  return {
    committeeName: "FRIENDS OF JANE DOE",
    masterKey: "16171",
    candidateFirstName: "JANE",
    candidateLastName: "DOE",
    reportKey: "500000001",
    reportYear: 2026,
    reportDescription: "PRE-PRIMARY",
    dateReportFiledIso: "2026-04-23",
    amountForwardCents: 0,
    totalContributionsCents: 100_00,
    totalOtherIncomeCents: 0,
    totalFundsCents: 100_00,
    totalExpendituresCents: 40_00,
    balanceOnHandCents: 60_00,
    valueInkindReceivedCents: null,
    valueInkindMadeCents: null,
    outstandingLoansOwedCents: null,
    outstandingDebtOwedCents: null,
    outstandingLoansToCents: null,
    valueIndependentExpendituresCents: null,
    ...overrides,
  };
}

describe("createOhioDirectContributionAccumulator", () => {
  it("aggregates donor-support rows into size buckets with contributor counts", () => {
    const accumulator = createOhioDirectContributionAccumulator({
      committeeId: "16171",
      electionYear: 2026,
      sourceUrl: "https://example.test/ohio",
    });
    // Two gifts from the same person land in one bucket with one contributor.
    accumulator.add(contributionRow({ amountCents: 50_00 }));
    accumulator.add(contributionRow({ amountCents: 25_00 }));
    accumulator.add(
      contributionRow({
        amountCents: 250_000_00,
        contributorFirstName: null,
        contributorLastName: null,
        nonIndividual: "BIG MONEY LLC",
        shortDescription: "31-E FR Contributions",
      })
    );
    accumulator.add(
      contributionRow({ amountCents: 100_00, contributorFirstName: "ALEX", shortDescription: "31-J-1 In-Kind Cont Rcvd" })
    );
    const result = accumulator.finish({
      coverRows: [coverRow({ totalContributionsCents: 250_175_00, totalOtherIncomeCents: 25_00 })],
    });

    // Receipts come from the cover page (authoritative); the itemized sum
    // is kept as a reconciliation diagnostic.
    expect(result.summary.totalReceipts).toBe(250_200);
    expect(result.itemizedReceiptsTotal).toBe(250_175);
    expect(result.summary.directContributionTotal).toBe(250_175);
    expect(result.summary.totalDisbursements).toBe(40);
    expect(result.summary.cashOnHand).toBe(60);
    expect(result.summary.sourceUrl).toBe("https://example.test/ohio");
    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(4);
    expect(result.skippedContributionRowCount).toBe(0);
    expect(result.directBreakdowns).toEqual([
      {
        categoryType: "contribution_size",
        categoryName: "$5,000+",
        amount: 250_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/ohio",
      },
      {
        categoryType: "contribution_size",
        categoryName: "$100-$249",
        amount: 100,
        contributorCount: 1,
        sourceUrl: "https://example.test/ohio",
      },
      {
        categoryType: "contribution_size",
        categoryName: "$1-$99",
        amount: 75,
        contributorCount: 1,
        sourceUrl: "https://example.test/ohio",
      },
    ]);
  });

  it("ignores rows for other committees", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    accumulator.add(contributionRow({ masterKey: "9999", amountCents: 100_00 }));
    const result = accumulator.finish({ coverRows: [] });
    expect(result.matchedContributionRowCount).toBe(0);
    expect(result.summary.totalReceipts).toBe(0);
  });

  it("counts other income toward receipts but not the direct total or buckets", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    accumulator.add(contributionRow({ amountCents: 100_00 }));
    accumulator.add(
      contributionRow({ amountCents: 900_00, shortDescription: "31-A-2 Other Income", otherIncomeType: "RE" })
    );
    const result = accumulator.finish({ coverRows: [] });
    // Without cover rows the itemized sum is the receipts fallback.
    expect(result.summary.totalReceipts).toBe(1_000);
    expect(result.summary.directContributionTotal).toBe(100);
    expect(result.otherIncomeRowCount).toBe(1);
    expect(result.unknownShortDescriptionRowCount).toBe(0);
    expect(result.directBreakdowns).toHaveLength(1);
  });

  it("fails closed on an unknown SHORT_DESCRIPTION: receipts yes, buckets no, counted", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    accumulator.add(contributionRow({ amountCents: 100_00, shortDescription: "31-Z Brand New Form" }));
    const result = accumulator.finish({ coverRows: [] });
    expect(result.summary.totalReceipts).toBe(100);
    expect(result.summary.directContributionTotal).toBe(0);
    expect(result.unknownShortDescriptionRowCount).toBe(1);
    expect(result.directBreakdowns).toHaveLength(0);
  });

  it("skips and counts missing, non-positive, and out-of-cycle amounts", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    accumulator.add(contributionRow({ amountCents: null }));
    accumulator.add(contributionRow({ amountCents: -50_00 }));
    accumulator.add(contributionRow({ amountCents: 0 }));
    accumulator.add(contributionRow({ amountCents: 100_00, reportYear: 2024 }));
    accumulator.add(contributionRow({ amountCents: 100_00, reportYear: null }));
    const result = accumulator.finish({ coverRows: [] });
    expect(result.summary.totalReceipts).toBe(0);
    expect(result.missingAmountRowCount).toBe(1);
    expect(result.nonPositiveAmountRowCount).toBe(2);
    expect(result.outOfCycleRowCount).toBe(2);
    expect(result.skippedContributionRowCount).toBe(5);
  });

  it("sums disbursements across the cycle's cover reports and takes cash from the latest", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    const result = accumulator.finish({
      coverRows: [
        coverRow({
          reportKey: "400000001",
          reportYear: 2025,
          reportDescription: "SEMIANNUAL (JULY)",
          dateReportFiledIso: "2025-07-31",
          totalExpendituresCents: 10_00,
          balanceOnHandCents: 90_00,
        }),
        coverRow({
          reportKey: "500000001",
          reportYear: 2026,
          dateReportFiledIso: "2026-04-23",
          totalExpendituresCents: 30_00,
          balanceOnHandCents: 250_00,
        }),
        // Outside the cycle window — ignored.
        coverRow({ reportKey: "300000001", reportYear: 2024, totalExpendituresCents: 999_00 }),
        // Another committee — ignored.
        coverRow({ masterKey: "9999", totalExpendituresCents: 999_00 }),
      ],
    });
    expect(result.summary.totalDisbursements).toBe(40);
    expect(result.summary.cashOnHand).toBe(250);
    expect(result.summary.totalReceipts).toBe(200);
    expect(result.coverReportCount).toBe(2);
    expect(result.coverReceiptsTotal).toBe(200);
  });

  it("breaks date-filed ties by numeric report key when picking the latest report", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    const result = accumulator.finish({
      coverRows: [
        coverRow({ reportKey: "99", dateReportFiledIso: "2026-04-23", balanceOnHandCents: 1_00 }),
        coverRow({ reportKey: "100", dateReportFiledIso: "2026-04-23", balanceOnHandCents: 2_00 }),
      ],
    });
    expect(result.summary.cashOnHand).toBe(2);
  });

  it("writes NULL cash on hand for a negative latest balance and flags it (decision 1)", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    const result = accumulator.finish({
      coverRows: [coverRow({ balanceOnHandCents: -12_34 })],
    });
    expect(result.summary.cashOnHand).toBeNull();
    expect(result.negativeBalanceOnHand).toBe(true);
    // The disbursement sum is unaffected.
    expect(result.summary.totalDisbursements).toBe(40);
  });

  it("returns NULL cover-derived fields when the committee has no cycle cover rows", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    accumulator.add(contributionRow({ amountCents: 100_00 }));
    const result = accumulator.finish({ coverRows: [coverRow({ masterKey: "9999" })] });
    expect(result.summary.totalDisbursements).toBeNull();
    expect(result.summary.cashOnHand).toBeNull();
    expect(result.coverReceiptsTotal).toBeNull();
    expect(result.coverReportCount).toBe(0);
    expect(result.negativeBalanceOnHand).toBe(false);
    expect(result.summary.totalReceipts).toBe(100);
    expect(result.itemizedReceiptsTotal).toBe(100);
  });

  it("caps the number of buckets at maxBreakdownsPerCategory, keeping the largest", () => {
    const accumulator = createOhioDirectContributionAccumulator({
      committeeId: "16171",
      electionYear: 2026,
      maxBreakdownsPerCategory: 2,
    });
    accumulator.add(contributionRow({ amountCents: 50_00 }));
    accumulator.add(contributionRow({ amountCents: 150_00, contributorFirstName: "B" }));
    accumulator.add(contributionRow({ amountCents: 300_00, contributorFirstName: "C" }));
    const result = accumulator.finish({ coverRows: [] });
    expect(result.directBreakdowns.map((breakdown) => breakdown.categoryName)).toEqual([
      "$250-$499",
      "$100-$249",
    ]);
  });

  it("rejects a non-numeric committee id and an implausible election year", () => {
    expect(() =>
      createOhioDirectContributionAccumulator({ committeeId: "ABC123", electionYear: 2026 })
    ).toThrow(/numeric Ohio SOS master key/);
    expect(() =>
      createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 1899 })
    ).toThrow(/election year/);
  });

  it("refuses use after finish", () => {
    const accumulator = createOhioDirectContributionAccumulator({ committeeId: "16171", electionYear: 2026 });
    accumulator.finish({ coverRows: [] });
    expect(() => accumulator.add(contributionRow())).toThrow(/already finished/);
    expect(() => accumulator.finish({ coverRows: [] })).toThrow(/already finished/);
  });
});
