import { describe, expect, it } from "vitest";
import { aggregateSanFranciscoBalances } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoBalanceAggregator.js";
import type { SanFranciscoSummaryRow } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoOpenDataClient.js";

function summaryRow(
  overrides: Partial<SanFranciscoSummaryRow>,
): SanFranciscoSummaryRow {
  return {
    filingNid: "n1",
    filingIdNumber: "100",
    filingType: "FiledOriginal",
    formType: "FPPC460",
    periodStart: "2024-01-01T00:00:00.000",
    periodEnd: "2024-06-30T00:00:00.000",
    monetaryContributionsCents: null,
    line2Cents: null,
    contributionsCents: null,
    expendituresCents: null,
    endingCashCents: null,
    outstandingDebtsCents: null,
    loansReceivedCents: null,
    ...overrides,
  };
}

describe("aggregateSanFranciscoBalances", () => {
  // The real filing chain of committee 1467508 (Phase 4 gate committee):
  // overlapping periods included — a Jan-Sep amendment coexists with a
  // Jan-Jun original upstream — and $200,000 of loans across two filings.
  const filings = [
    summaryRow({
      filingNid: "n1",
      filingIdNumber: "101",
      filingType: "FiledAmendment",
      periodStart: "2024-01-01T00:00:00.000",
      periodEnd: "2024-09-21T00:00:00.000",
      endingCashCents: 8114375,
      outstandingDebtsCents: 10000000,
      loansReceivedCents: 10000000,
    }),
    summaryRow({
      filingNid: "n2",
      filingIdNumber: "102",
      periodStart: "2024-01-01T00:00:00.000",
      periodEnd: "2024-06-30T00:00:00.000",
      endingCashCents: 319427,
      outstandingDebtsCents: 50000,
      loansReceivedCents: 0,
    }),
    summaryRow({
      filingNid: "n3",
      filingIdNumber: "103",
      periodStart: "2024-09-22T00:00:00.000",
      periodEnd: "2024-10-19T00:00:00.000",
      endingCashCents: 7437394,
      outstandingDebtsCents: 20000000,
      loansReceivedCents: 10000000,
    }),
    summaryRow({
      filingNid: "n4",
      filingIdNumber: "104",
      periodStart: "2024-10-31T00:00:00.000",
      periodEnd: "2025-01-08T00:00:00.000",
      endingCashCents: 0,
      outstandingDebtsCents: 2938,
      loansReceivedCents: 0,
    }),
  ];

  it("takes balances from the latest filing and sums loans across filings", () => {
    expect(aggregateSanFranciscoBalances(filings)).toEqual({
      cashOnHandCents: 0,
      debtsOwedCents: 2938,
      loansReceivedCents: 20000000,
      latestFilingPeriodEnd: "2025-01-08T00:00:00.000",
      form460Filings: 4,
    });
  });

  it("ignores non-460 filings", () => {
    const result = aggregateSanFranciscoBalances([
      ...filings,
      summaryRow({
        filingNid: "n5",
        filingIdNumber: "999",
        formType: "FPPC497",
        periodEnd: "2026-01-01T00:00:00.000",
        endingCashCents: 123456,
        loansReceivedCents: 500000,
      }),
    ]);
    expect(result.cashOnHandCents).toBe(0);
    expect(result.loansReceivedCents).toBe(20000000);
    expect(result.form460Filings).toBe(4);
  });

  it("returns nulls when the committee has no 460 filings", () => {
    expect(
      aggregateSanFranciscoBalances([
        summaryRow({ formType: "FPPC461", endingCashCents: 100 }),
      ]),
    ).toEqual({
      cashOnHandCents: null,
      debtsOwedCents: null,
      loansReceivedCents: 0,
      latestFilingPeriodEnd: null,
      form460Filings: 0,
    });
  });

  it("breaks period-end ties numerically on the filing id", () => {
    const result = aggregateSanFranciscoBalances([
      summaryRow({
        filingNid: "a",
        filingIdNumber: "99",
        endingCashCents: 111,
      }),
      summaryRow({
        filingNid: "b",
        filingIdNumber: "100",
        endingCashCents: 222,
      }),
    ]);
    expect(result.cashOnHandCents).toBe(222);
  });
});
