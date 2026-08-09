import { describe, expect, it } from "vitest";
import { aggregateSanFranciscoDirectContributions } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoDirectContributionAggregator.js";
import type { SanFranciscoItemizedTransactionRow } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoOpenDataClient.js";

function row(
  overrides: Partial<SanFranciscoItemizedTransactionRow> & {
    formType: string;
    calculatedAmountCents: number;
  },
): SanFranciscoItemizedTransactionRow {
  return {
    filingNid: "f1",
    transactionId: null,
    transactionDate: "2024-10-01T00:00:00.000",
    contributorFirstName: null,
    contributorLastName: null,
    occupation: null,
    employer: null,
    city: null,
    state: null,
    zip: null,
    entityCode: null,
    transactionAmount1Cents: null,
    memoCode: false,
    isItemized: true,
    crossReferenceMatch: null,
    crossReferenceSchedule: null,
    supportOpposeCode: null,
    transactionCode: null,
    ...overrides,
  };
}

describe("aggregateSanFranciscoDirectContributions", () => {
  it("composes itemized = non-memo A + non-memo C + unpaired late", () => {
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({ formType: "A", calculatedAmountCents: 50_000 }),
        row({ formType: "C", calculatedAmountCents: 20_000 }),
        row({ formType: "F497P1", calculatedAmountCents: 100_000 }),
        row({ formType: "F460ALine2", calculatedAmountCents: 12_345 }),
        row({ formType: "F460CLine2", calculatedAmountCents: 678 }),
      ],
      publicFundsApprovalCents: [],
    });
    expect(result.itemizedCents).toBe(170_000);
    expect(result.scheduleACents).toBe(50_000);
    expect(result.scheduleCCents).toBe(20_000);
    expect(result.unpairedLateCents).toBe(100_000);
    expect(result.unitemizedCents).toBe(12_345);
    expect(result.unitemizedNonmonetaryCents).toBe(678);
  });

  it("excludes memo rows from every sum but reports them", () => {
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({ formType: "A", calculatedAmountCents: 50_000 }),
        row({ formType: "A", calculatedAmountCents: 7_000, memoCode: true }),
        row({
          formType: "F497P1",
          calculatedAmountCents: 3_000,
          memoCode: true,
        }),
      ],
      publicFundsApprovalCents: [],
    });
    expect(result.itemizedCents).toBe(50_000);
    expect(result.diagnostics.memoRowsExcluded).toBe(2);
    expect(result.diagnostics.memoCentsExcluded).toBe(10_000);
  });

  it("drops a late row on any Schedule A transaction_id hit, splitting the counters by amount", () => {
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({
          formType: "A",
          transactionId: "INC1",
          calculatedAmountCents: 50_000,
        }),
        row({
          formType: "A",
          transactionId: "INC2",
          calculatedAmountCents: 40_000,
        }),
        row({
          formType: "F497P1",
          transactionId: "INC1",
          calculatedAmountCents: 50_000,
        }),
        row({
          formType: "F497P1",
          transactionId: "INC2",
          calculatedAmountCents: 41_000,
        }),
      ],
      publicFundsApprovalCents: [],
    });
    expect(result.itemizedCents).toBe(90_000);
    expect(result.diagnostics.latePairedById).toBe(1);
    expect(result.diagnostics.latePairedByIdAmountMismatch).toBe(1);
    expect(result.diagnostics.unpairedLateRows).toBe(0);
  });

  it("excludes late-reported loans whose Schedule twin is B1", () => {
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({
          formType: "B1",
          transactionId: "LOAN1",
          transactionDate: null,
          calculatedAmountCents: 10_000_000,
        }),
        row({
          formType: "F497P1",
          transactionId: "LOAN1",
          calculatedAmountCents: 10_000_000,
        }),
      ],
      publicFundsApprovalCents: [],
    });
    expect(result.itemizedCents).toBe(0);
    expect(result.diagnostics.lateLoanRowsExcluded).toBe(1);
    expect(result.diagnostics.lateLoanCentsExcluded).toBe(10_000_000);
  });

  it("excludes 497-reported public-financing disbursements matching an approval amount", () => {
    const cityRow = row({
      formType: "F497P1",
      contributorLastName: "City and Council of San Francisco",
      calculatedAmountCents: 6_000_000,
    });
    const withApproval = aggregateSanFranciscoDirectContributions({
      rows: [cityRow],
      publicFundsApprovalCents: [6_000_000],
    });
    expect(withApproval.itemizedCents).toBe(0);
    expect(withApproval.diagnostics.latePublicFundsRowsExcluded).toBe(1);
    expect(withApproval.diagnostics.latePublicFundsCentsExcluded).toBe(
      6_000_000,
    );
    // Without a matching approval the city row is NOT assumed to be public
    // money — it stays in as an unpaired late contribution.
    const withoutApproval = aggregateSanFranciscoDirectContributions({
      rows: [cityRow],
      publicFundsApprovalCents: [],
    });
    expect(withoutApproval.itemizedCents).toBe(6_000_000);
  });

  it("pairs id-less late rows by amount, date, and case-insensitive name", () => {
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({
          formType: "A",
          contributorLastName: "Lurie",
          calculatedAmountCents: 25_000,
          transactionDate: "2024-10-20T00:00:00.000",
        }),
        row({
          formType: "F497P1",
          contributorLastName: "LURIE",
          calculatedAmountCents: 25_000,
          transactionDate: "2024-10-20T00:00:00.000",
        }),
        row({
          formType: "F497P1",
          contributorLastName: "MEI",
          calculatedAmountCents: 5_000,
          transactionDate: "2024-10-21T00:00:00.000",
        }),
      ],
      publicFundsApprovalCents: [],
    });
    expect(result.diagnostics.latePairedByAmountDate).toBe(1);
    expect(result.diagnostics.unpairedLateRows).toBe(1);
    expect(result.itemizedCents).toBe(30_000);
  });

  it("consumes each amount/date twin once, so duplicate late rows only pair against distinct Schedule A rows", () => {
    const late = {
      formType: "F497P1",
      contributorLastName: "DOE",
      calculatedAmountCents: 25_000,
      transactionDate: "2024-10-20T00:00:00.000",
    } as const;
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({
          formType: "A",
          contributorLastName: "Doe",
          calculatedAmountCents: 25_000,
          transactionDate: "2024-10-20T00:00:00.000",
        }),
        row(late),
        row(late),
      ],
      publicFundsApprovalCents: [],
    });
    expect(result.diagnostics.latePairedByAmountDate).toBe(1);
    expect(result.diagnostics.unpairedLateRows).toBe(1);
    // One late row is the re-reported twin; the other is real money.
    expect(result.itemizedCents).toBe(50_000);
  });

  it("keeps refunds in the sums and out of size buckets", () => {
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({
          formType: "A",
          entityCode: "IND",
          occupation: "Attorney",
          calculatedAmountCents: 50_000,
        }),
        row({
          formType: "A",
          entityCode: "IND",
          occupation: "Attorney",
          calculatedAmountCents: -20_000,
        }),
      ],
      publicFundsApprovalCents: [],
    });
    expect(result.itemizedCents).toBe(30_000);
    expect(result.diagnostics.refundRows).toBe(1);
    expect(result.diagnostics.refundCents).toBe(-20_000);
    const sizes = result.breakdowns.filter(
      (b) => b.categoryType === "contribution_size",
    );
    expect(sizes).toEqual([
      {
        categoryType: "contribution_size",
        categoryName: "$500-$999",
        amountCents: 50_000,
        contributorCount: 1,
      },
    ]);
    const occupations = result.breakdowns.filter(
      (b) => b.categoryType === "occupation",
    );
    // Net of the refund; the refund does not add a contributor.
    expect(occupations).toEqual([
      {
        categoryType: "occupation",
        categoryName: "Attorney",
        amountCents: 30_000,
        contributorCount: 1,
      },
    ]);
  });

  it("restricts occupation/employer to individual contributors", () => {
    const result = aggregateSanFranciscoDirectContributions({
      rows: [
        row({
          formType: "A",
          entityCode: "COM",
          occupation: "PAC",
          employer: "PAC Inc",
          calculatedAmountCents: 100_000,
        }),
        row({
          formType: "A",
          entityCode: "IND",
          occupation: "Teacher",
          employer: "SFUSD",
          calculatedAmountCents: 10_000,
        }),
      ],
      publicFundsApprovalCents: [],
    });
    const names = result.breakdowns
      .filter((b) => b.categoryType !== "contribution_size")
      .map((b) => b.categoryName);
    expect(names.sort()).toEqual(["SFUSD", "Teacher"]);
    // Size buckets cover all entity codes.
    const sizes = result.breakdowns.filter(
      (b) => b.categoryType === "contribution_size",
    );
    expect(sizes.map((b) => b.categoryName).sort()).toEqual([
      "$1,000-$4,999",
      "$100-$249",
    ]);
  });

  it("caps occupation/employer at the limit but never size buckets", () => {
    const rows = [
      ...Array.from({ length: 3 }, (_, index) =>
        row({
          formType: "A",
          entityCode: "IND",
          occupation: `Occupation ${index}`,
          calculatedAmountCents: (index + 1) * 1_000,
        }),
      ),
      row({ formType: "A", calculatedAmountCents: 600_000 }),
    ];
    const result = aggregateSanFranciscoDirectContributions({
      rows,
      publicFundsApprovalCents: [],
      maxBreakdownsPerCategory: 2,
    });
    const occupations = result.breakdowns.filter(
      (b) => b.categoryType === "occupation",
    );
    expect(occupations.map((b) => b.categoryName)).toEqual([
      "Occupation 2",
      "Occupation 1",
    ]);
    const sizes = result.breakdowns.filter(
      (b) => b.categoryType === "contribution_size",
    );
    expect(sizes.map((b) => b.categoryName).sort()).toEqual([
      "$1-$99",
      "$5,000+",
    ]);
  });

  it("rejects unexpected form types loudly", () => {
    expect(() =>
      aggregateSanFranciscoDirectContributions({
        rows: [row({ formType: "F496", calculatedAmountCents: 1_000 })],
        publicFundsApprovalCents: [],
      }),
    ).toThrow(/unexpected form type F496/);
  });
});
