import { describe, expect, it } from "vitest";

import {
  aggregateKentuckyOutsideSpending,
  normalizeKentuckyCandidateNameKeys,
} from "../../../src/pipeline/kentuckyFinance/kentuckyOutsideSpendingAggregator.js";
import type { KentuckyKrefIndependentExpenditureRecord } from "../../../src/pipeline/kentuckyFinance/kentuckyKrefClient.js";

function expenditure(
  overrides: Partial<KentuckyKrefIndependentExpenditureRecord> = {}
): KentuckyKrefIndependentExpenditureRecord {
  return {
    toWhomMade: "Media Vendor",
    spenderName: "Kentucky Future Project Action Fund",
    date: "10/15/2023",
    candidateName: "Andy Beshear",
    supportOppose: "support",
    officeOrBallotMeasure: "GOVERNOR",
    electionDate: "11/7/2023",
    electionYear: 2023,
    amount: 2500,
    ...overrides,
  };
}

describe("kentuckyOutsideSpendingAggregator", () => {
  it("aggregates structured KREF support and opposition groups", () => {
    const sourceUrl =
      "https://secure.kentucky.gov/kref/publicsearch/IndependentExpenditureSearch/ExportIndependentExpenditures";
    const result = aggregateKentuckyOutsideSpending({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeOrBallotMeasure: "Governor",
      sourceUrl,
      expenditureRecords: [
        expenditure({ amount: 2500 }),
        expenditure({ amount: 500.25, spenderName: "Kentucky Future Project Action Fund" }),
        expenditure({
          spenderName: "Commonwealth Freedom Fund",
          supportOppose: "oppose",
          amount: 750,
        }),
        expenditure({ candidateName: "Other Candidate", amount: 9000 }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 3000.25,
        opposeTotal: 750,
        groups: [
          {
            committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
            committeeName: "Kentucky Future Project Action Fund",
            supportOppose: "support",
            amount: 3000.25,
            sourceUrl,
          },
          {
            committeeKey: "COMMONWEALTH FREEDOM FUND",
            committeeName: "Commonwealth Freedom Fund",
            supportOppose: "oppose",
            amount: 750,
            sourceUrl,
          },
        ],
        sourceUrl,
      },
      matchedExpenditureRowCount: 3,
      includedExpenditureRowCount: 3,
      skippedExpenditureRowCount: 0,
    });
  });

  it("matches candidate names with comma form and first-last aliases", () => {
    expect([...normalizeKentuckyCandidateNameKeys("Beshear, Andy")]).toEqual(
      expect.arrayContaining(["BESHEAR ANDY", "ANDY BESHEAR"])
    );

    const result = aggregateKentuckyOutsideSpending({
      candidateName: "Beshear, Andy",
      electionDate: "2023-11-07",
      officeOrBallotMeasure: "GOVERNOR",
      expenditureRecords: [
        expenditure({ candidateName: "Andy Beshear", electionDate: "11/7/2023", amount: 100 }),
        expenditure({ candidateName: "Andy J. Beshear", amount: 200 }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 300,
        opposeTotal: 0,
      },
      matchedExpenditureRowCount: 2,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 0,
    });
  });

  it("matches app state-legislative office names against KREF office labels", () => {
    const result = aggregateKentuckyOutsideSpending({
      candidateName: "Julie Adams",
      electionDate: "5/19/2026",
      officeOrBallotMeasure: "State Senator",
      expenditureRecords: [
        expenditure({
          candidateName: "Julie Adams",
          electionDate: "5/19/2026",
          officeOrBallotMeasure: "STATE SENATOR (EVEN)",
          amount: 25_000,
        }),
      ],
    });

    expect(result.summary?.supportTotal).toBe(25_000);
    expect(result.matchedExpenditureRowCount).toBe(1);
    expect(result.includedExpenditureRowCount).toBe(1);
  });

  it("requires structured candidate, election date, and office matches", () => {
    const result = aggregateKentuckyOutsideSpending({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeOrBallotMeasure: "Governor",
      expenditureRecords: [
        expenditure({ candidateName: "Other Candidate", amount: 100 }),
        // Same-year primary rows belong to the cycle and are INCLUDED.
        expenditure({ electionDate: "5/16/2023", amount: 200 }),
        // Prior-cycle rows are excluded by year.
        expenditure({ electionDate: "11/8/2022", amount: 250 }),
        expenditure({ officeOrBallotMeasure: "Attorney General", amount: 300 }),
        expenditure({ amount: 400 }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 600,
        opposeTotal: 0,
      },
      matchedExpenditureRowCount: 2,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 0,
    });
  });

  it("skips target-matched rows with missing spender, stance, or usable amount", () => {
    const result = aggregateKentuckyOutsideSpending({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeOrBallotMeasure: "Governor",
      expenditureRecords: [
        expenditure({ spenderName: "", amount: 100 }),
        expenditure({ supportOppose: undefined, amount: 200 }),
        expenditure({ amount: 0 }),
        expenditure({ amount: Number.NaN }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 4,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 4,
    });
  });

  it("collapses same spender and stance, but keeps support and oppose separate", () => {
    const result = aggregateKentuckyOutsideSpending({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeOrBallotMeasure: "Governor",
      expenditureRecords: [
        expenditure({ amount: 100 }),
        expenditure({ amount: 200, spenderName: "Kentucky  Future Project Action Fund" }),
        expenditure({ amount: 50, supportOppose: "oppose" }),
      ],
    });

    expect(result.summary).toEqual({
      supportTotal: 300,
      opposeTotal: 50,
      groups: [
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "support",
          amount: 300,
        }),
        expect.objectContaining({
          committeeKey: "KENTUCKY FUTURE PROJECT ACTION FUND",
          supportOppose: "oppose",
          amount: 50,
        }),
      ],
      sourceUrl: null,
    });
  });

  it("limits groups by amount after preserving totals", () => {
    const result = aggregateKentuckyOutsideSpending({
      candidateName: "Andy Beshear",
      electionDate: "11/7/2023",
      officeOrBallotMeasure: "Governor",
      maxGroups: 1,
      expenditureRecords: [
        expenditure({ spenderName: "Small PAC", amount: 100 }),
        expenditure({ spenderName: "Large PAC", amount: 900 }),
      ],
    });

    expect(result.summary?.groups).toEqual([
      expect.objectContaining({ committeeKey: "LARGE PAC", committeeName: "Large PAC", amount: 900 }),
    ]);
    expect(result.summary?.supportTotal).toBe(1000);
  });

  it("handles empty candidate names and validates required inputs", () => {
    expect(
      aggregateKentuckyOutsideSpending({
        candidateName: "   ",
        electionDate: "11/7/2023",
        officeOrBallotMeasure: "Governor",
        expenditureRecords: [expenditure()],
      })
    ).toEqual({
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });

    expect(() =>
      aggregateKentuckyOutsideSpending({
        candidateName: "Andy Beshear",
        electionDate: "bad date",
        officeOrBallotMeasure: "Governor",
        expenditureRecords: [],
      })
    ).toThrow("MM/DD/YYYY or YYYY-MM-DD");
    expect(() =>
      aggregateKentuckyOutsideSpending({
        candidateName: "Andy Beshear",
        electionDate: "11/7/2023",
        officeOrBallotMeasure: " ",
        expenditureRecords: [],
      })
    ).toThrow("office or ballot measure is required");
    expect(() =>
      aggregateKentuckyOutsideSpending({
        candidateName: "Andy Beshear",
        electionDate: "11/7/2023",
        officeOrBallotMeasure: "Governor",
        expenditureRecords: [],
        maxGroups: 0,
      })
    ).toThrow("maxGroups");
  });
});
