import { describe, expect, it } from "vitest";

import { aggregateTennesseeOutsideSpending } from "../../../src/pipeline/tennesseeFinance/tennesseeOutsideSpendingAggregator.js";
import type { TennesseeCampExpenditureRecord } from "../../../src/pipeline/tennesseeFinance/tennesseeCampClient.js";

function expenditure(overrides: Partial<TennesseeCampExpenditureRecord> = {}): TennesseeCampExpenditureRecord {
  return {
    type: "Independent",
    adjustment: "N",
    amount: 533,
    date: "10/01/2022",
    electionYear: 2022,
    reportName: "Pre-General",
    candidatePacName: "RIGHT TENNESSEE",
    vendorName: "Media Vendor",
    purpose: "Mail",
    candidateFor: "LEE, BILL",
    supportOpposeCode: "S",
    ...overrides,
  };
}

describe("tennesseeOutsideSpendingAggregator", () => {
  it("aggregates structured independent expenditure support and opposition groups", () => {
    const sourceUrl = "https://apps.tn.gov/tncamp/public/ceresults.htm?d-1341904-e=1&6578706f7274=1";
    const result = aggregateTennesseeOutsideSpending({
      candidateName: "Bill Lee",
      ownerName: "LEE, BILL",
      electionYear: 2022,
      sourceUrl,
      expenditureRecords: [
        expenditure({ amount: 533 }),
        expenditure({ amount: 1000 }),
        expenditure({
          candidatePacName: "TENNESSEE FUTURE PAC",
          supportOpposeCode: "O",
          amount: 250,
        }),
        expenditure({ candidateFor: "DOE, JANE", amount: 9000 }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 1533,
        opposeTotal: 250,
        groups: [
          {
            committeeKey: "RIGHT TENNESSEE",
            committeeName: "RIGHT TENNESSEE",
            supportOppose: "support",
            amount: 1533,
            expenditureCount: 2,
            sourceUrl,
          },
          {
            committeeKey: "TENNESSEE FUTURE PAC",
            committeeName: "TENNESSEE FUTURE PAC",
            supportOppose: "oppose",
            amount: 250,
            expenditureCount: 1,
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

  it("requires independent type, support/oppose code, positive amount, non-adjustment, and cycle date", () => {
    const result = aggregateTennesseeOutsideSpending({
      candidateName: "Bill Lee",
      ownerName: "LEE, BILL",
      electionYear: 2022,
      expenditureRecords: [
        expenditure({ type: null }),
        expenditure({ supportOpposeCode: "" }),
        expenditure({ amount: 0 }),
        expenditure({ adjustment: "Y" }),
        expenditure({ date: "12/31/2020" }),
        expenditure({ date: "01/01/2021", amount: 200 }),
        expenditure({ date: "2022-11-01", amount: 300 }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 500,
        opposeTotal: 0,
      },
      matchedExpenditureRowCount: 7,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 5,
    });
  });

  it("limits groups by amount and validates inputs", () => {
    const result = aggregateTennesseeOutsideSpending({
      candidateName: "Bill Lee",
      ownerName: "LEE, BILL",
      electionYear: 2022,
      maxGroups: 1,
      expenditureRecords: [
        expenditure({ candidatePacName: "SMALL PAC", amount: 100 }),
        expenditure({ candidatePacName: "LARGE PAC", amount: 900 }),
      ],
    });

    expect(result.summary?.groups).toEqual([
      expect.objectContaining({ committeeKey: "LARGE PAC", committeeName: "LARGE PAC", amount: 900, expenditureCount: 1 }),
    ]);
    expect(result.summary?.supportTotal).toBe(1000);

    expect(() =>
      aggregateTennesseeOutsideSpending({
        candidateName: "Bill Lee",
        electionYear: 1999,
        expenditureRecords: [],
      })
    ).toThrow("Invalid Tennessee outside spending aggregation election year");
    expect(() =>
      aggregateTennesseeOutsideSpending({
        candidateName: "Bill Lee",
        electionYear: 2022,
        expenditureRecords: [],
        maxGroups: 0,
      })
    ).toThrow("Invalid Tennessee outside spending aggregation maxGroups");
  });
});
