import { describe, expect, it } from "vitest";

import { aggregateDistrictOfColumbiaOutsideSpending } from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaOutsideSpendingAggregator.js";
import type { DistrictOfColumbiaOcfExpenditureRecord } from "../../../src/pipeline/districtOfColumbiaFinance/districtOfColumbiaOcfClient.js";

function expenditure(overrides: Partial<DistrictOfColumbiaOcfExpenditureRecord> = {}): DistrictOfColumbiaOcfExpenditureRecord {
  return {
    committeeName: "DCCSA IEC",
    committeeKey: "DCCSA IEC",
    payeeName: "Media Vendor",
    purpose: "Independent Expenditures",
    furtherExplanation: "Digital ads supporting Phil Mendelson",
    amount: 2500,
    date: "05/01/2022",
    ...overrides,
  };
}

describe("districtOfColumbiaOutsideSpendingAggregator", () => {
  it("aggregates strict independent expenditure support and opposition groups", () => {
    const sourceUrl = "https://efiling.ocf.dc.gov/DataDownload/Export?exportType=CSV";
    const result = aggregateDistrictOfColumbiaOutsideSpending({
      candidateName: "Phil Mendelson",
      electionYear: 2022,
      sourceUrl,
      expenditureRecords: [
        expenditure({ amount: 2500 }),
        expenditure({ amount: 500.25, furtherExplanation: "Mail supporting Phil Mendelson for Council" }),
        expenditure({
          committeeName: "DC Future IEC",
          committeeKey: "DC FUTURE IEC",
          furtherExplanation: "Independent expenditure against Phil Mendelson",
          amount: 750,
        }),
        expenditure({ furtherExplanation: "Digital ads supporting Other Candidate", amount: 9000 }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 3000.25,
        opposeTotal: 750,
        groups: [
          {
            committeeKey: "DCCSA IEC",
            committeeName: "DCCSA IEC",
            supportOppose: "support",
            amount: 3000.25,
            sourceUrl,
          },
          {
            committeeKey: "DC FUTURE IEC",
            committeeName: "DC Future IEC",
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

  it("accepts support-of wording and comma-form candidate names", () => {
    const result = aggregateDistrictOfColumbiaOutsideSpending({
      candidateName: "Phil Mendelson",
      electionYear: 2022,
      expenditureRecords: [
        expenditure({ furtherExplanation: "Mailer in support of Phil Mendelson", amount: 100 }),
        expenditure({ furtherExplanation: "Mailer supporting Mendelson Phil", amount: 200 }),
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

  it("skips multi-candidate, mixed-direction, unclear-direction, and wrong-candidate text", () => {
    const result = aggregateDistrictOfColumbiaOutsideSpending({
      candidateName: "Phil Mendelson",
      electionYear: 2022,
      expenditureRecords: [
        expenditure({ furtherExplanation: "Digital ads supporting Phil Mendelson and Jane Doe" }),
        expenditure({ furtherExplanation: "Digital ads supporting Phil Mendelson and opposing Jane Doe" }),
        expenditure({ furtherExplanation: "Digital ads mentioning Phil Mendelson" }),
        expenditure({ furtherExplanation: "Digital ads supporting Jane Doe" }),
        expenditure({ furtherExplanation: "Digital ads supporting Phil Mendelson" }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 2500,
        opposeTotal: 0,
      },
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
    });
  });

  it("skips matched candidate rows with non-IE purpose, bad amounts, wrong year, and missing committee fields", () => {
    const result = aggregateDistrictOfColumbiaOutsideSpending({
      candidateName: "Phil Mendelson",
      electionYear: 2022,
      expenditureRecords: [
        expenditure({ purpose: "Printing", amount: 100 }),
        expenditure({ amount: 0 }),
        expenditure({ amount: -10 }),
        expenditure({ date: "12/31/2020", amount: 100 }),
        expenditure({ date: undefined, amount: 100 }),
        expenditure({ committeeName: "", committeeKey: "", amount: 100 }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 6,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 6,
    });
  });

  it("filters to the two-year election cycle ending in the election year", () => {
    const result = aggregateDistrictOfColumbiaOutsideSpending({
      candidateName: "Phil Mendelson",
      electionYear: 2022,
      expenditureRecords: [
        expenditure({ date: "12/31/2020", amount: 100 }),
        expenditure({ date: "1/1/2021", amount: 200 }),
        expenditure({ date: "2022-11-01", amount: 300 }),
        expenditure({ date: "1/1/2023", amount: 400 }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 500,
        opposeTotal: 0,
      },
      matchedExpenditureRowCount: 4,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 2,
    });
  });

  it("limits outside groups by amount", () => {
    const result = aggregateDistrictOfColumbiaOutsideSpending({
      candidateName: "Phil Mendelson",
      electionYear: 2022,
      maxGroups: 1,
      expenditureRecords: [
        expenditure({ committeeName: "Small IEC", committeeKey: "SMALL IEC", amount: 100 }),
        expenditure({ committeeName: "Large IEC", committeeKey: "LARGE IEC", amount: 900 }),
      ],
    });

    expect(result.summary?.groups).toEqual([
      expect.objectContaining({ committeeKey: "LARGE IEC", committeeName: "Large IEC", amount: 900 }),
    ]);
    expect(result.summary?.supportTotal).toBe(1000);
  });

  it("handles empty candidate names and validates inputs", () => {
    expect(
      aggregateDistrictOfColumbiaOutsideSpending({
        candidateName: "   ",
        electionYear: 2022,
        expenditureRecords: [expenditure()],
      })
    ).toEqual({
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });

    expect(() =>
      aggregateDistrictOfColumbiaOutsideSpending({
        candidateName: "Phil Mendelson",
        electionYear: 2013,
        expenditureRecords: [],
      })
    ).toThrow("Invalid D.C. outside spending aggregation election year");
    expect(() =>
      aggregateDistrictOfColumbiaOutsideSpending({
        candidateName: "Phil Mendelson",
        electionYear: 2022,
        expenditureRecords: [],
        maxGroups: 0,
      })
    ).toThrow("Invalid D.C. outside spending aggregation maxGroups");
  });
});
