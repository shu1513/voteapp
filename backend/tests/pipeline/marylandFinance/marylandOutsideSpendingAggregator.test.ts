import { describe, expect, it } from "vitest";

import { aggregateMarylandOutsideSpending } from "../../../src/pipeline/marylandFinance/marylandOutsideSpendingAggregator.js";
import type { MarylandCfsExpenditureRow } from "../../../src/pipeline/marylandFinance/marylandCfsArtifactReader.js";

function expenditure(overrides: Partial<MarylandCfsExpenditureRow> = {}): MarylandCfsExpenditureRow {
  return {
    "Filing Entity Id": "16020184",
    "Committee Name": "Momentum Maryland PAC",
    "Abbreviated Committee Name": "Momentum MD",
    "Committee Type": "Political Action Committee",
    "Payee Type": "Business Entity",
    "Payee Company Name": "Media Vendor LLC",
    "Payee Last Name": "",
    "Payee First Name": "",
    "Payee Middle Name": "",
    "Payee Country": "United States",
    "Payee Mailing Address1": "100 Main St",
    "Payee Mailing Address2": "",
    "Payee City": "Annapolis",
    "Payee State": "MD",
    "Payee Zip Code": '="21401"',
    "Vendor Type": "Advertising",
    "Vendor Name": "Media Vendor LLC",
    "Vendor Country": "United States",
    "Vendor Mailing Address1": "100 Main St",
    "Vendor Mailing Address2": "",
    "Vendor City": "Annapolis",
    "Vendor State": "MD",
    "Vendor Zip Code": '="21401"',
    "Transaction Type": "Expenditure",
    "Transaction Date": "03/15/2026",
    "Transaction Amount": "$10,000.00",
    Category: "Media",
    Purpose: "Independent expenditure",
    "Fund Type": "Electoral",
    Description: "Digital ads",
    "Pay In-Kind Contribution": "False",
    "Committee Filing Entity ID": "",
    "Report Name": "2026 Pre-Primary",
    "Candidate/Ballot Issue": "Gallucci, Justin",
    "Office Sought": "State Senator",
    Position: "Support",
    "Amount Applied": "$7,500.00",
    ...overrides,
  };
}

describe("marylandOutsideSpendingAggregator", () => {
  it("aggregates outside support and opposition groups by candidate, office, and position", () => {
    const sourceUrl = "https://campaignfinance.maryland.gov/public/cf/downloads";
    const result = aggregateMarylandOutsideSpending({
      candidateName: "Justin Gallucci",
      officeName: "State Senator",
      electionYear: 2026,
      sourceUrl,
      expenditureRows: [
        expenditure(),
        expenditure({
          "Transaction Amount": "$3,000.25",
          "Amount Applied": "",
          Description: "Mail",
        }),
        expenditure({
          "Filing Entity Id": "16030001",
          "Committee Name": "Maryland Future Fund",
          "Transaction Amount": "$2,000.00",
          "Amount Applied": "$1,500.00",
          Position: "Against",
        }),
        expenditure({
          "Candidate/Ballot Issue": "Other, Candidate",
          "Transaction Amount": "$99,999.00",
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 10500.25,
        opposeTotal: 1500,
        groups: [
          {
            committeeId: "16020184",
            committeeName: "Momentum Maryland PAC",
            supportOppose: "support",
            amount: 10500.25,
            sourceUrl,
          },
          {
            committeeId: "16030001",
            committeeName: "Maryland Future Fund",
            supportOppose: "oppose",
            amount: 1500,
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

  it("matches direct and comma-form candidate names without fuzzy matching", () => {
    const result = aggregateMarylandOutsideSpending({
      candidateName: "Justin Gallucci",
      officeName: "State Senator",
      electionYear: 2026,
      expenditureRows: [
        expenditure({ "Candidate/Ballot Issue": "Gallucci, Justin" }),
        expenditure({ "Candidate/Ballot Issue": "Justin Gallucci" }),
        expenditure({ "Candidate/Ballot Issue": "Justin Gallucci for Senate" }),
        expenditure({ "Candidate/Ballot Issue": "Justin Galluci" }),
      ],
    });

    expect(result.matchedExpenditureRowCount).toBe(2);
    expect(result.includedExpenditureRowCount).toBe(2);
    expect(result.skippedExpenditureRowCount).toBe(0);
  });

  it("requires matching office sought", () => {
    const result = aggregateMarylandOutsideSpending({
      candidateName: "Justin Gallucci",
      officeName: "State Senator",
      electionYear: 2026,
      expenditureRows: [
        expenditure({ "Office Sought": "House of Delegates", "Amount Applied": "$1,000.00" }),
        expenditure({ "Office Sought": "State Senate", "Amount Applied": "$2,000.00" }),
      ],
    });

    expect(result.summary?.supportTotal).toBe(2000);
    expect(result.matchedExpenditureRowCount).toBe(1);
    expect(result.includedExpenditureRowCount).toBe(1);
  });

  it("supports position aliases and cents without floating-point drift", () => {
    const result = aggregateMarylandOutsideSpending({
      candidateName: "Justin Gallucci",
      officeName: "State Senator",
      electionYear: 2026,
      expenditureRows: [
        expenditure({ Position: "For", "Amount Applied": "$0.10" }),
        expenditure({ Position: "Opposed", "Amount Applied": "$0.20" }),
      ],
    });

    expect(result.summary?.supportTotal).toBe(0.1);
    expect(result.summary?.opposeTotal).toBe(0.2);
  });

  it("skips candidate committees, invalid position, invalid amount, wrong year, and missing committee fields", () => {
    const result = aggregateMarylandOutsideSpending({
      candidateName: "Justin Gallucci",
      officeName: "State Senator",
      electionYear: 2026,
      expenditureRows: [
        expenditure({ "Committee Type": "Candidate Committee" }),
        expenditure({ Position: "Neutral" }),
        expenditure({ "Amount Applied": "not money", "Transaction Amount": "also bad" }),
        expenditure({ "Amount Applied": "$0.00", "Transaction Amount": "($10.00)" }),
        expenditure({ "Transaction Date": "12/31/2024" }),
        expenditure({ "Filing Entity Id": "" }),
        expenditure({ "Committee Name": "" }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 7,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 7,
    });
  });

  it("handles empty candidate or office names and validates inputs", () => {
    expect(
      aggregateMarylandOutsideSpending({
        candidateName: " ",
        officeName: "State Senator",
        electionYear: 2026,
        expenditureRows: [expenditure()],
      })
    ).toEqual({
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });

    expect(
      aggregateMarylandOutsideSpending({
        candidateName: "Justin Gallucci",
        officeName: " ",
        electionYear: 2026,
        expenditureRows: [expenditure()],
      })
    ).toEqual({
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });

    expect(() =>
      aggregateMarylandOutsideSpending({
        candidateName: "Justin Gallucci",
        officeName: "State Senator",
        electionYear: 1999,
        expenditureRows: [],
      })
    ).toThrow("Invalid Maryland outside spending aggregation election year");
    expect(() =>
      aggregateMarylandOutsideSpending({
        candidateName: "Justin Gallucci",
        officeName: "State Senator",
        electionYear: 2026,
        expenditureRows: [],
        maxGroups: 0,
      })
    ).toThrow("Invalid Maryland outside spending aggregation maxGroups");
  });
});
