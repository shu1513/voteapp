import { describe, expect, it } from "vitest";

import { aggregateNewMexicoOutsideSpending } from "../../../src/pipeline/newMexicoFinance/newMexicoOutsideSpendingAggregator.js";
import type { NewMexicoCfisExpenditureRow } from "../../../src/pipeline/newMexicoFinance/newMexicoCfisArtifactReader.js";

function expenditure(overrides: Partial<NewMexicoCfisExpenditureRow> = {}): NewMexicoCfisExpenditureRow {
  return {
    OrgID: "9001",
    "Expenditure Amount": "70000.00",
    "Expenditure Date": "04/01/2026",
    "Payee Last Name": "Vendor",
    "Payee First Name": "",
    "Payee Middle Name": "",
    "Payee Prefix": "",
    "Payee Suffix": "",
    "Payee Address 1": "",
    "Payee Address 2": "",
    "Payee City": "Santa Fe",
    "Payee State": "NM",
    "Payee Zip Code": "87501",
    Description: "Independent expenditure",
    "Expenditure ID": "E1",
    "Filed Date": "04/02/2026",
    Election: "2026 General",
    "Report Name": "First Report",
    "Start of Period": "04/01/2026",
    "End of Period": "04/30/2026",
    Purpose: "Independent expenditure supporting/opposing others (explain)*",
    "Expenditure Type": "Independent Expenditure",
    Reason: "Haaland, Deb",
    Stance: "Support",
    "Report Entity Type": "PAC - Independent Expenditure",
    "Committee Name": "Accountable New Mexico",
    "Candidate Last Name": "",
    "Candidate First Name": "",
    "Candidate Middle Name": "",
    "Candidate Prefix": "",
    "Candidate Suffix": "",
    Amended: "",
    ...overrides,
  };
}

describe("newMexicoOutsideSpendingAggregator", () => {
  it("aggregates outside support and opposition groups by exact target candidate and cycle", () => {
    const sourceUrl =
      "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=EXP";
    const result = aggregateNewMexicoOutsideSpending({
      candidateName: "Deb Haaland",
      electionYear: 2026,
      sourceUrl,
      expenditureRows: [
        expenditure({ "Expenditure Amount": "70,000.00" }),
        expenditure({ "Expenditure ID": "E2", "Expenditure Amount": "30,000.25" }),
        expenditure({
          OrgID: "9002",
          "Committee Name": "New Mexico Future",
          "Expenditure ID": "E3",
          "Expenditure Amount": "5000",
          Stance: "Oppose",
        }),
        expenditure({ "Expenditure ID": "E4", Reason: "Other, Person", "Expenditure Amount": "900000" }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 100000.25,
        opposeTotal: 5000,
        groups: [
          {
            committeeId: "9001",
            committeeName: "Accountable New Mexico",
            supportOppose: "support",
            amount: 100000.25,
            sourceUrl,
          },
          {
            committeeId: "9002",
            committeeName: "New Mexico Future",
            supportOppose: "oppose",
            amount: 5000,
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

  it("accepts CFIS four-decimal expenditure amounts", () => {
    expect(
      aggregateNewMexicoOutsideSpending({
        candidateName: "Deb Haaland",
        electionYear: 2026,
        expenditureRows: [expenditure({ "Expenditure Amount": "42.1900" })],
      })
    ).toMatchObject({
      summary: {
        supportTotal: 42.19,
        opposeTotal: 0,
        groups: [
          expect.objectContaining({
            committeeId: "9001",
            amount: 42.19,
          }),
        ],
      },
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
    });
  });

  it("accepts political committee independent expenditure entity labels", () => {
    const result = aggregateNewMexicoOutsideSpending({
      candidateName: "Deb Haaland",
      electionYear: 2026,
      expenditureRows: [
        expenditure({
          "Report Entity Type": "Political Committee - Independent Expenditure",
          "Expenditure Amount": "1000.00",
        }),
      ],
    });

    expect(result).toMatchObject({
      summary: {
        supportTotal: 1000,
        opposeTotal: 0,
      },
      matchedExpenditureRowCount: 1,
      includedExpenditureRowCount: 1,
      skippedExpenditureRowCount: 0,
    });
  });

  it("accepts independent expenditure entity labels that do not include PAC or political committee wording", () => {
    const result = aggregateNewMexicoOutsideSpending({
      candidateName: "Deb Haaland",
      electionYear: 2026,
      expenditureRows: [
        expenditure({
          "Report Entity Type": "Independent Expenditure Committee",
          "Expenditure Amount": "2500.00",
        }),
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

  it("matches direct and comma-form candidate names without fuzzy matching", () => {
    expect(
      aggregateNewMexicoOutsideSpending({
        candidateName: "Deb Haaland",
        electionYear: 2026,
        expenditureRows: [
          expenditure({ Reason: "Haaland, Deb" }),
          expenditure({ "Expenditure ID": "E2", Reason: "Deb Haaland" }),
          expenditure({ "Expenditure ID": "E3", Reason: "Deb Haaland for Governor" }),
          expenditure({ "Expenditure ID": "E4", Reason: "Deb Haland" }),
        ],
      })
    ).toMatchObject({
      matchedExpenditureRowCount: 2,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 0,
    });
  });

  it("skips non-independent entities, bad stance, bad amount, wrong year, and missing committee fields", () => {
    const result = aggregateNewMexicoOutsideSpending({
      candidateName: "Deb Haaland",
      electionYear: 2026,
      expenditureRows: [
        expenditure({ "Report Entity Type": "Candidate" }),
        expenditure({ "Expenditure ID": "E2", Stance: "Neutral" }),
        expenditure({ "Expenditure ID": "E3", "Expenditure Amount": "not money" }),
        expenditure({ "Expenditure ID": "E4", "Expenditure Amount": "-10" }),
        expenditure({ "Expenditure ID": "E5", "Expenditure Date": "12/31/2024" }),
        expenditure({ "Expenditure ID": "E6", OrgID: "" }),
        expenditure({ "Expenditure ID": "E7", "Committee Name": "" }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 7,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 7,
    });
  });

  it("handles empty candidate names and validates inputs", () => {
    expect(
      aggregateNewMexicoOutsideSpending({
        candidateName: "   ",
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
      aggregateNewMexicoOutsideSpending({
        candidateName: "Deb Haaland",
        electionYear: 2019,
        expenditureRows: [],
      })
    ).toThrow("Invalid New Mexico outside spending aggregation election year");
    expect(() =>
      aggregateNewMexicoOutsideSpending({
        candidateName: "Deb Haaland",
        electionYear: 2026,
        expenditureRows: [],
        maxGroups: 0,
      })
    ).toThrow("Invalid New Mexico outside spending aggregation maxGroups");
  });
});
