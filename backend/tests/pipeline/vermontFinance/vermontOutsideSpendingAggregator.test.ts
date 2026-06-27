import { describe, expect, it } from "vitest";

import { aggregateVermontOutsideSpending } from "../../../src/pipeline/vermontFinance/vermontOutsideSpendingAggregator.js";
import type { VermontExpenditureRow } from "../../../src/pipeline/vermontFinance/vermontCampaignFinanceClient.js";

function expenditure(overrides: Partial<VermontExpenditureRow> = {}): VermontExpenditureRow {
  return {
    transactionId: 10,
    transactionVersionId: 1,
    guid: "expenditure-guid-1",
    filerRegistrationGuid: "pac-guid",
    filerName: "VERMONT FUTURE PAC",
    transactionAmount: 1000,
    transactionDate: "09/01/2024",
    transactionCategoryCode: "PUCON",
    transactionCategoryDescription: "Contribution to Candidate",
    expenditurePurpose: "Contribution to Candidate",
    description: null,
    isStanceSupport: null,
    payeeType: "Candidate",
    sourceName: "SCOTT, PHIL",
    transactionSource: "Candidate",
    filerTypeCode: "PAC",
    filerTypeDescription: "Political Action Committee",
    electionYear: 2024,
    electionCycle: "2024 General",
    electionId: 35,
    officeId: null,
    officeType: null,
    entityId: 33545,
    reportName: "10/01/2024 - GENERAL",
    candidateMentioned: null,
    candidateFirstName: "PHIL",
    candidateLastName: "SCOTT",
    candidateMiddleName: null,
    sourceAddressLine1: null,
    sourceAddressLine2: null,
    sourceCity: null,
    sourceState: null,
    sourceZipCode: null,
    ...overrides,
  };
}

describe("vermontOutsideSpendingAggregator", () => {
  it("groups PAC contributions to a candidate registrant as supporting PAC support", () => {
    const result = aggregateVermontOutsideSpending({
      candidateName: "Phil Scott",
      candidateEntityId: 33545,
      electionYear: 2024,
      sourceUrl: "https://campaignfinance.vermont.gov/",
      expenditureRows: [
        expenditure({ transactionAmount: 1000 }),
        expenditure({ transactionId: 11, guid: "row-2", transactionAmount: 250 }),
      ],
    });

    expect(result).toEqual({
      matchedExpenditureRowCount: 2,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 0,
      summary: {
        outsideSupportTotal: 1250,
        outsideOpposeTotal: 0,
        sourceUrl: "https://campaignfinance.vermont.gov/",
        groups: [
          {
            filerRegistrationGuid: "pac-guid",
            filerName: "VERMONT FUTURE PAC",
            supportOppose: "support",
            supportMechanism: "vt_pac_contribution_to_registrant",
            amount: 1250,
            expenditureCount: 2,
            entityId: 33545,
            sourceUrl: "https://campaignfinance.vermont.gov/",
          },
        ],
      },
    });
  });

  it("matches candidate payee by normalized payee name when entity id is unavailable", () => {
    const result = aggregateVermontOutsideSpending({
      candidateName: "Phil Scott",
      electionYear: 2024,
      expenditureRows: [expenditure({ entityId: null, sourceName: "SCOTT, PHIL", candidateFirstName: null, candidateLastName: null })],
    });

    expect(result.includedExpenditureRowCount).toBe(1);
    expect(result.summary.groups).toEqual([
      expect.objectContaining({
        filerRegistrationGuid: "pac-guid",
        supportMechanism: "vt_pac_contribution_to_registrant",
      }),
    ]);
  });

  it("skips ordinary PAC expenses, candidate campaign expenses, wrong payees, and non-contribution categories", () => {
    const result = aggregateVermontOutsideSpending({
      candidateName: "Phil Scott",
      candidateEntityId: 33545,
      electionYear: 2024,
      expenditureRows: [
        expenditure({ transactionAmount: 500 }),
        expenditure({ transactionId: 11, guid: "bad-year", electionYear: 2022 }),
        expenditure({ transactionId: 12, guid: "candidate-filer", filerTypeCode: "CAN", filerTypeDescription: "Candidate" }),
        expenditure({ transactionId: 13, guid: "vendor-payee", payeeType: "Business/Group/Organization" }),
        expenditure({
          transactionId: 14,
          guid: "media-category",
          transactionCategoryCode: "PUMEDIA",
          transactionCategoryDescription: "Media Buy",
          expenditurePurpose: "Media Buy",
        }),
        expenditure({ transactionId: 15, guid: "refund", transactionCategoryDescription: "Returned Contribution to Candidate" }),
        expenditure({ transactionId: 16, guid: "zero", transactionAmount: 0 }),
        expenditure({ transactionId: 17, guid: "other-candidate", entityId: 999, sourceName: "DOE, JANE", candidateFirstName: "JANE", candidateLastName: "DOE" }),
      ],
    });

    expect(result.matchedExpenditureRowCount).toBe(7);
    expect(result.includedExpenditureRowCount).toBe(1);
    expect(result.skippedExpenditureRowCount).toBe(6);
    expect(result.summary.outsideSupportTotal).toBe(500);
  });

  it("limits groups and rejects invalid inputs", () => {
    const result = aggregateVermontOutsideSpending({
      candidateName: "Phil Scott",
      electionYear: 2024,
      maxGroups: 1,
      expenditureRows: [
        expenditure({ filerRegistrationGuid: "small-pac", filerName: "SMALL PAC", transactionAmount: 100 }),
        expenditure({ filerRegistrationGuid: "big-pac", filerName: "BIG PAC", transactionAmount: 1000 }),
      ],
    });

    expect(result.summary.groups).toEqual([expect.objectContaining({ filerRegistrationGuid: "big-pac", amount: 1000 })]);
    expect(() =>
      aggregateVermontOutsideSpending({ candidateName: "Phil Scott", electionYear: 1999, expenditureRows: [] })
    ).toThrow("Invalid Vermont outside spending aggregation election year");
    expect(() =>
      aggregateVermontOutsideSpending({
        candidateName: "Phil Scott",
        candidateEntityId: -1,
        electionYear: 2024,
        expenditureRows: [],
      })
    ).toThrow("candidateEntityId");
  });
});
