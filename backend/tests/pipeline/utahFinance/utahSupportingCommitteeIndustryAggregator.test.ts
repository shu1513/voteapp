import { describe, expect, it, vi } from "vitest";

import {
  aggregateUtahSupportingCommitteeIndustries,
} from "../../../src/pipeline/utahFinance/utahSupportingCommitteeIndustryAggregator.js";
import type { UtahDisclosuresTransactionRow } from "../../../src/pipeline/utahFinance/utahDisclosuresClient.js";

const CANDIDATE_SOURCE_URL = "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport/98765?ReportYear=2024";
const PAC_SOURCE_URL = "https://disclosures.utah.gov/Search/AdvancedSearch/GenerateReport?ReportYear=2024&EntityType=PAC";

function transaction(overrides: Partial<UtahDisclosuresTransactionRow> = {}): UtahDisclosuresTransactionRow {
  return {
    filed: "01/05/2024",
    entityType: "PCC",
    entityName: "Friends of Jane Doe",
    report: "Year End",
    transactionId: "T100",
    transactionType: "Contribution",
    transactionDate: "01/02/2024",
    amount: 100,
    name: "John Smith",
    address1: "1 Main",
    city: "Salt Lake City",
    state: "UT",
    zip: "84111",
    inKind: false,
    loan: false,
    ...overrides,
  };
}

describe("utahSupportingCommitteeIndustryAggregator", () => {
  it("aggregates supporting PACs and classifies organization donors into industries", async () => {
    const result = await aggregateUtahSupportingCommitteeIndustries({
      electionYear: 2024,
      candidateCommitteeName: "Friends of Jane Doe",
      candidateSourceUrl: CANDIDATE_SOURCE_URL,
      committeeSourceUrl: PAC_SOURCE_URL,
      minIndustryAmount: 5_000,
      candidateTransactions: [
        transaction({ transactionId: "candidate-pac-1", amount: 2_500, name: "Utah Builders PAC" }),
        transaction({ transactionId: "candidate-pac-2", amount: 500, name: "UTAH BUILDERS POLITICAL ACTION COMMITTEE" }),
        transaction({ transactionId: "individual", amount: 1_000, name: "John Smith" }),
      ],
      committeeTransactions: [
        transaction({
          entityType: "PAC",
          entityName: "Utah Builders Political Action Committee",
          transactionId: "pac-donor-1",
          amount: 15_000,
          name: "Wasatch Construction LLC",
        }),
        transaction({
          entityType: "PAC",
          entityName: "UTAH BUILDERS PAC",
          transactionId: "pac-donor-2",
          amount: 10_000,
          name: "Mountain Builders Association",
        }),
        transaction({
          entityType: "PAC",
          entityName: "UTAH BUILDERS PAC",
          transactionId: "pac-individual",
          amount: 20_000,
          name: "Jane Roe",
        }),
      ],
    });

    expect(result).toEqual({
      supportingCommittees: [
        {
          committeeName: "Utah Builders PAC",
          amount: 3000,
          contributorCount: 2,
          sourceUrl: CANDIDATE_SOURCE_URL,
        },
      ],
      supportingCommitteeIndustryBreakdowns: [
        {
          supportingCommitteeName: "Utah Builders PAC",
          industrySlug: "construction",
          amount: 25000,
          contributorCount: 2,
          sourceUrl: PAC_SOURCE_URL,
        },
      ],
      matchedCommitteeTransactionRowCount: 3,
      includedOrganizationDonorRowCount: 2,
      skippedCommitteeTransactionRowCount: 1,
    });
  });

  it("uses the minimum industry amount threshold", async () => {
    const result = await aggregateUtahSupportingCommitteeIndustries({
      electionYear: 2024,
      minIndustryAmount: 25_000,
      candidateTransactions: [transaction({ amount: 1_000, name: "Utah Energy PAC" })],
      committeeTransactions: [
        transaction({
          entityType: "PAC",
          entityName: "Utah Energy PAC",
          amount: 24_999,
          name: "Midland Energy",
        }),
      ],
    });

    expect(result.supportingCommittees).toEqual([
      expect.objectContaining({ committeeName: "Utah Energy PAC", amount: 1000 }),
    ]);
    expect(result.supportingCommitteeIndustryBreakdowns).toEqual([]);
  });

  it("uses a $5k default industry threshold for Utah supporting committee enrichment", async () => {
    const result = await aggregateUtahSupportingCommitteeIndustries({
      electionYear: 2024,
      candidateTransactions: [transaction({ amount: 1_000, name: "Utah Energy PAC" })],
      committeeTransactions: [
        transaction({
          entityType: "PAC",
          entityName: "Utah Energy PAC",
          amount: 6_000,
          name: "Midland Energy",
        }),
      ],
    });

    expect(result.supportingCommitteeIndustryBreakdowns).toEqual([
      expect.objectContaining({
        supportingCommitteeName: "Utah Energy PAC",
        industrySlug: "oil_gas_energy",
        amount: 6000,
      }),
    ]);
  });

  it("can classify unknown organization donors with optional AI", async () => {
    const financeIndustryClassifier = vi.fn(async () => [
      {
        rawLabel: "Civic Future Alliance",
        labelType: "donor" as const,
        normalizedLabel: "CIVIC FUTURE ALLIANCE",
        industrySlug: "environmental_group" as const,
        confidence: "medium" as const,
        classificationSource: "ai" as const,
        matchedRule: null,
      },
    ]);

    const result = await aggregateUtahSupportingCommitteeIndustries({
      electionYear: 2024,
      minIndustryAmount: 5_000,
      financeIndustryClassifier,
      candidateTransactions: [transaction({ amount: 1_000, name: "Clean Air PAC" })],
      committeeTransactions: [
        transaction({
          entityType: "PAC",
          entityName: "Clean Air PAC",
          amount: 7_500,
          name: "Civic Future Alliance",
        }),
      ],
    });

    expect(financeIndustryClassifier).toHaveBeenCalledWith({
      labels: [
        {
          rawLabel: "Civic Future Alliance",
          labelType: "donor",
          normalizedLabel: "CIVIC FUTURE ALLIANCE",
          amount: 7500,
        },
      ],
    });
    expect(result.supportingCommitteeIndustryBreakdowns).toEqual([
      expect.objectContaining({
        supportingCommitteeName: "Clean Air PAC",
        industrySlug: "environmental_group",
        amount: 7500,
      }),
    ]);
  });

  it("does not call AI when industry AI classification is disabled", async () => {
    const financeIndustryClassifier = vi.fn(async () => []);

    const result = await aggregateUtahSupportingCommitteeIndustries({
      electionYear: 2024,
      minIndustryAmount: 5_000,
      financeIndustryClassifier,
      classifyIndustriesWithAi: false,
      candidateTransactions: [transaction({ amount: 1_000, name: "Clean Air PAC" })],
      committeeTransactions: [
        transaction({
          entityType: "PAC",
          entityName: "Clean Air PAC",
          amount: 7_500,
          name: "Civic Future Alliance",
        }),
      ],
    });

    expect(financeIndustryClassifier).not.toHaveBeenCalled();
    expect(result.supportingCommitteeIndustryBreakdowns).toEqual([]);
  });

  it("does not emit industry breakdowns for committees excluded by the max committee limit", async () => {
    const result = await aggregateUtahSupportingCommitteeIndustries({
      electionYear: 2024,
      maxSupportingCommittees: 1,
      minIndustryAmount: 5_000,
      candidateTransactions: [
        transaction({ transactionId: "candidate-energy", amount: 2_000, name: "Utah Energy PAC" }),
        transaction({ transactionId: "candidate-builders", amount: 1_000, name: "Utah Builders PAC" }),
      ],
      committeeTransactions: [
        transaction({
          entityType: "PAC",
          entityName: "Utah Builders PAC",
          transactionId: "builders-donor",
          amount: 25_000,
          name: "Wasatch Construction LLC",
        }),
        transaction({
          entityType: "PAC",
          entityName: "Utah Energy PAC",
          transactionId: "energy-donor",
          amount: 8_000,
          name: "Midland Energy",
        }),
      ],
    });

    expect(result.supportingCommittees).toEqual([expect.objectContaining({ committeeName: "Utah Energy PAC" })]);
    expect(result.supportingCommitteeIndustryBreakdowns).toEqual([
      expect.objectContaining({
        supportingCommitteeName: "Utah Energy PAC",
        industrySlug: "oil_gas_energy",
        amount: 8000,
      }),
    ]);
    expect(result.matchedCommitteeTransactionRowCount).toBe(1);
    expect(result.includedOrganizationDonorRowCount).toBe(1);
  });

  it("validates limits and election years", async () => {
    await expect(
      aggregateUtahSupportingCommitteeIndustries({
        electionYear: 1997,
        candidateTransactions: [],
        committeeTransactions: [],
      })
    ).rejects.toThrow("Invalid Utah supporting committee industry election year");

    await expect(
      aggregateUtahSupportingCommitteeIndustries({
        electionYear: 2024,
        candidateTransactions: [],
        committeeTransactions: [],
        maxSupportingCommittees: 0,
      })
    ).rejects.toThrow("maxSupportingCommittees");
  });
});
