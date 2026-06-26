import { describe, expect, it, vi } from "vitest";

import { buildArizonaCandidateFinanceSnapshot } from "../../../src/pipeline/arizonaFinance/arizonaCandidateFinanceSnapshot.js";
import type {
  ArizonaSpotlightIncomeTransaction,
  ArizonaSpotlightIndependentExpenditure,
} from "../../../src/pipeline/arizonaFinance/arizonaSpotlightClient.js";

function income(overrides: Partial<ArizonaSpotlightIncomeTransaction> = {}): ArizonaSpotlightIncomeTransaction {
  return {
    transactionDate: "2023-03-28",
    committeeId: "201800057",
    committeeName: "Elect Katie Hobbs",
    amount: 250,
    transactionName: "Doe, Jane",
    transactionType: "Contribution from Individuals",
    occupation: "Teacher",
    employer: "Phoenix High School District",
    city: "Phoenix",
    state: "AZ",
    zipCode: "85001",
    sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=Income",
    ...overrides,
  };
}

function expenditure(
  overrides: Partial<ArizonaSpotlightIndependentExpenditure> = {}
): ArizonaSpotlightIndependentExpenditure {
  return {
    transactionDate: "2023-12-22",
    committeeId: "201000285",
    committeeName: "Toa Pac",
    amount: 5400,
    transactionName: "Elect Katie Hobbs",
    transactionType: "Ind. Expend. (Non-Recall) - cash",
    supportOppose: "Support",
    sourceUrl: "https://seethemoney.az.gov/Reporting/AdvancedSearch/?CategoryType=IndependentExpenditures",
    ...overrides,
  };
}

describe("arizonaCandidateFinanceSnapshot", () => {
  it("fetches candidate income, outside spending, and outside-group income into one snapshot", async () => {
    const searchIncomeTransactions = vi
      .fn()
      .mockResolvedValueOnce([
        income({ amount: 100, occupation: "Teacher" }),
        income({
          amount: 500,
          occupation: "Attorney",
          transactionName: "Roe, John",
          zipCode: "85002",
        }),
      ])
      .mockResolvedValueOnce([
        income({
          committeeId: "201000285",
          committeeName: "Toa Pac",
          amount: 300,
          transactionName: "Ogorchock, Jace",
          occupation: "Teacher",
          employer: "Phoenix High School District",
        }),
      ])
      .mockResolvedValueOnce([]);
    const searchIndependentExpenditures = vi
      .fn()
      .mockResolvedValueOnce([expenditure({ amount: 5400 })])
      .mockResolvedValueOnce([
        expenditure({
          committeeId: "9001",
          committeeName: "Oppose Committee",
          amount: 250,
          supportOppose: "Oppose",
        }),
      ]);

    const result = await buildArizonaCandidateFinanceSnapshot({
      candidateName: "Katie Hobbs",
      candidateCommitteeId: "201800057",
      electionYear: 2024,
      directIncomeLimit: 10,
      independentExpenditureLimitPerPosition: 5,
      outsideGroupIncomeLimitPerGroup: 7,
      spotlightClient: {
        searchIncomeTransactions,
        searchIndependentExpenditures,
      },
    });

    expect(searchIncomeTransactions).toHaveBeenCalledTimes(3);
    expect(searchIncomeTransactions.mock.calls[0]?.[0]).toEqual({
      electionYear: 2024,
      filerId: "201800057",
      limit: 10,
    });
    expect(searchIncomeTransactions.mock.calls[1]?.[0]).toEqual({
      electionYear: 2024,
      filerId: "201000285",
      limit: 7,
    });
    expect(searchIncomeTransactions.mock.calls[2]?.[0]).toEqual({
      electionYear: 2024,
      filerId: "9001",
      limit: 7,
    });
    expect(searchIndependentExpenditures.mock.calls.map((call) => call[0])).toEqual([
      {
        electionYear: 2024,
        candidateName: "Katie Hobbs",
        candidateFilerId: "201800057",
        position: "Support",
        limit: 5,
      },
      {
        electionYear: 2024,
        candidateName: "Katie Hobbs",
        candidateFilerId: "201800057",
        position: "Oppose",
        limit: 5,
      },
    ]);

    expect(result).toMatchObject({
      candidateName: "Katie Hobbs",
      candidateCommitteeId: "201800057",
      candidateFilerId: "201800057",
      electionYear: 2024,
      fetched: {
        directIncomeTransactionCount: 2,
        supportIndependentExpenditureCount: 1,
        opposeIndependentExpenditureCount: 1,
        outsideGroupIncomeTransactionCount: 1,
        outsideGroupIncomeCommitteeCount: 2,
      },
      directFinance: {
        summary: {
          totalReceipts: 600,
          directContributionTotal: 600,
        },
      },
      outsideSpending: {
        summary: {
          supportTotal: 5400,
          opposeTotal: 250,
          groups: [
            expect.objectContaining({ committeeId: "201000285", supportOppose: "support", amount: 5400 }),
            expect.objectContaining({ committeeId: "9001", supportOppose: "oppose", amount: 250 }),
          ],
        },
      },
    });
    expect(result.directFinance.directBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "occupation", categoryName: "Attorney", amount: 500 }),
        expect.objectContaining({ categoryType: "occupation", categoryName: "Teacher", amount: 100 }),
      ])
    );
    expect(result.outsideGroupContributions.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          committeeId: "201000285",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Ogorchock, Jace",
          amount: 300,
        }),
        expect.objectContaining({
          committeeId: "201000285",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "education",
          amount: 300,
        }),
      ])
    );
  });

  it("uses an explicit candidate filer id for independent expenditure searches", async () => {
    const searchIncomeTransactions = vi.fn().mockResolvedValue([]);
    const searchIndependentExpenditures = vi.fn().mockResolvedValue([]);

    await buildArizonaCandidateFinanceSnapshot({
      candidateName: "Candidate",
      candidateCommitteeId: "COMMITTEE-1",
      candidateFilerId: "FILER-2",
      electionYear: 2024,
      spotlightClient: {
        searchIncomeTransactions,
        searchIndependentExpenditures,
      },
    });

    expect(searchIndependentExpenditures.mock.calls.map((call) => call[0]?.candidateFilerId)).toEqual([
      "FILER-2",
      "FILER-2",
    ]);
  });

  it("can build a direct-only snapshot without outside calls", async () => {
    const searchIncomeTransactions = vi.fn().mockResolvedValue([
      income({
        amount: 100,
        occupation: "Teacher",
      }),
    ]);
    const searchIndependentExpenditures = vi.fn().mockResolvedValue([expenditure()]);

    const result = await buildArizonaCandidateFinanceSnapshot({
      candidateName: "Katie Hobbs",
      candidateCommitteeId: "201800057",
      electionYear: 2024,
      includeOutside: false,
      spotlightClient: {
        searchIncomeTransactions,
        searchIndependentExpenditures,
      },
    });

    expect(searchIncomeTransactions).toHaveBeenCalledTimes(1);
    expect(searchIndependentExpenditures).not.toHaveBeenCalled();
    expect(result.outsideSpending.summary).toBeNull();
    expect(result.outsideGroupContributions.outsideGroupBreakdowns).toEqual([]);
    expect(result.fetched).toMatchObject({
      directIncomeTransactionCount: 1,
      supportIndependentExpenditureCount: 0,
      opposeIndependentExpenditureCount: 0,
      outsideGroupIncomeTransactionCount: 0,
      outsideGroupIncomeCommitteeCount: 0,
    });
  });

  it("does not fetch outside-group income when no outside groups are found", async () => {
    const searchIncomeTransactions = vi.fn().mockResolvedValueOnce([income()]);
    const searchIndependentExpenditures = vi.fn().mockResolvedValue([]);

    const result = await buildArizonaCandidateFinanceSnapshot({
      candidateName: "Katie Hobbs",
      candidateCommitteeId: "201800057",
      electionYear: 2024,
      spotlightClient: {
        searchIncomeTransactions,
        searchIndependentExpenditures,
      },
    });

    expect(searchIncomeTransactions).toHaveBeenCalledTimes(1);
    expect(result.outsideSpending.summary).toBeNull();
  });

  it("validates inputs and limits", async () => {
    await expect(
      buildArizonaCandidateFinanceSnapshot({
        candidateName: "",
        candidateCommitteeId: "1",
        electionYear: 2024,
      })
    ).rejects.toThrow("Arizona candidate name is required");
    await expect(
      buildArizonaCandidateFinanceSnapshot({
        candidateName: "Candidate",
        candidateCommitteeId: "",
        electionYear: 2024,
      })
    ).rejects.toThrow("Arizona candidate committee id is required");
    await expect(
      buildArizonaCandidateFinanceSnapshot({
        candidateName: "Candidate",
        candidateCommitteeId: "1",
        electionYear: 2001,
      })
    ).rejects.toThrow("Invalid Arizona candidate finance snapshot election year");
    await expect(
      buildArizonaCandidateFinanceSnapshot({
        candidateName: "Candidate",
        candidateCommitteeId: "1",
        electionYear: 2024,
        directIncomeLimit: 0,
      })
    ).rejects.toThrow("Invalid Arizona candidate finance snapshot directIncomeLimit");
  });
});
