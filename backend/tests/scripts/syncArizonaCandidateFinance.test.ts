import { describe, expect, it } from "vitest";

import type { ArizonaCandidateFinanceSnapshot } from "../../src/pipeline/arizonaFinance/arizonaCandidateFinanceSnapshot.js";
import {
  parseSyncArizonaCandidateFinanceScriptArgs,
  toSyncArizonaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncArizonaCandidateFinance.js";

describe("syncArizonaCandidateFinance script", () => {
  it("parses required and optional flags", () => {
    expect(
      parseSyncArizonaCandidateFinanceScriptArgs([
        "--candidate-name=Katie Hobbs",
        "--committee-id",
        "201600105",
        "--candidate-filer-id=201600105",
        "--year=2024",
        "--force",
        "--timeout-ms=5000",
        "--direct-limit=100",
        "--ie-limit=50",
        "--outside-income-limit=75",
        "--outside-max-groups=5",
        "--direct-max-breakdowns=10",
        "--outside-max-breakdowns=8",
        "--min-industry-amount=25000.50",
      ])
    ).toEqual({
      candidateName: "Katie Hobbs",
      candidateCommitteeId: "201600105",
      candidateFilerId: "201600105",
      electionYear: 2024,
      includeOutside: true,
      force: true,
      timeoutMs: 5000,
      directIncomeLimit: 100,
      independentExpenditureLimitPerPosition: 50,
      outsideGroupIncomeLimitPerGroup: 75,
      outsideMaxGroups: 5,
      directMaxBreakdownsPerCategory: 10,
      outsideMaxBreakdownsPerCategory: 8,
      minIndustryAmount: 25000.5,
    });
  });

  it("defaults optional flags and can skip outside spending", () => {
    expect(
      parseSyncArizonaCandidateFinanceScriptArgs([
        "--candidate-name=Katie Hobbs",
        "--committee-id=201600105",
        "--year=2024",
        "--skip-outside",
      ])
    ).toMatchObject({
      candidateName: "Katie Hobbs",
      candidateCommitteeId: "201600105",
      candidateFilerId: undefined,
      electionYear: 2024,
      includeOutside: false,
      force: false,
      timeoutMs: undefined,
    });
  });

  it("rejects malformed flags strictly", () => {
    expect(() => parseSyncArizonaCandidateFinanceScriptArgs(["--year=2024"])).toThrow(
      "Missing required --candidate-name flag"
    );
    expect(() =>
      parseSyncArizonaCandidateFinanceScriptArgs([
        "--candidate-name=Katie Hobbs",
        "--committee-id=201600105",
        "--year=20x4",
      ])
    ).toThrow("Invalid --year value");
    expect(() =>
      parseSyncArizonaCandidateFinanceScriptArgs([
        "--candidate-name=Katie Hobbs",
        "--committee-id=201600105",
        "--year=2024",
        "--direct-limit=0",
      ])
    ).toThrow("Invalid --direct-limit value");
    expect(() =>
      parseSyncArizonaCandidateFinanceScriptArgs([
        "--candidate-name=Katie Hobbs",
        "--committee-id=201600105",
        "--year=2024",
        "--min-industry-amount=-1",
      ])
    ).toThrow("Invalid --min-industry-amount value");
  });

  it("formats script output", () => {
    const snapshot: ArizonaCandidateFinanceSnapshot = {
      candidateName: "Katie Hobbs",
      candidateCommitteeId: "201600105",
      candidateFilerId: "201600105",
      electionYear: 2024,
      directFinance: {
        summary: {
          totalReceipts: 100,
          directContributionTotal: 100,
          sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
        },
        directBreakdowns: [
          {
            categoryType: "occupation",
            categoryName: "Attorney",
            amount: 100,
            contributorCount: 1,
            sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
          },
        ],
        matchedIncomeTransactionCount: 1,
        includedIncomeTransactionCount: 1,
        skippedIncomeTransactionCount: 0,
      },
      outsideSpending: {
        summary: {
          supportTotal: 250,
          opposeTotal: 0,
          groups: [
            {
              committeeId: "202400001",
              committeeName: "Arizona Future PAC",
              supportOppose: "support",
              amount: 250,
              expenditureCount: 1,
              sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
            },
          ],
          sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
        },
        matchedIndependentExpenditureCount: 1,
        includedIndependentExpenditureCount: 1,
        skippedIndependentExpenditureCount: 0,
      },
      outsideGroupContributions: {
        outsideGroupBreakdowns: [
          {
            committeeId: "202400001",
            supportOppose: "support",
            categoryType: "industry",
            categoryName: "real_estate",
            amount: 250,
            contributorCount: 1,
            sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
          },
        ],
        matchedIncomeTransactionCount: 1,
        includedIncomeTransactionCount: 1,
        skippedIncomeTransactionCount: 0,
      },
      fetched: {
        directIncomeTransactionCount: 1,
        supportIndependentExpenditureCount: 1,
        opposeIndependentExpenditureCount: 0,
        outsideGroupIncomeTransactionCount: 1,
        outsideGroupIncomeCommitteeCount: 1,
      },
    };

    const output = toSyncArizonaCandidateFinanceScriptOutput({
      startedAt: new Date("2026-06-25T12:00:00.000Z"),
      options: {
        candidateName: "Katie Hobbs",
        candidateCommitteeId: "201600105",
        candidateFilerId: "201600105",
        electionYear: 2024,
        includeOutside: true,
        force: false,
      },
      snapshot,
    });

    expect(output).toMatchObject({
      type: "arizona_candidate_finance_snapshot_sync",
      started_at: "2026-06-25T12:00:00.000Z",
      candidate_name: "Katie Hobbs",
      candidate_committee_id: "201600105",
      candidate_filer_id: "201600105",
      election_year: 2024,
      include_outside: true,
      snapshot: {
        candidateName: "Katie Hobbs",
        fetched: {
          directIncomeTransactionCount: 1,
          outsideGroupIncomeCommitteeCount: 1,
        },
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
