import { describe, expect, it, vi } from "vitest";

import {
  parseProbeArizonaCandidateFinanceArgs,
  runProbeArizonaCandidateFinance,
} from "../../src/scripts/probeArizonaCandidateFinance.js";

describe("probeArizonaCandidateFinance script", () => {
  it("parses live probe options", () => {
    expect(
      parseProbeArizonaCandidateFinanceArgs([
        "--candidate-name=Katie Hobbs",
        "--year",
        "2024",
        "--office=Governor",
        "--committee-id=201600105",
        "--candidate-filer-id=201600105",
        "--limit=3",
        "--resolution-limit=25",
        "--income-limit=100",
        "--ie-limit=50",
        "--outside-income-limit=75",
        "--outside-max-groups=4",
        "--min-industry-amount=25000",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      candidateName: "Katie Hobbs",
      electionYear: 2024,
      officeName: "Governor",
      committeeId: "201600105",
      candidateFilerId: "201600105",
      limit: 3,
      resolutionLimit: 25,
      incomeLimit: 100,
      independentExpenditureLimit: 50,
      outsideIncomeLimit: 75,
      outsideMaxGroups: 4,
      minIndustryAmount: 25000,
      timeoutMs: 5000,
    });
  });

  it("returns an unmatched no-write result when committee resolution finds no rows", async () => {
    const args = parseProbeArizonaCandidateFinanceArgs([
      "--candidate-name=No Match",
      "--year=2024",
      "--office=Governor",
    ]);
    const client = {
      searchIncomeTransactions: vi.fn(async () => []),
      searchIndependentExpenditures: vi.fn(async () => []),
    };

    const output = await runProbeArizonaCandidateFinance({
      args,
      client,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "arizona_candidate_finance_live_probe",
      ts: "2026-06-25T12:00:00.000Z",
      ok: false,
      resolution: {
        status: "unmatched",
        reason: "no_income_rows",
        candidateNameNormalized: "NO MATCH",
        officeNameNormalized: "GOVERNOR",
      },
      fetched: null,
    });
    expect(client.searchIncomeTransactions).toHaveBeenCalledWith(
      expect.objectContaining({ electionYear: 2024, filerName: "No Match", limit: 100 }),
      expect.objectContaining({ timeoutMs: 30000 })
    );
    expect(client.searchIndependentExpenditures).not.toHaveBeenCalled();
  });

  it("builds a live probe summary with direct occupations and outside industry evidence", async () => {
    const args = parseProbeArizonaCandidateFinanceArgs([
      "--candidate-name=Katie Hobbs",
      "--year=2024",
      "--office=Governor",
      "--limit=5",
      "--min-industry-amount=25000",
    ]);
    const client = {
      searchIncomeTransactions: vi.fn(async (input: { filerName?: string | null; filerId?: string | null }) => {
        if (input.filerName) {
          return [
            {
              transactionDate: "2024-01-01",
              committeeId: "AZ100",
              committeeName: "Katie Hobbs for Governor",
              amount: 100,
              transactionName: "Small Donor",
              transactionType: "Individual Contribution",
              occupation: "Teacher",
              sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
            },
          ];
        }
        if (input.filerId === "AZ100") {
          return [
            {
              transactionDate: "2024-02-01",
              committeeId: "AZ100",
              committeeName: "Katie Hobbs for Governor",
              amount: 1000,
              transactionName: "Taylor Example",
              transactionType: "Individual Contribution",
              occupation: "Attorney",
              sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
            },
          ];
        }
        if (input.filerId === "AZPAC1") {
          return [
            {
              transactionDate: "2024-03-01",
              committeeId: "AZPAC1",
              committeeName: "Arizona Progress PAC",
              amount: 50000,
              transactionName: "Desert AI Labs LLC",
              transactionType: "Contribution",
              sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
            },
          ];
        }
        return [];
      }),
      searchIndependentExpenditures: vi.fn(
        async (input: { position?: "Support" | "Oppose" | "Both" | null }) =>
          input.position === "Support"
            ? [
                {
                  transactionDate: "2024-04-01",
                  committeeId: "AZPAC1",
                  committeeName: "Arizona Progress PAC",
                  amount: 40000,
                  transactionName: "Mail",
                  transactionType: "Independent Expenditure",
                  supportOppose: "Support" as const,
                  sourceUrl: "https://seethemoney.az.gov/Reporting/Explore",
                },
              ]
            : []
      ),
    };

    const output = await runProbeArizonaCandidateFinance({
      args,
      client,
      now: new Date("2026-06-25T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      ok: true,
      resolution: {
        status: "matched",
        committeeId: "AZ100",
        committeeName: "Katie Hobbs for Governor",
        candidateFilerId: "AZ100",
      },
      direct_campaign: {
        top_occupations: [
          {
            category_name: "Attorney",
            amount: 1000,
            contributor_count: 1,
          },
        ],
        contribution_size_buckets: [
          {
            category_name: "$1,000-$4,999",
            amount: 1000,
            contributor_count: 1,
          },
        ],
      },
      outside_spending: {
        top_supporting_groups: [
          {
            committee_id: "AZPAC1",
            committee_name: "Arizona Progress PAC",
            support_oppose: "support",
            amount: 40000,
          },
        ],
        top_opposing_groups: [],
        top_supporting_industries: [
          {
            category_name: "technology",
            industry_slug: "technology",
            support_oppose: "support",
            amount: 50000,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "Desert AI Labs LLC",
                amount: 50000,
                committee_id: "AZPAC1",
                committee_name: "Arizona Progress PAC",
              },
            ],
          },
        ],
      },
      fetched: {
        directIncomeTransactionCount: 1,
        supportIndependentExpenditureCount: 1,
        opposeIndependentExpenditureCount: 0,
        outsideGroupIncomeTransactionCount: 1,
        outsideGroupIncomeCommitteeCount: 1,
      },
    });
  });

  it("rejects malformed required options", () => {
    expect(() => parseProbeArizonaCandidateFinanceArgs(["--year=2024", "--office=Governor"])).toThrow(
      "Missing required --candidate-name"
    );
    expect(() =>
      parseProbeArizonaCandidateFinanceArgs(["--candidate-name=Katie Hobbs", "--year=2024x", "--office=Governor"])
    ).toThrow("Invalid --year value");
    expect(() =>
      parseProbeArizonaCandidateFinanceArgs([
        "--candidate-name=Katie Hobbs",
        "--year=2024",
        "--office=Governor",
        "--limit=0",
      ])
    ).toThrow("Invalid --limit value");
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() =>
      parseProbeArizonaCandidateFinanceArgs([
        "--candidate-name=Katie Hobbs",
        "--year=2024",
        "--office=Governor",
        "--limit=9007199254740993",
      ])
    ).toThrow("Invalid --limit value: 9007199254740993");
  });
});
