import { afterEach, describe, expect, it, vi } from "vitest";

import {
  parseProbeWashingtonCandidateFinanceArgs,
  runProbeWashingtonCandidateFinance,
} from "../../src/scripts/probeWashingtonCandidateFinance.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("probeWashingtonCandidateFinance script", () => {
  it("parses live probe options without exposing the app token in output args", async () => {
    vi.stubEnv("WASHINGTON_PDC_APP_TOKEN", "env-token");

    const args = parseProbeWashingtonCandidateFinanceArgs([
      "--candidate-name=Bob Ferguson",
      "--year",
      "2024",
      "--office=Governor",
      "--scope=statewide",
      "--limit=3",
      "--funder-limit=7",
      "--min-industry-amount=25000",
      "--timeout-ms=5000",
    ]);

    expect(args).toEqual({
      candidateName: "Bob Ferguson",
      electionYear: 2024,
      officeScope: "statewide",
      officeName: "Governor",
      legislativeDistrict: null,
      limit: 3,
      funderLimit: 7,
      minIndustryAmount: 25000,
      timeoutMs: 5000,
      appToken: "env-token",
    });

    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "unmatched" as const,
        reason: "no_candidate_committee_match" as const,
        candidateNameNormalized: "BOB FERGUSON",
        officeNameNormalized: "GOVERNOR",
      })),
    };

    const output = await runProbeWashingtonCandidateFinance({
      args,
      client,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "washington_candidate_finance_live_probe",
      ts: "2026-06-21T12:00:00.000Z",
      ok: false,
      args: {
        candidateName: "Bob Ferguson",
        electionYear: 2024,
        appTokenProvided: true,
      },
    });
    expect(output.args).not.toHaveProperty("appToken");
    expect(client.resolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Bob Ferguson", officeName: "Governor", electionYear: 2024 }),
      expect.objectContaining({ appToken: "env-token", timeoutMs: 5000 })
    );
  });

  it("builds a no-write probe summary with direct occupations and outside industry backtrace", async () => {
    const args = parseProbeWashingtonCandidateFinanceArgs([
      "--candidate-name=Bob Ferguson",
      "--year=2024",
      "--office=Governor",
      "--limit=5",
      "--min-industry-amount=25000",
    ]);
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        filerId: "FERGR *115",
        committeeId: "32311",
        committeeName: "Robert W. Ferguson (Bob Ferguson)",
        confidence: "exact" as const,
        source: "pdc_api" as const,
        sourceUrl: "https://data.wa.gov/resource/3h9x-7bvm.json",
        matchedSummaryRowCount: 1,
      })),
      getDirectOccupationAggregates: vi.fn(async () => [
        {
          categoryName: "ATTORNEY - LAWYER",
          amount: 719187.76,
          count: 1200,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
      ]),
      getContributionSizeAggregates: vi.fn(async () => [
        {
          categoryName: "1000_4999",
          amount: 100000,
          count: 20,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
      ]),
      getIndependentExpenditureGroups: vi.fn(async () => [
        {
          sponsorId: "FUSEV  147",
          sponsorName: "FUSE VOTES",
          supportOppose: "support" as const,
          amount: 2457.26,
          expenditureCount: 1,
          sourceUrl: "https://data.wa.gov/resource/67cp-h962.json",
        },
        {
          sponsorId: "WA24   101",
          sponsorName: "WASHINGTON 24",
          supportOppose: "oppose" as const,
          amount: 10000,
          expenditureCount: 1,
          sourceUrl: "https://data.wa.gov/resource/67cp-h962.json",
        },
      ]),
      getSponsorSummaryByName: vi.fn(async (input: { sponsorName: string }) =>
        input.sponsorName === "FUSE VOTES"
          ? [
              {
                filerId: "FUSEV  147",
                committeeId: "6708",
                filerName: "FUSE VOTES",
                electionYear: 2024,
                activeCandidate: false,
                hasReports: true,
                sourceUrl: "https://data.wa.gov/resource/3h9x-7bvm.json",
              },
            ]
          : []
      ),
      getSponsorOrganizationFunders: vi.fn(async () => [
        {
          categoryName: "Washington Conservation Action Votes",
          amount: 30000,
          count: 1,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
        {
          categoryName: "Small Unclassified Donor",
          amount: 100,
          count: 1,
          sourceUrl: "https://data.wa.gov/resource/kv7h-kjye.json",
        },
      ]),
    };

    const output = await runProbeWashingtonCandidateFinance({
      args,
      client,
      now: new Date("2026-06-21T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      ok: true,
      direct_campaign: {
        top_occupations: [
          {
            category_name: "ATTORNEY - LAWYER",
            amount: 719187.76,
            contributor_count: 1200,
          },
        ],
        contribution_size_buckets: [
          {
            category_name: "1000_4999",
            amount: 100000,
            contributor_count: 20,
          },
        ],
      },
      outside_spending: {
        top_supporting_groups: [
          {
            sponsor_id: "FUSEV  147",
            sponsor_name: "FUSE VOTES",
            support_oppose: "support",
            amount: 2457.26,
          },
        ],
        top_opposing_groups: [
          {
            sponsor_id: "WA24   101",
            sponsor_name: "WASHINGTON 24",
            support_oppose: "oppose",
            amount: 10000,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "environmental_group",
            industry_slug: "environmental_group",
            support_oppose: "support",
            amount: 30000,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "Washington Conservation Action Votes",
                amount: 30000,
                sponsor_id: "FUSEV  147",
                sponsor_name: "FUSE VOTES",
              },
            ],
          },
        ],
        top_opposing_industries: [],
        skipped_sponsor_funder_lookup_count: 1,
      },
    });
    expect(client.getIndependentExpenditureGroups).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Bob Ferguson", office: "GOVERNOR", electionYear: 2024 }),
      expect.objectContaining({ timeoutMs: 30000 })
    );
  });

  it("rejects malformed required options", () => {
    expect(() => parseProbeWashingtonCandidateFinanceArgs(["--year=2024", "--office=Governor"])).toThrow(
      "Missing required --candidate-name"
    );
    expect(() =>
      parseProbeWashingtonCandidateFinanceArgs(["--candidate-name=Bob Ferguson", "--year=2024", "--office=Governor", "--scope=city"])
    ).toThrow("Invalid --scope value");
    expect(() =>
      parseProbeWashingtonCandidateFinanceArgs(["--candidate-name=Bob Ferguson", "--year=2024x", "--office=Governor"])
    ).toThrow("Invalid --year value");
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() =>
      parseProbeWashingtonCandidateFinanceArgs([
        "--candidate-name=Bob Ferguson",
        "--year=2024",
        "--office=Governor",
        "--limit=9007199254740993",
      ])
    ).toThrow("Invalid --limit value: 9007199254740993");
  });
});
