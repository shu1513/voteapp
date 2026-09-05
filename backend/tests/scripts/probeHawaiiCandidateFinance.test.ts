import { afterEach, describe, expect, it, vi } from "vitest";

import {
  parseProbeHawaiiCandidateFinanceArgs,
  runProbeHawaiiCandidateFinance,
} from "../../src/scripts/probeHawaiiCandidateFinance.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("probeHawaiiCandidateFinance script", () => {
  it("parses live probe options without exposing the app token in output args", async () => {
    vi.stubEnv("HAWAII_CSC_APP_TOKEN", "env-token");

    const args = parseProbeHawaiiCandidateFinanceArgs([
      "--candidate-name=Josh Green",
      "--year",
      "2022",
      "--office=Governor",
      "--scope=statewide",
      "--limit=3",
      "--funder-limit=7",
      "--min-industry-amount=25000",
      "--timeout-ms=5000",
    ]);

    expect(args).toEqual({
      candidateName: "Josh Green",
      electionYear: 2022,
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
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
        candidateNameNormalized: "JOSH GREEN",
        officeNameNormalized: "Governor",
      })),
    };

    const output = await runProbeHawaiiCandidateFinance({
      args,
      client,
      now: new Date("2026-06-22T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "hawaii_candidate_finance_live_probe",
      ts: "2026-06-22T12:00:00.000Z",
      ok: false,
      args: {
        candidateName: "Josh Green",
        electionYear: 2022,
        appTokenProvided: true,
      },
    });
    expect(output.args).not.toHaveProperty("appToken");
    expect(client.resolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Josh Green", officeName: "Governor", electionYear: 2022 }),
      expect.objectContaining({ appToken: "env-token", timeoutMs: 5000 })
    );
  });

  it("builds a no-write probe summary with direct occupations and outside industry backtrace", async () => {
    const args = parseProbeHawaiiCandidateFinanceArgs([
      "--candidate-name=Josh Green",
      "--year=2022",
      "--office=Governor",
      "--limit=5",
      "--min-industry-amount=25000",
    ]);
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        committeeId: "CC10174",
        committeeName: "Green, Josh",
        electionPeriod: "2020-2022 (KP2)",
        totalAmount: 4070153.38,
        confidence: "exact" as const,
        source: "csc_api" as const,
        sourceUrl: "https://hicscdata.hawaii.gov/dataset/Campaign-Contributions-Received-By-Hawaii-State-an/jexd-xbcg",
        matchedSummaryRowCount: 1,
      })),
      getDirectOccupationAggregates: vi.fn(async () => [
        {
          categoryName: "Attorney",
          amount: 332962.31,
          count: 1200,
        },
      ]),
      getContributionSizeAggregates: vi.fn(async () => [
        {
          categoryName: "1000_4999",
          amount: 150000,
          count: 30,
        },
      ]),
      getIndependentExpenditureGroups: vi.fn(async () => [
        {
          committeeId: "NC20760",
          committeeName: "Be Change Now",
          supportOppose: "support" as const,
          amount: 500557,
          expenditureCount: 1,
          electionPeriod: "2020-2022 (KP2)",
        },
        {
          committeeId: "NC99999",
          committeeName: "Hawaii Future PAC",
          supportOppose: "oppose" as const,
          amount: 10000,
          expenditureCount: 1,
          electionPeriod: "2020-2022 (KP2)",
        },
      ]),
      getNoncandidateCommitteeFunders: vi.fn(async (input: { committeeId: string }) =>
        input.committeeId === "NC20760"
          ? [
              {
                categoryName: "Hawaii Construction Builders Fund",
                amount: 2086436.92,
                count: 1,
              },
              {
                categoryName: "Small Unclassified Donor",
                amount: 100,
                count: 1,
              },
            ]
          : []
      ),
    };

    const output = await runProbeHawaiiCandidateFinance({
      args,
      client,
      now: new Date("2026-06-22T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "hawaii_candidate_finance_live_probe",
      ok: true,
      direct_campaign: {
        top_occupations: [
          {
            category_name: "Attorney",
            amount: 332962.31,
            contributor_count: 1200,
          },
        ],
        contribution_size_buckets: [
          {
            category_name: "1000_4999",
            amount: 150000,
            contributor_count: 30,
          },
        ],
      },
      outside_spending: {
        top_supporting_groups: [
          {
            committee_id: "NC20760",
            committee_name: "Be Change Now",
            support_oppose: "support",
            amount: 500557,
          },
        ],
        top_opposing_groups: [
          {
            committee_id: "NC99999",
            committee_name: "Hawaii Future PAC",
            support_oppose: "oppose",
            amount: 10000,
          },
        ],
        top_supporting_industries: [
          {
            category_name: "construction",
            industry_slug: "construction",
            support_oppose: "support",
            amount: 2086436.92,
            contributor_count: 1,
            evidence: [
              {
                organization_name: "Hawaii Construction Builders Fund",
                amount: 2086436.92,
                committee_id: "NC20760",
                committee_name: "Be Change Now",
              },
            ],
          },
        ],
        top_opposing_industries: [],
        skipped_outside_funder_lookup_count: 0,
      },
    });
    expect(client.getIndependentExpenditureGroups).toHaveBeenCalledWith(
      expect.objectContaining({ candidateName: "Josh Green", electionYear: 2022 }),
      expect.objectContaining({ timeoutMs: 30000 })
    );
    expect(client.getNoncandidateCommitteeFunders).toHaveBeenCalledWith(
      expect.objectContaining({ committeeId: "NC20760", electionPeriod: "2020-2022 (KP2)" }),
      expect.objectContaining({ timeoutMs: 30000 })
    );
  });

  it("rejects malformed required options", () => {
    expect(() => parseProbeHawaiiCandidateFinanceArgs(["--year=2022", "--office=Governor"])).toThrow(
      "Missing required --candidate-name"
    );
    expect(() =>
      parseProbeHawaiiCandidateFinanceArgs(["--candidate-name=Josh Green", "--year=2022", "--office=Governor", "--scope=city"])
    ).toThrow("Invalid --scope value");
    expect(() =>
      parseProbeHawaiiCandidateFinanceArgs(["--candidate-name=Josh Green", "--year=2022x", "--office=Governor"])
    ).toThrow("Invalid --year value");
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() =>
      parseProbeHawaiiCandidateFinanceArgs([
        "--candidate-name=Josh Green",
        "--year=2022",
        "--office=Governor",
        "--limit=9007199254740993",
      ])
    ).toThrow("Invalid --limit value: 9007199254740993");
  });
});
