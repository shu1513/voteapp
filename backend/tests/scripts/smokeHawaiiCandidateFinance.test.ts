import { describe, expect, it, vi } from "vitest";

import { runHawaiiCandidateFinanceLiveSmoke } from "../../src/scripts/smokeHawaiiCandidateFinance.js";

describe("smokeHawaiiCandidateFinance script", () => {
  it("passes the no-write Josh Green smoke checks when live-shape data is present", async () => {
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        committeeId: "CC10174",
        committeeName: "Green, Josh",
        electionPeriod: "2018-2022",
        totalAmount: 4070153.38,
        confidence: "exact" as const,
        source: "csc_api" as const,
        sourceUrl: "https://hicscdata.hawaii.gov/dataset/Campaign-Contributions-Received-By-Hawaii-State-an/jexd-xbcg",
        matchedSummaryRowCount: 1,
      })),
      getDirectOccupationAggregates: vi.fn(async () => [
        {
          categoryName: "Not Employed",
          amount: 354161.49,
          count: 538,
        },
        {
          categoryName: "Attorney",
          amount: 332495,
          count: 276,
        },
      ]),
      getContributionSizeAggregates: vi.fn(async () => [
        {
          categoryName: "1000_4999",
          amount: 2169593.55,
          count: 1353,
        },
      ]),
      getIndependentExpenditureGroups: vi.fn(async () => [
        {
          committeeId: "NC20760",
          committeeName: "Be Change Now",
          supportOppose: "support" as const,
          amount: 584819,
          expenditureCount: 37,
          electionPeriod: "2018-2022",
        },
        {
          committeeId: "NC20991",
          committeeName: "Victory Calls 2022",
          supportOppose: "oppose" as const,
          amount: 234000,
          expenditureCount: 28,
          electionPeriod: "2018-2022",
        },
      ]),
      getNoncandidateCommitteeFunders: vi.fn(async (input: { committeeId: string }) =>
        input.committeeId === "NC20760"
          ? [
              {
                categoryName: "Hawaii Carpenters Market Recovery Program Fund",
                amount: 2086436.92,
                count: 1,
              },
            ]
          : []
      ),
    };

    const output = await runHawaiiCandidateFinanceLiveSmoke({
      client,
      now: new Date("2026-06-23T12:00:00.000Z"),
    });

    expect(output).toMatchObject({
      type: "hawaii_candidate_finance_live_smoke",
      ts: "2026-06-23T12:00:00.000Z",
      ok: true,
      checks: [
        { name: "matched_josh_green_committee", passed: true },
        { name: "top_occupations_present", passed: true },
        { name: "attorney_occupation_present", passed: true },
        { name: "be_change_now_support_group_present", passed: true },
        { name: "construction_support_industry_present", passed: true },
        { name: "opposition_groups_present", passed: true },
      ],
      probe: {
        ok: true,
        outside_spending: {
          top_supporting_industries: [
            {
              industry_slug: "construction",
              evidence: [
                {
                  organization_name: "Hawaii Carpenters Market Recovery Program Fund",
                  committee_name: "Be Change Now",
                },
              ],
            },
          ],
        },
      },
    });
    expect(client.resolveCandidateCommittee).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateName: "Josh Green",
        electionYear: 2022,
        officeName: "Governor",
      }),
      expect.objectContaining({ timeoutMs: 30000 })
    );
  });

  it("fails the smoke when required live-shape evidence is missing", async () => {
    const client = {
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched" as const,
        committeeId: "CC10174",
        committeeName: "Green, Josh",
        electionPeriod: "2018-2022",
        totalAmount: 4070153.38,
        confidence: "exact" as const,
        source: "csc_api" as const,
        sourceUrl: null,
        matchedSummaryRowCount: 1,
      })),
      getDirectOccupationAggregates: vi.fn(async () => []),
      getContributionSizeAggregates: vi.fn(async () => []),
      getIndependentExpenditureGroups: vi.fn(async () => []),
      getNoncandidateCommitteeFunders: vi.fn(async () => []),
    };

    const output = await runHawaiiCandidateFinanceLiveSmoke({
      client,
      now: new Date("2026-06-23T12:00:00.000Z"),
    });

    expect(output.ok).toBe(false);
    expect(output.checks).toEqual(
      expect.arrayContaining([
        { name: "top_occupations_present", passed: false },
        { name: "be_change_now_support_group_present", passed: false },
        { name: "construction_support_industry_present", passed: false },
        { name: "opposition_groups_present", passed: false },
      ])
    );
  });
});
