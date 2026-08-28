import { afterEach, describe, expect, it, vi } from "vitest";

import { loadNevadaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/nevadaFinance/nevadaBallotLookupFinanceLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const ELECTION = {
  election_id: ELECTION_ID,
  state: "NV",
  office_scope: "statewide",
  office_canonical_name: "Governor",
} as const;

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("nevadaBallotLookupFinanceLoader", () => {
  it("stays inert when disabled or the election office is out of scope", async () => {
    vi.stubEnv("NEVADA_CAMPAIGN_FINANCE_ENABLED", "false");
    const query = vi.fn();
    const candidates = [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }];

    expect(
      await loadNevadaCandidateFinanceSummariesByCandidateElection({ query }, candidates, [ELECTION])
    ).toEqual(new Map());

    vi.stubEnv("NEVADA_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(
      await loadNevadaCandidateFinanceSummariesByCandidateElection({ query }, candidates, [
        { ...ELECTION, office_scope: "county", office_canonical_name: "Sheriff" },
      ])
    ).toEqual(new Map());
    expect(query).not.toHaveBeenCalled();
  });

  it("publishes totals, organization-donor industries, and the Nevada coverage notes", async () => {
    vi.stubEnv("NEVADA_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              committee_id: "JOSEPH LOMBARDO",
              election_year: 2026,
              total_receipts: "9090820.60",
              direct_contribution_total: "9090820.60",
              total_disbursements: "5454661.06",
              cash_on_hand: "9197364.58",
              outside_support_total: null,
              outside_oppose_total: null,
              source_url: null,
              last_synced_at: "2026-08-26T12:00:00.000Z",
            },
          ],
        };
      }
      if (queries.length === 2) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              category_type: "industry",
              category_name: "gaming",
              amount: "125000.00",
              contributor_count: null,
              source_url: null,
            },
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              category_type: "contribution_size",
              category_name: "$5,000+",
              amount: "8000000.00",
              contributor_count: null,
              source_url: null,
            },
          ],
        };
      }
      return { rows: [] };
    });

    const result = await loadNevadaCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [ELECTION]
    );

    const summary = result.get(`${CANDIDATE_ID}${String.fromCharCode(0)}${ELECTION_ID}`);
    expect(summary).toMatchObject({
      source: "NEVADA_AURORA",
      cycle: 2026,
      controlled_committee_id: "JOSEPH LOMBARDO",
      direct_campaign: {
        total_raised: 9_090_820.6,
        total_spent: 5_454_661.06,
        cash_on_hand: 9_197_364.58,
        top_occupations: [],
      },
    });
    expect(summary?.direct_campaign.direct_coverage_note).toMatch(/does not collect donor occupation/);
    expect(summary?.direct_campaign.top_industries?.[0]).toMatchObject({
      category_name: "gaming",
      amount: 125_000,
    });
    expect(
      summary?.direct_campaign.top_contribution_sizes?.[0] ??
        summary?.direct_campaign.top_industries?.length
    ).toBeTruthy();
    // Every SQL touches nv_ tables only.
    expect(queries.every((sql) => /nv_candidate_finance_/.test(sql))).toBe(true);
  });
});
