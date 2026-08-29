import { afterEach, describe, expect, it, vi } from "vitest";

import { loadMontanaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/montanaFinance/montanaBallotLookupFinanceLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const ELECTION = {
  election_id: ELECTION_ID,
  state: "MT",
  office_scope: "state_upper",
  office_canonical_name: "State Senator",
} as const;

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("montanaBallotLookupFinanceLoader", () => {
  it("stays inert when disabled or the election office is out of scope", async () => {
    vi.stubEnv("MONTANA_CAMPAIGN_FINANCE_ENABLED", "false");
    const query = vi.fn();
    const candidates = [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }];

    expect(
      await loadMontanaCandidateFinanceSummariesByCandidateElection({ query }, candidates, [ELECTION])
    ).toEqual(new Map());

    vi.stubEnv("MONTANA_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(
      await loadMontanaCandidateFinanceSummariesByCandidateElection({ query }, candidates, [
        { ...ELECTION, office_scope: "county", office_canonical_name: "Sheriff" },
      ])
    ).toEqual(new Map());
    expect(query).not.toHaveBeenCalled();
  });

  it("publishes totals, occupations, and the Montana coverage note from mt_ tables", async () => {
    vi.stubEnv("MONTANA_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              committee_id: "21020",
              election_year: 2026,
              total_receipts: null,
              direct_contribution_total: "66517.57",
              total_disbursements: "61467.18",
              cash_on_hand: "4222.65",
              outside_support_total: null,
              outside_oppose_total: null,
              source_url: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
              last_synced_at: "2026-08-28T12:00:00.000Z",
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
              category_type: "occupation",
              category_name: "Retired",
              amount: "20000.00",
              contributor_count: 42,
              source_url: null,
            },
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              category_type: "contribution_size",
              category_name: "$100-$249",
              amount: "30000.00",
              contributor_count: 180,
              source_url: null,
            },
          ],
        };
      }
      return { rows: [] };
    });

    const result = await loadMontanaCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [ELECTION]
    );

    const summary = result.get(`${CANDIDATE_ID}${String.fromCharCode(0)}${ELECTION_ID}`);
    expect(summary).toMatchObject({
      source: "MONTANA_COPP",
      cycle: 2026,
      controlled_committee_id: "21020",
      direct_campaign: {
        total_raised: 66_517.57,
        total_spent: 61_467.18,
        cash_on_hand: 4_222.65,
      },
    });
    expect(summary?.direct_campaign.direct_coverage_note).toMatch(/cash-balance chain/);
    // The export file is lossy (dropped rows, stale amendments, sub-$50
    // omissions), and occupations come only from it — the note says so.
    expect(summary?.direct_campaign.direct_coverage_note).toMatch(/omit small or amended contributions/);
    expect(summary?.outside_spending.outside_coverage_note).toMatch(/opposing an opponent/);
    expect(summary?.direct_campaign.top_occupations?.[0]).toMatchObject({
      category_name: "Retired",
      amount: 20_000,
    });
    // Every SQL touches mt_ tables only.
    expect(queries.every((sql) => /mt_candidate_finance_/.test(sql))).toBe(true);
  });
});
