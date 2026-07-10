import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
});

import { loadLouisianaCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/louisianaFinance/louisianaBallotLookupFinanceLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

describe("louisianaBallotLookupFinanceLoader", () => {
  it("maps Louisiana finance tables into ballot lookup summaries without occupation data", async () => {
    vi.stubEnv("LOUISIANA_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            committee_id: "12345",
            election_year: 2027,
            total_receipts: "1000.00",
            direct_contribution_total: "1000.00",
            total_disbursements: null,
            cash_on_hand: null,
            outside_support_total: "5000.00",
            outside_oppose_total: null,
            source_url: "https://example.invalid/summary",
            last_synced_at: "2026-06-01T00:00:00.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            category_type: "contribution_size",
            category_name: "$1,000-$4,999",
            amount: "1000.00",
            contributor_count: "1",
            source_url: "https://example.invalid/direct",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            committee_id: "PAC1",
            committee_name: "Better Louisiana PAC",
            support_oppose: "support",
            amount: "5000.00",
            source_url: "https://example.invalid/group",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            support_oppose: "support",
            category_name: "technology",
            amount: "5000.00",
            contributor_count: "1",
            source_url: "https://example.invalid/industry",
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            industry_name: "technology",
            committee_id: "PAC1",
            committee_name: "Better Louisiana PAC",
            support_oppose: "support",
            organization_name: "Google LLC",
            amount: "5000.00",
            contributor_count: "1",
            source_url: "https://example.invalid/donor",
          },
        ],
      });

    const result = await loadLouisianaCandidateFinanceSummariesByCandidateElection(
      { query },
      [
        {
          candidate_id: CANDIDATE_ID,
          election_id: ELECTION_ID,
        },
      ],
      [
        {
          election_id: ELECTION_ID,
          state: "LA",
          office_scope: "statewide",
          office_canonical_name: "Governor",
        },
      ]
    );

    expect(query).toHaveBeenCalledTimes(5);
    expect(String(query.mock.calls[0]?.[0])).toContain("public.la_candidate_finance_links");
    expect(String(query.mock.calls[2]?.[0])).toContain("public.la_candidate_finance_outside_groups");
    expect(String(query.mock.calls[4]?.[0])).toContain("public.finance_label_classifications");

    const summary = result.get(`${CANDIDATE_ID}\u0000${ELECTION_ID}`);
    expect(summary).toMatchObject({
      source: "LOUISIANA_ETHICS",
      cycle: 2027,
      fec_candidate_id: null,
      controlled_committee_id: "12345",
      direct_campaign: {
        total_raised: 1000,
        total_spent: null,
        cash_on_hand: null,
        debts_owed: null,
        top_occupations: [],
        top_employers: [],
        top_industries: [],
        contribution_size_buckets: [
          {
            category_name: "$1,000-$4,999",
            amount: 1000,
            contributor_count: 1,
            source_url: "https://example.invalid/direct",
          },
        ],
      },
      outside_spending: {
        support_total: 5000,
        oppose_total: null,
        top_supporting_groups: [
          {
            committee_id: "PAC1",
            committee_name: "Better Louisiana PAC",
            support_oppose: "support",
            amount: 5000,
            source_url: "https://example.invalid/group",
          },
        ],
        top_opposing_groups: [],
        top_supporting_industries: [
          {
            category_name: "technology",
            amount: 5000,
            contributor_count: 1,
            source_url: "https://example.invalid/industry",
          },
        ],
        top_opposing_industries: [],
      },
      backing_summary: {
        top_direct_donor_occupations: [],
        top_outside_supporting_industries: [
          {
            category_name: "technology",
            amount: 5000,
            contributor_count: 1,
            source_url: "https://example.invalid/industry",
            supporting_organizations: [
              {
                organization_name: "Google LLC",
                organization_type: "donor",
                amount: 5000,
                contributor_count: 1,
                committee_id: "PAC1",
                committee_name: "Better Louisiana PAC",
                source_url: "https://example.invalid/donor",
              },
            ],
          },
        ],
      },
    });
    expect(summary?.backing_summary.top_outside_supporting_industries[0]?.explanation).toContain("Google LLC");
  });

  it("skips non-Louisiana and unsupported-office requests without querying", async () => {
    vi.stubEnv("LOUISIANA_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn();

    const result = await loadLouisianaCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "TX", office_scope: "statewide", office_canonical_name: "Governor" }]
    );

    expect(result.size).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });

  it("uses one read transaction when called with a pool", async () => {
    vi.stubEnv("LOUISIANA_CAMPAIGN_FINANCE_ENABLED", "true");
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [] }),
      release: vi.fn(),
    };
    const pool = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue(client),
    };

    const result = await loadLouisianaCandidateFinanceSummariesByCandidateElection(
      pool,
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "LA", office_scope: "statewide", office_canonical_name: "Governor" }]
    );

    expect(result.size).toBe(0);
    expect(pool.query).not.toHaveBeenCalled();
    expect(pool.connect).toHaveBeenCalledTimes(1);
    expect(client.query.mock.calls[0]?.[0]).toBe("BEGIN READ ONLY");
    expect(String(client.query.mock.calls[1]?.[0])).toContain("public.la_candidate_finance_links");
    expect(client.query.mock.calls.at(-1)?.[0]).toBe("COMMIT");
    expect(client.release).toHaveBeenCalledTimes(1);
  });
});
