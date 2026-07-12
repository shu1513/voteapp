import { afterEach, describe, expect, it, vi } from "vitest";

import { loadIllinoisCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/illinoisFinance/illinoisBallotLookupFinanceLoader.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

describe("illinoisBallotLookupFinanceLoader", () => {
  it("does not expose transfer-sensitive totals for a multi-committee candidate", async () => {
    vi.stubEnv("ILLINOIS_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            committee_id: null,
            election_year: 2025,
            total_receipts: null,
            direct_contribution_total: null,
            total_disbursements: null,
            cash_on_hand: "1500.00",
            debts_owed: "200.00",
            outside_support_total: null,
            outside_oppose_total: null,
            source_url: "https://www.elections.il.gov/CampaignDisclosure/",
            last_synced_at: "2025-04-01T00:00:00.000Z",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });

    const result = await loadIllinoisCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [{ election_id: ELECTION_ID, state: "IL", office_scope: "place", office_canonical_name: "Mayor" }]
    );

    expect(query).toHaveBeenCalledTimes(5);
    expect(String(query.mock.calls[0]?.[0])).toContain("count(DISTINCT link.committee_key) = 1");
    expect(result.get(`${CANDIDATE_ID}\u0000${ELECTION_ID}`)?.direct_campaign).toMatchObject({
      total_raised: null,
      total_spent: null,
      cash_on_hand: 1500,
      debts_owed: 200,
    });
  });
});
