import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { loadNewYorkCityCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/newYorkCityFinance/newYorkCityBallotLookupFinanceLoader.js";

const originalFlag = process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED;
beforeEach(() => { process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED = "true"; });
afterEach(() => {
  if (originalFlag === undefined) delete process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED;
  else process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED = originalFlag;
});
describe("newYorkCityBallotLookupFinanceLoader", () => {
  it("loads NYC summary using shared shape", async () => {
    const db = { query: vi.fn()
      .mockResolvedValueOnce({ rows: [{
        candidate_id: "candidate-1", election_id: "election-1", cfb_candidate_id: "A1", election_year: 2025,
        private_contributions: "1000", net_expenditures: "400", outstanding_bills: "10", public_funds: "200",
        source_url: "https://www.nyccfb.info/follow-the-money/data-library/", last_synced_at: "2025-01-01T00:00:00Z",
      }] })
      .mockResolvedValueOnce({ rows: [
        { candidate_id: "candidate-1", election_id: "election-1", category_type: "occupation", category_name: "Teacher", amount: "500", contributor_count: "2", source_url: null },
        { candidate_id: "candidate-1", election_id: "election-1", category_type: "employer", category_name: "NYC DOE", amount: "400", contributor_count: "2", source_url: null },
      ] }),
    };
    const result = await loadNewYorkCityCandidateFinanceSummariesByCandidateElection(
      db as never,
      [{ candidate_id: "candidate-1", election_id: "election-1" }],
      [{ election_id: "election-1", state: "NY", office_scope: "place", office_canonical_name: "Mayor", geoid_compact: "3651000" }]
    );
    expect(result.get("candidate-1\u0000election-1")).toMatchObject({
      source: "NEW_YORK_CITY_CFB",
      direct_campaign: {
        total_raised: 1000, total_spent: 400, cash_on_hand: null, debts_owed: 10, public_funds_received: 200,
        top_occupations: [expect.objectContaining({ category_name: "Teacher", amount: 500 })],
        top_employers: [expect.objectContaining({ category_name: "NYC DOE", amount: 400 })],
      },
    });
  });

  it("does not query for City Council or a non-NYC place", async () => {
    const db = { query: vi.fn() };
    const result = await loadNewYorkCityCandidateFinanceSummariesByCandidateElection(
      db as never,
      [{ candidate_id: "candidate-1", election_id: "election-1" }],
      [{ election_id: "election-1", state: "NY", office_scope: "place", office_canonical_name: "City Council Member", geoid_compact: "3651000" }]
    );
    expect(result.size).toBe(0);
    expect(db.query).not.toHaveBeenCalled();
  });
});
