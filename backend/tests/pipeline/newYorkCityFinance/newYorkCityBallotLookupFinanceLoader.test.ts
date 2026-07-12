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
        outside_support_total: "300", outside_oppose_total: "50",
        source_url: "https://www.nyccfb.info/follow-the-money/data-library/", last_synced_at: "2025-01-01T00:00:00Z",
      }] })
      .mockResolvedValueOnce({ rows: [
        { candidate_id: "candidate-1", election_id: "election-1", category_type: "occupation", category_name: "Teacher", amount: "500", contributor_count: "2", source_url: null },
        { candidate_id: "candidate-1", election_id: "election-1", category_type: "employer", category_name: "NYC DOE", amount: "400", contributor_count: "2", source_url: null },
      ] })
      .mockResolvedValueOnce({ rows: [
        { candidate_id: "candidate-1", election_id: "election-1", spender_id: "Z1", spender_name: "Outside Group", support_oppose: "support", amount: "300", expenditure_count: "2", source_url: "https://example.test/outside" },
      ] })
      .mockResolvedValueOnce({ rows: [
        { candidate_id: "candidate-1", election_id: "election-1", support_oppose: "support", category_type: "industry", category_name: "real_estate", amount: "200", contributor_count: "2", source_url: "https://example.test/funders" },
      ] })
      .mockResolvedValueOnce({ rows: [
        { candidate_id: "candidate-1", election_id: "election-1", industry_name: "real_estate", committee_id: "Z1", committee_name: "Outside Group", support_oppose: "support", organization_name: "Example Realty LLC", organization_type: "donor", amount: "200", contributor_count: "1", source_url: "https://example.test/funders" },
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
      outside_spending: {
        support_total: 300,
        oppose_total: 50,
        top_supporting_groups: [expect.objectContaining({ committee_id: "Z1", amount: 300, expenditure_count: 2 })],
        top_supporting_industries: [expect.objectContaining({ category_name: "real_estate", amount: 200 })],
      },
      backing_summary: {
        top_outside_supporting_industries: [expect.objectContaining({
          explanation: expect.stringContaining("entire election cycle; it is not earmarked to this candidate"),
          supporting_organizations: [expect.objectContaining({
            organization_name: "Example Realty LLC",
            committee_name: "Outside Group",
            amount: 200,
          })],
        })],
      },
    });
    expect(db.query.mock.calls[1]?.[0]).toContain("category_type = 'contribution_size' OR rn <= 5");
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
