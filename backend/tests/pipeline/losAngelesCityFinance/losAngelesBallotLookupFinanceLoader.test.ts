import { afterEach, describe, expect, it, vi } from "vitest";
import { loadLosAngelesCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/losAngelesCityFinance/losAngelesBallotLookupFinanceLoader.js";
afterEach(() => vi.unstubAllEnvs());
describe("Los Angeles ballot finance loader", () => {
  it("gates exact GEOID and maps DB snapshot", async () => {
    vi.stubEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: "c",
            election_id: "e",
            fppc_committee_id: "1471359",
            election_year: 2026,
            total_receipts: "10",
            total_disbursements: "8",
            cash_on_hand: "2",
            outside_support_total: "3",
            outside_oppose_total: "1",
            source_url: "https://ethics.lacity.gov",
            last_synced_at: "2026-07-11",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });
    const result =
      await loadLosAngelesCandidateFinanceSummariesByCandidateElection(
        { query } as never,
        [{ candidate_id: "c", election_id: "e" }],
        [
          {
            election_id: "e",
            state: "CA",
            district_type: "place",
            geoid_compact: "0644000",
            office_scope: "place",
            office_canonical_name: "Mayor",
          },
        ],
      );
    expect(result.get("c\u0000e")).toMatchObject({
      source: "LOS_ANGELES_CITY_ETHICS",
      direct_campaign: { total_raised: 10 },
      outside_spending: { support_total: 3 },
    });
  });
  it("does no DB work for another California city", async () => {
    vi.stubEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn();
    const result =
      await loadLosAngelesCandidateFinanceSummariesByCandidateElection(
        { query } as never,
        [{ candidate_id: "c", election_id: "e" }],
        [
          {
            election_id: "e",
            state: "CA",
            district_type: "place",
            geoid_compact: "0666000",
            office_scope: "place",
            office_canonical_name: "Mayor",
          },
        ],
      );
    expect(result.size).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });
});
