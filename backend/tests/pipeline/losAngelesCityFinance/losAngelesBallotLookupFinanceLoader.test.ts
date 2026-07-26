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
            matching_funds: "4",
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
      direct_campaign: { total_raised: 10, public_funds_received: 4 },
      outside_spending: { support_total: 3 },
    });
  });
  it("maps a null matching-funds snapshot to null public funds", async () => {
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
            matching_funds: null,
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
    expect(
      result.get("c\u0000e")?.direct_campaign.public_funds_received,
    ).toBeNull();
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
  it("accepts both Phase 2 offices before querying snapshots", async () => {
    vi.stubEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await loadLosAngelesCandidateFinanceSummariesByCandidateElection(
      { query } as never,
      [
        { candidate_id: "attorney", election_id: "attorney-election" },
        { candidate_id: "controller", election_id: "controller-election" },
      ],
      [
        {
          election_id: "attorney-election",
          state: "CA",
          district_type: "place",
          geoid_compact: "0644000",
          office_scope: "place",
          office_canonical_name: "Municipal Attorney",
        },
        {
          election_id: "controller-election",
          state: "CA",
          district_type: "place",
          geoid_compact: "0644000",
          office_scope: "place",
          office_canonical_name: "Municipal Controller",
        },
      ],
    );
    expect(JSON.parse(String(query.mock.calls[0]?.[1]?.[0]))).toEqual([
      { candidate_id: "attorney", election_id: "attorney-election" },
      { candidate_id: "controller", election_id: "controller-election" },
    ]);
  });

  it("accepts a council election only with a recognized exact seat title", async () => {
    vi.stubEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await loadLosAngelesCandidateFinanceSummariesByCandidateElection(
      { query } as never,
      [
        { candidate_id: "district-3", election_id: "valid" },
        { candidate_id: "unknown", election_id: "invalid" },
      ],
      [
        {
          election_id: "valid",
          state: "CA",
          district_type: "place",
          geoid_compact: "0644000",
          office_scope: "place",
          office_canonical_name: "City Council Member",
          official_ballot_title: "Member of the City Council, District No. 3",
        },
        {
          election_id: "invalid",
          state: "CA",
          district_type: "place",
          geoid_compact: "0644000",
          office_scope: "place",
          office_canonical_name: "City Council Member",
          official_ballot_title: "City Council Member",
        },
      ],
    );
    expect(JSON.parse(String(query.mock.calls[0]?.[1]?.[0]))).toEqual([
      { candidate_id: "district-3", election_id: "valid" },
    ]);
  });
  it("accepts only the exact LAUSD district and board-seat title", async () => {
    vi.stubEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await loadLosAngelesCandidateFinanceSummariesByCandidateElection(
      { query } as never,
      [
        { candidate_id: "lausd", election_id: "valid" },
        { candidate_id: "other-school", election_id: "invalid" },
      ],
      [
        {
          election_id: "valid",
          state: "CA",
          district_type: "school_unified",
          geoid_compact: "0622710",
          office_scope: "school_unified",
          office_canonical_name: "School Board Member",
          official_ballot_title:
            "Member of the Board of Education, District 6",
        },
        {
          election_id: "invalid",
          state: "CA",
          district_type: "school_unified",
          geoid_compact: "0600001",
          office_scope: "school_unified",
          office_canonical_name: "School Board Member",
          official_ballot_title:
            "Member of the Board of Education, District 6",
        },
      ],
    );
    expect(JSON.parse(String(query.mock.calls[0]?.[1]?.[0]))).toEqual([
      { candidate_id: "lausd", election_id: "valid" },
    ]);
  });
});
