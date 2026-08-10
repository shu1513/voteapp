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
            matching_funds: "6",
            outside_support_total: "3",
            outside_oppose_total: "1",
            membership_support_total: "203457",
            membership_oppose_total: "0",
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
      direct_campaign: { total_raised: 10, public_funds_received: 6 },
      // Member communications ride along as their own totals — they are
      // legally distinct from independent expenditures and must never be
      // folded into support_total/oppose_total.
      outside_spending: {
        support_total: 3,
        oppose_total: 1,
        membership_support_total: 203457,
        membership_oppose_total: 0,
      },
    });
    // The summary SQL must actually select the membership columns — they
    // were silently dropped from the read path once before.
    expect(String(query.mock.calls[0]?.[0])).toContain("summary.membership_support_total");
    expect(String(query.mock.calls[0]?.[0])).toContain("summary.membership_oppose_total");
    // The five largest outside groups must also arrive largest-first — the
    // card renders them in row order without re-sorting. spender_id is the
    // final tiebreaker because spender_name is not unique in the table.
    expect(String(query.mock.calls[2]?.[0])).toContain(
      "ORDER BY candidate_id,election_id,support_oppose,amount DESC,spender_name,spender_id",
    );
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
