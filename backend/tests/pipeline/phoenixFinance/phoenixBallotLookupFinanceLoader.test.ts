import { afterEach, describe, expect, it, vi } from "vitest";
import { candidateElectionKey } from "../../../src/pipeline/address/ballotLookupFinanceShared.js";
import { loadPhoenixCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/phoenixFinance/phoenixBallotLookupFinanceLoader.js";

const KEY = candidateElectionKey("c", "e");

afterEach(() => vi.unstubAllEnvs());

const PHOENIX_COUNCIL_ELECTION = {
  election_id: "e",
  state: "AZ",
  district_type: "place",
  geoid_compact: "0455000",
  office_scope: "place",
  office_canonical_name: "City Council Member",
  official_ballot_title: "Phoenix City Council, District 4",
};

function summaryRow(over: Record<string, unknown> = {}) {
  return {
    candidate_id: "c",
    election_id: "e",
    cop_id: "CAN-25-4",
    election_year: 2026,
    total_raised: "316139.10",
    total_spent: "108905.71",
    cash_on_hand: "231095.51",
    debts_owed: null,
    loans_received: "0",
    outside_support_total: "0",
    outside_oppose_total: "0",
    direct_coverage_note:
      "Totals come from the committee's reports filed in the city's e-filing system.",
    outside_coverage_note:
      "Outside spending totals cover independent expenditures reported to the Phoenix City Clerk by city-registered committees.",
    source_url: "https://apps-secure.phoenix.gov/CampaignFinance",
    last_synced_at: "2026-08-12",
    ...over,
  };
}

describe("Phoenix ballot finance loader", () => {
  it("maps a DB snapshot with both always-on notes and empty size buckets", async () => {
    vi.stubEnv("PHOENIX_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [summaryRow()] })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: "c",
            election_id: "e",
            category_type: "employer",
            category_name: "Desert Law LLP",
            amount: "500",
            contributor_count: "3",
            source_url: null,
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: "c",
            election_id: "e",
            spender_filer_id: "PAC-22-14",
            spender_name: "Some IE PAC",
            support_oppose: "oppose",
            amount: "6500",
            expenditure_count: "1",
            source_url: null,
          },
        ],
      });
    const result = await loadPhoenixCandidateFinanceSummariesByCandidateElection(
      { query } as never,
      [{ candidate_id: "c", election_id: "e" }],
      [PHOENIX_COUNCIL_ELECTION],
    );
    expect(result.get(KEY)).toMatchObject({
      source: "PHOENIX_CITY_CLERK",
      cycle: 2026,
      controlled_committee_id: "CAN-25-4",
      direct_campaign: {
        total_raised: 316139.1,
        total_spent: 108905.71,
        cash_on_hand: 231095.51,
        debts_owed: null,
        loans_received: 0,
        direct_coverage_note: expect.stringContaining("e-filing system"),
        contribution_size_buckets: [],
        top_employers: [
          {
            category_name: "Desert Law LLP",
            amount: 500,
            contributor_count: 3,
            // Breakdown rows without their own URL inherit the summary's.
            source_url: "https://apps-secure.phoenix.gov/CampaignFinance",
          },
        ],
      },
      outside_spending: {
        support_total: 0,
        oppose_total: 0,
        outside_coverage_note: expect.stringContaining("Phoenix City Clerk"),
        top_opposing_groups: [
          expect.objectContaining({
            committee_id: "PAC-22-14",
            committee_name: "Some IE PAC",
            amount: 6500,
            expenditure_count: 1,
          }),
        ],
      },
    });
    const sql = String(query.mock.calls[0]?.[0]);
    for (const column of [
      "link.cop_id",
      "summary.loans_received",
      "summary.direct_coverage_note",
      "summary.outside_coverage_note",
    ]) {
      expect(sql).toContain(column);
    }
  });

  it("does no DB work when the master flag is off or the election is not Phoenix", async () => {
    vi.stubEnv("PHOENIX_CAMPAIGN_FINANCE_ENABLED", "false");
    const query = vi.fn();
    expect(
      (
        await loadPhoenixCandidateFinanceSummariesByCandidateElection(
          { query } as never,
          [{ candidate_id: "c", election_id: "e" }],
          [PHOENIX_COUNCIL_ELECTION],
        )
      ).size,
    ).toBe(0);
    vi.stubEnv("PHOENIX_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(
      (
        await loadPhoenixCandidateFinanceSummariesByCandidateElection(
          { query } as never,
          [{ candidate_id: "c", election_id: "e" }],
          // Tucson, not Phoenix.
          [{ ...PHOENIX_COUNCIL_ELECTION, geoid_compact: "0477000" }],
        )
      ).size,
    ).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });
});
