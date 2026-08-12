import { afterEach, describe, expect, it, vi } from "vitest";
import { candidateElectionKey } from "../../../src/pipeline/address/ballotLookupFinanceShared.js";
import { loadSanJoseCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/sanJoseFinance/sanJoseBallotLookupFinanceLoader.js";

const KEY = candidateElectionKey("c", "e");

afterEach(() => vi.unstubAllEnvs());

const SAN_JOSE_COUNCIL_ELECTION = {
  election_id: "e",
  state: "CA",
  district_type: "place",
  geoid_compact: "0668000",
  office_scope: "place",
  office_canonical_name: "City Council Member",
  official_ballot_title: "Member, City Council, District 7",
};

function summaryRow(over: Record<string, unknown> = {}) {
  return {
    candidate_id: "c",
    election_id: "e",
    fppc_id: "1484291",
    election_year: 2026,
    total_raised: "117125.37",
    total_spent: "108905.71",
    cash_on_hand: "32668.66",
    debts_owed: "0",
    loans_received: "20000",
    outside_support_total: "101249.54",
    outside_oppose_total: null,
    direct_coverage_note: null,
    source_url: "https://efile.sanjoseca.gov",
    last_synced_at: "2026-08-11",
    ...over,
  };
}

describe("San José ballot finance loader", () => {
  it("maps a DB snapshot with loans, real expenditure counts, and a coverage note", async () => {
    vi.stubEnv("SAN_JOSE_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          summaryRow({
            direct_coverage_note:
              "Totals cover the committee's e-filed disclosures from 2026-01-01 onward; it began that period with money from earlier activity that is not in the city's e-filing export.",
          }),
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: "c",
            election_id: "e",
            category_type: "occupation",
            category_name: "Retired",
            amount: "500",
            contributor_count: "3",
            // Breakdown rows without their own URL must inherit the
            // summary's, or the card footer loses its source link.
            source_url: null,
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            candidate_id: "c",
            election_id: "e",
            spender_filer_id: "1487288",
            spender_name: "SOME IE COMMITTEE",
            support_oppose: "support",
            amount: "101249.54",
            expenditure_count: "12",
            source_url: null,
          },
        ],
      });
    const result =
      await loadSanJoseCandidateFinanceSummariesByCandidateElection(
        { query } as never,
        [{ candidate_id: "c", election_id: "e" }],
        [SAN_JOSE_COUNCIL_ELECTION],
      );
    expect(result.get(KEY)).toMatchObject({
      source: "SAN_JOSE_CITY_CLERK",
      cycle: 2026,
      controlled_committee_id: "1484291",
      direct_campaign: {
        total_raised: 117125.37,
        total_spent: 108905.71,
        cash_on_hand: 32668.66,
        debts_owed: 0,
        loans_received: 20000,
        direct_coverage_note: expect.stringContaining("2026-01-01 onward"),
        top_occupations: [
          {
            category_name: "Retired",
            amount: 500,
            contributor_count: 3,
            source_url: "https://efile.sanjoseca.gov",
          },
        ],
      },
      outside_spending: {
        support_total: 101249.54,
        oppose_total: null,
        top_supporting_groups: [
          {
            committee_id: "1487288",
            committee_name: "SOME IE COMMITTEE",
            support_oppose: "support",
            amount: 101249.54,
            // Transaction-level source — real counts, unlike SF's manifest.
            expenditure_count: 12,
            source_url: "https://efile.sanjoseca.gov",
          },
        ],
      },
    });
    // The summary SQL must actually select the SJ-specific columns.
    const sql = String(query.mock.calls[0]?.[0]);
    for (const column of [
      "summary.total_raised",
      "summary.loans_received",
      "summary.direct_coverage_note",
    ]) {
      expect(sql).toContain(column);
    }
    // Largest-first ordering decides both the rank cutoff and row order.
    expect(String(query.mock.calls[2]?.[0])).toContain(
      "ORDER BY g.amount DESC,g.spender_name,g.spender_filer_id) rn",
    );
  });

  it("omits direct_coverage_note entirely when coverage is complete", async () => {
    vi.stubEnv("SAN_JOSE_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [summaryRow()] })
      .mockResolvedValue({ rows: [] });
    const result =
      await loadSanJoseCandidateFinanceSummariesByCandidateElection(
        { query } as never,
        [{ candidate_id: "c", election_id: "e" }],
        [SAN_JOSE_COUNCIL_ELECTION],
      );
    expect(
      "direct_coverage_note" in result.get(KEY)!.direct_campaign,
    ).toBe(false);
  });

  it("does no DB work for another California city", async () => {
    vi.stubEnv("SAN_JOSE_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn();
    const result =
      await loadSanJoseCandidateFinanceSummariesByCandidateElection(
        { query } as never,
        [{ candidate_id: "c", election_id: "e" }],
        [{ ...SAN_JOSE_COUNCIL_ELECTION, geoid_compact: "0667000" }],
      );
    expect(result.size).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });

  it("does no DB work when the master flag is off", async () => {
    vi.stubEnv("SAN_JOSE_CAMPAIGN_FINANCE_ENABLED", "false");
    const query = vi.fn();
    const result =
      await loadSanJoseCandidateFinanceSummariesByCandidateElection(
        { query } as never,
        [{ candidate_id: "c", election_id: "e" }],
        [SAN_JOSE_COUNCIL_ELECTION],
      );
    expect(result.size).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });

  it("accepts a council election only with a parseable district title", async () => {
    vi.stubEnv("SAN_JOSE_CAMPAIGN_FINANCE_ENABLED", "true");
    const query = vi.fn().mockResolvedValue({ rows: [] });
    await loadSanJoseCandidateFinanceSummariesByCandidateElection(
      { query } as never,
      [
        { candidate_id: "district-7", election_id: "valid" },
        { candidate_id: "unknown", election_id: "invalid" },
      ],
      [
        { ...SAN_JOSE_COUNCIL_ELECTION, election_id: "valid" },
        {
          ...SAN_JOSE_COUNCIL_ELECTION,
          election_id: "invalid",
          official_ballot_title: "City Council Member",
        },
      ],
    );
    expect(JSON.parse(String(query.mock.calls[0]?.[1]?.[0]))).toEqual([
      { candidate_id: "district-7", election_id: "valid" },
    ]);
  });
});
