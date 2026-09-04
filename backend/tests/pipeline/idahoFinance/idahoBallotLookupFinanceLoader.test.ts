import { afterEach, describe, expect, it, vi } from "vitest";

import { loadIdahoCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/idahoFinance/idahoBallotLookupFinanceLoader.js";
import { GUID_A } from "./idahoTestFixtures.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const ELECTION = {
  election_id: ELECTION_ID,
  state: "ID",
  office_scope: "state_upper",
  office_canonical_name: "State Senator",
} as const;

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("idahoBallotLookupFinanceLoader", () => {
  it("stays inert when disabled or the election office is out of scope", async () => {
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_ENABLED", "false");
    const query = vi.fn();
    const candidates = [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }];

    expect(await loadIdahoCandidateFinanceSummariesByCandidateElection({ query }, candidates, [ELECTION])).toEqual(new Map());

    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(
      await loadIdahoCandidateFinanceSummariesByCandidateElection({ query }, candidates, [
        { ...ELECTION, office_scope: "county", office_canonical_name: "Prosecuting Attorney" },
      ])
    ).toEqual(new Map());
    expect(query).not.toHaveBeenCalled();
  });

  it("publishes grid totals, size buckets, outside groups, and the coverage note from id_ tables", async () => {
    vi.stubEnv("IDAHO_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              committee_id: GUID_A,
              election_year: 2026,
              total_receipts: "1500.00",
              direct_contribution_total: "1500.00",
              total_disbursements: "50.00",
              cash_on_hand: "-25.00",
              outside_support_total: "250.00",
              outside_oppose_total: "100.00",
              source_url: `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=${GUID_A}&tabName=CAN&isLegacy=false`,
              last_synced_at: "2026-09-03T12:00:00.000Z",
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
              category_type: "contribution_size",
              category_name: "$1,000+",
              amount: "1000.00",
              contributor_count: null,
              source_url: null,
            },
          ],
        };
      }
      if (queries.length === 3) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              committee_id: "55555555-5555-4555-8555-555555555501",
              committee_name: "Sample PAC",
              support_oppose: "support",
              amount: "250.00",
              source_url: "https://sunshine.voteidaho.gov/public/cf/independent",
            },
          ],
        };
      }
      return { rows: [] };
    });

    const result = await loadIdahoCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [ELECTION]
    );

    const summary = result.get(`${CANDIDATE_ID}${String.fromCharCode(0)}${ELECTION_ID}`);
    expect(summary).toMatchObject({
      source: "IDAHO_SUNSHINE",
      cycle: 2026,
      controlled_committee_id: GUID_A,
      direct_campaign: { total_raised: 1500, total_spent: 50, cash_on_hand: -25, top_occupations: [] },
      outside_spending: { support_total: 250, oppose_total: 100 },
    });
    expect(summary?.direct_campaign.direct_coverage_note).toMatch(/does not collect donor occupation/);
    expect(summary?.direct_campaign.contribution_size_buckets?.[0]).toMatchObject({ category_name: "$1,000+", amount: 1000 });
    expect(summary?.outside_spending.top_supporting_groups[0]).toMatchObject({ committee_name: "Sample PAC", amount: 250 });
    // Every SQL touches id_ tables only and reads the Idaho identity columns.
    expect(queries.every((sql) => /id_candidate_finance_/.test(sql))).toBe(true);
    expect(queries[0]).toContain("registration_guid");
    expect(queries.some((sql) => sql.includes("filer_key"))).toBe(true);
  });
});
