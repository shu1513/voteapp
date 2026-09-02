import { afterEach, describe, expect, it, vi } from "vitest";

import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";
import { loadArkansasCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/arkansasFinance/arkansasBallotLookupFinanceLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const ELECTION = {
  election_id: ELECTION_ID,
  state: "AR",
  office_scope: "state_lower",
  office_canonical_name: "State Lower Chamber Legislator",
} as const;

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("arkansasBallotLookupFinanceLoader", () => {
  it("stays inert when disabled or the election is not an eligible office", async () => {
    const query = vi.fn();
    const candidates = [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }];

    expect(await loadArkansasCandidateFinanceSummariesByCandidateElection({ query }, candidates, [ELECTION])).toEqual(
      new Map()
    );

    vi.stubEnv("ARKANSAS_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(
      await loadArkansasCandidateFinanceSummariesByCandidateElection({ query }, candidates, [
        { ...ELECTION, office_scope: "county", office_canonical_name: "County Judge" },
      ])
    ).toEqual(new Map());
    expect(query).not.toHaveBeenCalled();
  });

  it("publishes registration totals, occupations, size buckets, and both coverage notes", async () => {
    vi.stubEnv("ARKANSAS_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              committee_id: "7968",
              election_year: 2026,
              total_receipts: "15800.00",
              direct_contribution_total: "15800.00",
              total_disbursements: "32541.03",
              cash_on_hand: "-1063.11",
              outside_support_total: null,
              outside_oppose_total: null,
              source_url: "https://ethics-disclosures.sos.arkansas.gov/",
              last_synced_at: "2026-09-02T12:00:00.000Z",
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
              category_type: "occupation",
              category_name: "Retired",
              amount: "1500.00",
              contributor_count: "3",
              source_url: "https://ethics-disclosures.sos.arkansas.gov/",
            },
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              category_type: "contribution_size",
              category_name: "$500-$999",
              amount: "1500.00",
              contributor_count: "3",
              source_url: "https://ethics-disclosures.sos.arkansas.gov/",
            },
          ],
        };
      }
      return { rows: [] };
    });

    const result = await loadArkansasCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [ELECTION]
    );

    expect(query).toHaveBeenCalledTimes(5);
    const summary = result.get(`${CANDIDATE_ID}\u0000${ELECTION_ID}`);
    expect(summary).toMatchObject({
      source: "ARKANSAS_CFIS",
      cycle: 2026,
      controlled_committee_id: "7968",
      direct_campaign: {
        total_raised: 15_800,
        total_spent: 32_541.03,
        cash_on_hand: -1_063.11,
        top_occupations: [
          {
            category_name: "Retired",
            amount: 1_500,
            contributor_count: 3,
            source_url: "https://ethics-disclosures.sos.arkansas.gov/",
          },
        ],
        top_employers: [],
        top_industries: [],
        contribution_size_buckets: [
          {
            category_name: "$500-$999",
            amount: 1_500,
            contributor_count: 3,
            source_url: "https://ethics-disclosures.sos.arkansas.gov/",
          },
        ],
      },
      outside_spending: {
        support_total: null,
        oppose_total: null,
        top_supporting_groups: [],
        top_opposing_groups: [],
      },
      backing_summary: {
        top_direct_donor_occupations: [{ category_name: "Retired" }],
      },
    });
    expect(summary?.direct_campaign.direct_coverage_note).toContain("registration figures");
    expect(summary?.outside_spending.outside_coverage_note).toContain("unavailable rather than zero");

    const [summarySql, directSql, outsideGroupSql] = queries;
    expect(summarySql).toContain("public.ar_candidate_finance_links");
    expect(summarySql).toContain("count(DISTINCT link.filing_entity_id)");
    expect(directSql).toContain("breakdown.category_type IN ('occupation', 'contribution_size')");
    expect(outsideGroupSql).toContain("outside_group.filing_entity_id AS committee_id");
    expect(outsideGroupSql).toContain("min(outside_group.filer_name) AS committee_name");
  });

  it("references only AR outside-group columns created by migrations", async () => {
    vi.stubEnv("ARKANSAS_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      return {
        rows: queries.length === 1 ? [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID, election_year: 2026 }] : [],
      };
    });

    await loadArkansasCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [ELECTION]
    );

    const outsideGroupSql = queries.find((sql) => sql.includes("ar_candidate_finance_outside_groups"));
    expect(outsideGroupSql).toBeDefined();
    const referencedColumns = new Set([...outsideGroupSql!.matchAll(/outside_group\.([a-z_]+)/g)].map((match) => match[1]));
    const schemaColumns = migrationTableColumns("ar_candidate_finance_outside_groups");
    expect(referencedColumns.size).toBeGreaterThan(0);
    expect(schemaColumns.size).toBeGreaterThan(0);
    for (const column of referencedColumns) {
      expect(schemaColumns.has(column!), `column ${column} missing from migrations`).toBe(true);
    }
  });
});
