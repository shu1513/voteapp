import { afterEach, describe, expect, it, vi } from "vitest";

import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";
import { loadNewHampshireCandidateFinanceSummariesByCandidateElection } from "../../../src/pipeline/newHampshireFinance/newHampshireBallotLookupFinanceLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const ELECTION = {
  election_id: ELECTION_ID,
  state: "NH",
  office_scope: "state_upper",
  office_canonical_name: "State Senator",
} as const;

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("newHampshireBallotLookupFinanceLoader", () => {
  it("stays inert when disabled or the election is not safely eligible", async () => {
    const query = vi.fn();
    const candidates = [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }];

    expect(
      await loadNewHampshireCandidateFinanceSummariesByCandidateElection(
        { query },
        candidates,
        [ELECTION]
      )
    ).toEqual(new Map());

    vi.stubEnv("NEW_HAMPSHIRE_CAMPAIGN_FINANCE_ENABLED", "true");
    expect(
      await loadNewHampshireCandidateFinanceSummariesByCandidateElection(
        { query },
        candidates,
        [{ ...ELECTION, office_scope: "federal", office_canonical_name: "United States Senator" }]
      )
    ).toEqual(new Map());
    expect(query).not.toHaveBeenCalled();
  });

  it("publishes NH totals, employer-derived industries, size buckets, and spender directions", async () => {
    vi.stubEnv("NEW_HAMPSHIRE_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      if (queries.length === 1) {
        return {
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              committee_id: "4102",
              election_year: 2026,
              total_receipts: "140.50",
              direct_contribution_total: "125.50",
              total_disbursements: null,
              cash_on_hand: null,
              outside_support_total: "90.25",
              outside_oppose_total: "12.75",
              source_url: null,
              last_synced_at: "2026-08-21T12:00:00.000Z",
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
              category_type: "industry",
              category_name: "healthcare",
              amount: "75.50",
              contributor_count: "2",
              source_url: null,
            },
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              category_type: "contribution_size",
              category_name: "$100-$249",
              amount: "125.50",
              contributor_count: "1",
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
              committee_id: "9001",
              committee_name: "Granite State Action",
              support_oppose: "support",
              amount: "90.25",
              source_url: null,
            },
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              committee_id: "9002",
              committee_name: "New Hampshire Accountability",
              support_oppose: "oppose",
              amount: "12.75",
              source_url: null,
            },
          ],
        };
      }
      return { rows: [] };
    });

    const result = await loadNewHampshireCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [ELECTION]
    );

    expect(query).toHaveBeenCalledTimes(5);
    const summary = result.get(`${CANDIDATE_ID}\u0000${ELECTION_ID}`);
    expect(summary).toMatchObject({
      source: "NEW_HAMPSHIRE_CFS",
      cycle: 2026,
      controlled_committee_id: "4102",
      direct_campaign: {
        total_raised: 125.5,
        total_spent: null,
        cash_on_hand: null,
        debts_owed: null,
        top_occupations: [],
        top_employers: [],
        top_industries: [
          {
            category_name: "healthcare",
            amount: 75.5,
            contributor_count: 2,
            source_url: "https://cfs.sos.nh.gov/",
          },
        ],
        contribution_size_buckets: [
          {
            category_name: "$100-$249",
            amount: 125.5,
            contributor_count: 1,
            source_url: "https://cfs.sos.nh.gov/",
          },
        ],
      },
      outside_spending: {
        support_total: 90.25,
        oppose_total: 12.75,
        top_supporting_groups: [
          {
            committee_id: "9001",
            committee_name: "Granite State Action",
            support_oppose: "support",
            amount: 90.25,
            source_url: "https://cfs.sos.nh.gov/",
          },
        ],
        top_opposing_groups: [
          {
            committee_id: "9002",
            committee_name: "New Hampshire Accountability",
            support_oppose: "oppose",
            amount: 12.75,
            source_url: "https://cfs.sos.nh.gov/",
          },
        ],
      },
      backing_summary: {
        top_direct_donor_occupations: [],
      },
    });
    expect(summary?.direct_campaign.direct_coverage_note).toContain(
      "industries are derived from disclosed contributor employers"
    );

    const [summarySql, directSql, outsideGroupSql] = queries;
    expect(summarySql).toContain("count(DISTINCT link.filing_entity_id)");
    expect(directSql).toContain("breakdown.category_type IN ('industry', 'contribution_size')");
    expect(directSql).not.toContain("'occupation'");
    expect(outsideGroupSql).toContain("outside_group.filing_entity_id AS committee_id");
    expect(outsideGroupSql).toContain("min(outside_group.filer_name) AS committee_name");
  });

  it("references only NH outside-group columns created by migrations", async () => {
    vi.stubEnv("NEW_HAMPSHIRE_CAMPAIGN_FINANCE_ENABLED", "true");
    const queries: string[] = [];
    const query = vi.fn(async (sql: string) => {
      queries.push(sql);
      return {
        rows: queries.length === 1
          ? [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID, election_year: 2026 }]
          : [],
      };
    });

    await loadNewHampshireCandidateFinanceSummariesByCandidateElection(
      { query },
      [{ candidate_id: CANDIDATE_ID, election_id: ELECTION_ID }],
      [ELECTION]
    );

    const outsideGroupSql = queries.find((sql) => sql.includes("nh_candidate_finance_outside_groups"));
    expect(outsideGroupSql).toBeDefined();
    const referencedColumns = new Set(
      [...outsideGroupSql!.matchAll(/outside_group\.([a-z_]+)/g)].map((match) => match[1])
    );
    const schemaColumns = migrationTableColumns("nh_candidate_finance_outside_groups");
    expect(referencedColumns.size).toBeGreaterThan(0);
    expect(schemaColumns.size).toBeGreaterThan(0);
    for (const column of referencedColumns) {
      expect(
        schemaColumns.has(column),
        `outside_group.${column} is not a column of nh_candidate_finance_outside_groups`
      ).toBe(true);
    }
  });
});
