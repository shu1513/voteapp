import type { Pool, PoolClient } from "pg";

import { isNewYorkCityCampaignFinanceEnabled } from "../../config/featureFlags.js";
import {
  addFinanceBreakdown,
  buildOutsideIndustrySupportExplanation,
  buildStateFinanceSummaryRequests,
  candidateElectionKey,
  mapFinanceBreakdown,
  parseFinanceAmount,
  parseFinanceCount,
  type BallotLookupFinanceBreakdown,
  type BallotLookupFinanceOutsideGroup,
  type BallotLookupFinanceOutsideIndustrySupportSummary,
  type BallotLookupFinanceSummary,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";
import { NEW_YORK_CITY_CFB_DATA_LIBRARY_URL } from "./newYorkCityCfbCsv.js";
import { toNewYorkCityCfbOfficeSearchInput } from "./newYorkCityFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;

type SummaryRow = {
  candidate_id: string;
  election_id: string;
  cfb_candidate_id: string;
  election_year: number;
  private_contributions: string | number | null;
  net_expenditures: string | number | null;
  outstanding_bills: string | number | null;
  public_funds: string | number | null;
  outside_support_total: string | number | null;
  outside_oppose_total: string | number | null;
  source_url: string | null;
  last_synced_at: string;
};

type BreakdownRow = {
  candidate_id: string;
  election_id: string;
  category_type: "occupation" | "employer" | "industry" | "contribution_size";
  category_name: string;
  amount: string | number;
  contributor_count: string | number | null;
  source_url: string | null;
};

type OutsideGroupRow = {
  candidate_id: string;
  election_id: string;
  spender_id: string;
  spender_name: string;
  support_oppose: "support" | "oppose";
  amount: string | number;
  expenditure_count: string | number;
  source_url: string | null;
};

export async function loadNewYorkCityCandidateFinanceSummariesByCandidateElection(
  db: Queryable,
  candidateRows: readonly StateFinanceRequestCandidateRow[],
  electionRows: readonly StateFinanceRequestElectionRow[]
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!isNewYorkCityCampaignFinanceEnabled()) return new Map();
  const requests = buildStateFinanceSummaryRequests("NY", candidateRows, electionRows, (row) =>
    toNewYorkCityCfbOfficeSearchInput({
      officeScope: row.office_scope,
      officeCanonicalName: row.office_canonical_name,
      districtGeoid: row.geoid_compact,
    }) !== null
  );
  if (requests.length === 0) return new Map();

  const summaryResult = await db.query<SummaryRow>(
    `
      WITH requested AS (
        SELECT candidate_id::uuid AS candidate_id, election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(candidate_id text, election_id text)
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        link.cfb_candidate_id,
        summary.election_year,
        summary.private_contributions,
        summary.net_expenditures,
        summary.outstanding_bills,
        summary.public_funds,
        summary.outside_support_total,
        summary.outside_oppose_total,
        summary.source_url,
        summary.last_synced_at::text
      FROM requested
      JOIN public.nyc_candidate_finance_links AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.nyc_candidate_finance_summaries AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
    `,
    [JSON.stringify(requests)]
  );
  if (summaryResult.rows.length === 0) return new Map();

  const selected = summaryResult.rows.map((row) => ({ candidate_id: row.candidate_id, election_id: row.election_id }));
  const breakdownResult = await db.query<BreakdownRow>(
    `
      WITH selected AS (
        SELECT candidate_id::uuid AS candidate_id, election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(candidate_id text, election_id text)
      ), ranked AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          breakdown.amount,
          breakdown.contributor_count,
          breakdown.source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, breakdown.category_type
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC
          ) AS rn
        FROM selected
        JOIN public.nyc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.nyc_candidate_finance_direct_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE category_type = 'contribution_size' OR rn <= 5
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selected)]
  );
  const outsideGroupResult = await db.query<OutsideGroupRow>(
    `
      WITH selected AS (
        SELECT candidate_id::uuid AS candidate_id, election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(candidate_id text, election_id text)
      ), ranked AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.spender_id,
          outside_group.spender_name,
          outside_group.support_oppose,
          outside_group.amount,
          outside_group.expenditure_count,
          outside_group.source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, outside_group.support_oppose
            ORDER BY outside_group.amount DESC, outside_group.spender_name ASC
          ) AS rn
        FROM selected
        JOIN public.nyc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.nyc_candidate_finance_outside_groups AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
      )
      SELECT candidate_id, election_id, spender_id, spender_name, support_oppose,
             amount, expenditure_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, spender_name ASC
    `,
    [JSON.stringify(selected)]
  );
  const outsideIndustryResult = await db.query<BreakdownRow & { support_oppose: "support" | "oppose" }>(
    `
      WITH selected AS (
        SELECT candidate_id::uuid AS candidate_id, election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(candidate_id text, election_id text)
      ), grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.support_oppose,
          breakdown.category_name,
          SUM(breakdown.amount) AS amount,
          SUM(breakdown.contributor_count) AS contributor_count,
          MIN(breakdown.source_url) AS source_url
        FROM selected
        JOIN public.nyc_candidate_finance_links AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.nyc_candidate_finance_outside_group_breakdowns AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
         AND breakdown.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id,
                 breakdown.support_oppose, breakdown.category_name
      ), ranked AS (
        SELECT grouped.*,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, support_oppose, 'industry'::text AS category_type,
             category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, category_name ASC
    `,
    [JSON.stringify(selected)]
  );

  const byType = {
    occupation: new Map<string, BallotLookupFinanceBreakdown[]>(),
    employer: new Map<string, BallotLookupFinanceBreakdown[]>(),
    industry: new Map<string, BallotLookupFinanceBreakdown[]>(),
    contribution_size: new Map<string, BallotLookupFinanceBreakdown[]>(),
  };
  for (const row of breakdownResult.rows) {
    addFinanceBreakdown(byType[row.category_type], row.candidate_id, row.election_id, mapFinanceBreakdown(row, NEW_YORK_CITY_CFB_DATA_LIBRARY_URL));
  }
  const outsideGroups = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  for (const row of outsideGroupResult.rows) {
    const key = `${candidateElectionKey(row.candidate_id, row.election_id)}\u0000${row.support_oppose}`;
    const groups = outsideGroups.get(key) ?? [];
    groups.push({
      committee_id: row.spender_id,
      committee_name: row.spender_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      expenditure_count: parseFinanceCount(row.expenditure_count),
      source_url: row.source_url,
    });
    outsideGroups.set(key, groups);
  }
  const outsideIndustries = {
    support: new Map<string, BallotLookupFinanceBreakdown[]>(),
    oppose: new Map<string, BallotLookupFinanceBreakdown[]>(),
  };
  for (const row of outsideIndustryResult.rows) {
    addFinanceBreakdown(
      outsideIndustries[row.support_oppose],
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row)
    );
  }

  return new Map(summaryResult.rows.map((row) => {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const occupations = byType.occupation.get(key) ?? [];
    const supportingIndustries = outsideIndustries.support.get(key) ?? [];
    return [key, {
      source: "NEW_YORK_CITY_CFB",
      cycle: row.election_year,
      fec_candidate_id: null,
      controlled_committee_id: row.cfb_candidate_id,
      last_synced_at: row.last_synced_at,
      direct_campaign: {
        total_raised: parseFinanceAmount(row.private_contributions),
        total_spent: parseFinanceAmount(row.net_expenditures),
        cash_on_hand: null,
        debts_owed: parseFinanceAmount(row.outstanding_bills),
        public_funds_received: parseFinanceAmount(row.public_funds),
        top_occupations: occupations,
        top_employers: byType.employer.get(key) ?? [],
        top_industries: byType.industry.get(key) ?? [],
        contribution_size_buckets: byType.contribution_size.get(key) ?? [],
      },
      outside_spending: {
        support_total: parseFinanceAmount(row.outside_support_total),
        oppose_total: parseFinanceAmount(row.outside_oppose_total),
        top_supporting_groups: outsideGroups.get(`${key}\u0000support`) ?? [],
        top_opposing_groups: outsideGroups.get(`${key}\u0000oppose`) ?? [],
        top_supporting_industries: supportingIndustries,
        top_opposing_industries: outsideIndustries.oppose.get(key) ?? [],
      },
      backing_summary: {
        top_direct_donor_occupations: occupations,
        top_outside_supporting_industries: supportingIndustries.map(
          (industry): BallotLookupFinanceOutsideIndustrySupportSummary => ({
            ...industry,
            explanation: buildOutsideIndustrySupportExplanation(industry.category_name, []),
            supporting_organizations: [],
          })
        ),
      },
    } satisfies BallotLookupFinanceSummary];
  }));
}
