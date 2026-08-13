import type { Pool, PoolClient } from "pg";

import {
  addFinanceBreakdown,
  buildOutsideIndustrySupportExplanation,
  buildStateFinanceSummaryRequests,
  candidateElectionKey,
  firstNonEmptySourceUrl,
  mapFinanceBreakdown,
  parseFinanceAmount,
  parseFinanceCount,
  type BallotLookupFinanceBreakdown,
  type BallotLookupFinanceOutsideGroup,
  type BallotLookupFinanceOutsideIndustrySupportEvidence,
  type BallotLookupFinanceOutsideIndustrySupportSummary,
  type BallotLookupFinanceSummary,
  type StateFinanceDirectBreakdownRow,
  type StateFinanceOutsideDonorEvidenceRow,
  type StateFinanceOutsideGroupRow,
  type StateFinanceOutsideIndustryRow,
  type StateFinanceSummaryRow,
  type StateFinanceRequestCandidateRow,
  type StateFinanceRequestElectionRow,
} from "../address/ballotLookupFinanceShared.js";

type Queryable = Pick<Pool | PoolClient, "query">;

type CandidateRow = StateFinanceRequestCandidateRow;
type ElectionRow = StateFinanceRequestElectionRow;
type StandardStateFinanceSummaryRow = StateFinanceSummaryRow;
type StandardStateFinanceDirectBreakdownRow = StateFinanceDirectBreakdownRow;
type StandardStateFinanceOutsideGroupRow = StateFinanceOutsideGroupRow;
type StandardStateFinanceOutsideIndustryRow = StateFinanceOutsideIndustryRow;
type StandardStateFinanceOutsideDonorEvidenceRow = StateFinanceOutsideDonorEvidenceRow;

export type StandardStateFinanceTables = {
  links: string;
  summaries: string;
  directBreakdowns: string;
  outsideGroups: string;
  outsideGroupBreakdowns: string;
};

export type StandardStateFinanceCommitteeColumn = "committee_id" | "committee_key";

/**
 * Per-relation identity overrides for states whose link and outside tables
 * name their identity columns differently (e.g. Washington: link.committee_id
 * but outside_group.sponsor_id/sponsor_name). Both outside relations (groups
 * and group breakdowns) always share one identity column in every state, so a
 * single pair covers them. Defaults preserve `committeeColumn` behavior; the
 * emitted rows keep the canonical committee_id/committee_name aliases either
 * way, so the row mapping never changes.
 */
export type StandardStateFinanceOutsideIdentityColumns = {
  id?: string;
  name?: string;
};

/**
 * How summary rows aggregate across a candidate's committees.
 * - "totals": sum every reported value (single-committee sources).
 * - "illinoisD2": D-2 semantics — receipts/spending only when exactly one
 *   committee reports (transfers between a candidate's committees would
 *   double-count), cash/debts only when every committee reported them.
 */
export type StandardStateFinanceSummaryVariant = "totals" | "illinoisD2";

export type StandardStateFinanceEvidenceLabelType = "donor" | "employer";

/**
 * Direct-breakdown category types the loader selects AND can route: occupation
 * rows feed top_occupations, contribution_size rows feed the buckets. States
 * whose direct table only carries one of them (Louisiana/Vermont: buckets
 * only) narrow the list. Types the mapper cannot route (e.g. industry) are
 * deliberately not accepted — routing them lands them in top_occupations.
 */
export type StandardStateFinanceDirectCategoryType = "occupation" | "contribution_size";

const STANDARD_COMMITTEE_COLUMNS: readonly StandardStateFinanceCommitteeColumn[] = ["committee_id", "committee_key"];
const STANDARD_EVIDENCE_LABEL_TYPES: readonly StandardStateFinanceEvidenceLabelType[] = ["donor", "employer"];
const STANDARD_DIRECT_CATEGORY_TYPES: readonly StandardStateFinanceDirectCategoryType[] = [
  "occupation",
  "contribution_size",
];

function assertIdentifier(value: string): string {
  if (!/^[a-z][a-z0-9_]*$/.test(value)) throw new Error(`Invalid standard finance table identifier: ${value}`);
  return value;
}

function assertCommitteeColumn(value: StandardStateFinanceCommitteeColumn): StandardStateFinanceCommitteeColumn {
  if (!STANDARD_COMMITTEE_COLUMNS.includes(value)) {
    throw new Error(`Invalid standard finance committee column: ${value}`);
  }
  return value;
}

// Interpolated into SQL, so identity columns are identifier-validated like the
// table names above.
function assertIdentityColumn(value: string, kind: string): string {
  if (!/^[a-z][a-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid standard finance ${kind} identity column: ${value}`);
  }
  return value;
}

function assertEvidenceLabelTypes(
  values: readonly StandardStateFinanceEvidenceLabelType[]
): readonly StandardStateFinanceEvidenceLabelType[] {
  if (values.length === 0 || values.some((value) => !STANDARD_EVIDENCE_LABEL_TYPES.includes(value))) {
    throw new Error(`Invalid standard finance evidence label types: ${values.join(", ")}`);
  }
  return values;
}

/** Optional per-source summary money columns (see fundingColumns). */
export type StandardStateFinanceFundingColumn = "loans_received" | "public_funds_received";

const STANDARD_FUNDING_COLUMNS: readonly StandardStateFinanceFundingColumn[] = [
  "loans_received",
  "public_funds_received",
];

function assertFundingColumns(
  values: readonly StandardStateFinanceFundingColumn[]
): readonly StandardStateFinanceFundingColumn[] {
  if (values.some((value) => !STANDARD_FUNDING_COLUMNS.includes(value))) {
    throw new Error(`Invalid standard finance funding columns: ${values.join(", ")}`);
  }
  return values;
}

function assertDirectCategoryTypes(
  values: readonly StandardStateFinanceDirectCategoryType[]
): readonly StandardStateFinanceDirectCategoryType[] {
  if (values.length === 0 || values.some((value) => !STANDARD_DIRECT_CATEGORY_TYPES.includes(value))) {
    throw new Error(`Invalid standard finance direct category types: ${values.join(", ")}`);
  }
  return values;
}

export async function loadStandardStateFinanceSummariesByCandidateElection(input: {
  db: Queryable;
  candidateRows: readonly CandidateRow[];
  electionRows: readonly ElectionRow[];
  state: string;
  source: BallotLookupFinanceSummary["source"];
  sourceUrl: string;
  enabled: () => boolean;
  tables: StandardStateFinanceTables;
  isEligibleElection?: (row: ElectionRow) => boolean;
  committeeColumn?: StandardStateFinanceCommitteeColumn;
  /** Link-table identity column for the summary query; default committeeColumn. */
  linkIdentityColumn?: string;
  /** Outside-group + outside-group-breakdown identity columns; defaults committeeColumn/committee_name. */
  outsideGroupIdentityColumns?: StandardStateFinanceOutsideIdentityColumns;
  summaryVariant?: StandardStateFinanceSummaryVariant;
  evidenceLabelTypes?: readonly StandardStateFinanceEvidenceLabelType[];
  /** Direct-breakdown category types selected; default occupation + contribution_size. */
  directBreakdownCategoryTypes?: readonly StandardStateFinanceDirectCategoryType[];
  /**
   * Wording for the outside-industry support explanation's action clause;
   * default "independent spending supporting this candidate". Louisiana and
   * Vermont describe their outside groups as PACs instead.
   */
  outsideSupportActionLabel?: string;
  /**
   * One sentence naming what this source's outside-spending totals do not
   * cover (see BallotLookupFinanceSummary.outside_spending). Omit unless the
   * source has a known, systematic gap.
   */
  outsideCoverageNote?: string;
  /**
   * One sentence naming what this source's direct breakdowns do not cover
   * (see BallotLookupFinanceSummary.direct_campaign). Omit unless the
   * official totals include money the transaction store does not itemize.
   */
  directCoverageNote?: string;
  /**
   * Extra summary money columns to select and publish, for sources whose
   * table carries them (Denver: public matching from the Fair Elections
   * Fund, plus candidate loans). Omit — the default — and the query and the
   * payload are byte-identical to before, so a source whose summaries table
   * lacks these columns is unaffected. The card renders each as its own
   * stat, which is why they are separate from the raised/spent totals: both
   * are money the campaign can spend that is deliberately NOT counted as
   * money raised from donors.
   */
  fundingColumns?: readonly StandardStateFinanceFundingColumn[];
}
): Promise<Map<string, BallotLookupFinanceSummary>> {
  if (!input.enabled()) {
    return new Map();
  }
  const tables = Object.fromEntries(
    Object.entries(input.tables).map(([name, value]) => [name, assertIdentifier(value)])
  ) as StandardStateFinanceTables;
  const committeeColumn = assertCommitteeColumn(input.committeeColumn ?? "committee_id");
  const linkIdentityColumn = assertIdentityColumn(input.linkIdentityColumn ?? committeeColumn, "link");
  const outsideIdColumn = assertIdentityColumn(input.outsideGroupIdentityColumns?.id ?? committeeColumn, "outside-group");
  const outsideNameColumn = assertIdentityColumn(
    input.outsideGroupIdentityColumns?.name ?? "committee_name",
    "outside-group name"
  );
  const summaryVariant = input.summaryVariant ?? "totals";
  const evidenceLabelTypes = assertEvidenceLabelTypes(input.evidenceLabelTypes ?? STANDARD_EVIDENCE_LABEL_TYPES);
  const evidenceLabelTypeList = evidenceLabelTypes.map((value) => `'${value}'`).join(", ");
  const directCategoryTypes = assertDirectCategoryTypes(
    input.directBreakdownCategoryTypes ?? STANDARD_DIRECT_CATEGORY_TYPES
  );
  const directCategoryTypeList = directCategoryTypes.map((value) => `'${value}'`).join(", ");
  const { candidateRows, electionRows, source, sourceUrl } = input;
  const requests = buildStateFinanceSummaryRequests(input.state, candidateRows, electionRows, input.isEligibleElection);
  if (requests.length === 0) {
    return new Map();
  }

  const summaryAggregateColumns =
    summaryVariant === "illinoisD2"
      ? `
        CASE
          WHEN count(DISTINCT link.${linkIdentityColumn}) = 1 AND count(summary.total_receipts) > 0
          THEN sum(summary.total_receipts)
          ELSE NULL
        END AS total_receipts,
        CASE
          WHEN count(DISTINCT link.${linkIdentityColumn}) = 1 AND count(summary.direct_contribution_total) > 0
          THEN sum(summary.direct_contribution_total)
          ELSE NULL
        END AS direct_contribution_total,
        CASE
          WHEN count(DISTINCT link.${linkIdentityColumn}) = 1 AND count(summary.total_disbursements) > 0
          THEN sum(summary.total_disbursements)
          ELSE NULL
        END AS total_disbursements,
        CASE
          WHEN count(summary.cash_on_hand) = count(DISTINCT link.${linkIdentityColumn})
          THEN sum(summary.cash_on_hand)
          ELSE NULL
        END AS cash_on_hand,
        CASE
          WHEN count(summary.debts_owed) = count(DISTINCT link.${linkIdentityColumn})
          THEN sum(summary.debts_owed)
          ELSE NULL
        END AS debts_owed,`
      : `
        CASE WHEN count(summary.total_receipts) = 0 THEN NULL ELSE sum(summary.total_receipts) END AS total_receipts,
        CASE
          WHEN count(summary.direct_contribution_total) = 0 THEN NULL
          ELSE sum(summary.direct_contribution_total)
        END AS direct_contribution_total,
        CASE WHEN count(summary.total_disbursements) = 0 THEN NULL ELSE sum(summary.total_disbursements) END AS total_disbursements,
        CASE WHEN count(summary.cash_on_hand) = 0 THEN NULL ELSE sum(summary.cash_on_hand) END AS cash_on_hand,`;

  // Same all-null-means-null rule as the totals above; appended only for the
  // sources that opted in, so every other query string is unchanged.
  const fundingColumns = assertFundingColumns(input.fundingColumns ?? []);
  const fundingSelect = fundingColumns
    .map(
      (column) =>
        `CASE WHEN count(summary.${column}) = 0 THEN NULL ELSE sum(summary.${column}) END AS ${column},`
    )
    .join("\n        ");

  const summaryResult = await input.db.query<StandardStateFinanceSummaryRow>(
    `
      WITH requested AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      )
      SELECT
        requested.candidate_id::text AS candidate_id,
        requested.election_id::text AS election_id,
        CASE
          WHEN count(DISTINCT link.${linkIdentityColumn}) = 1 THEN min(link.${linkIdentityColumn})
          ELSE NULL
        END AS committee_id,
        max(summary.election_year) AS election_year,${summaryAggregateColumns}
        ${fundingSelect}
        max(summary.outside_support_total) AS outside_support_total,
        max(summary.outside_oppose_total) AS outside_oppose_total,
        min(summary.source_url) FILTER (WHERE summary.source_url IS NOT NULL) AS source_url,
        max(summary.last_synced_at)::text AS last_synced_at
      FROM requested
      JOIN public.${tables.links} AS link
        ON link.candidate_id = requested.candidate_id
       AND link.election_id = requested.election_id
       AND link.link_status = 'active'
      JOIN public.${tables.summaries} AS summary
        ON summary.link_id = link.id
       AND summary.election_year = link.election_year
      GROUP BY requested.candidate_id, requested.election_id
    `,
    [JSON.stringify(requests)]
  );

  if (summaryResult.rows.length === 0) {
    return new Map();
  }

  const selectedRequests = summaryResult.rows.map((row) => ({
    candidate_id: row.candidate_id,
    election_id: row.election_id,
  }));

  const directBreakdownResult = await input.db.query<StandardStateFinanceDirectBreakdownRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.category_type,
          breakdown.category_name,
          sum(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE sum(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.${tables.links} AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.${tables.directBreakdowns} AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type IN (${directCategoryTypeList})
        GROUP BY selected.candidate_id, selected.election_id, breakdown.category_type, breakdown.category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, category_type
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, category_type, category_name, amount, contributor_count, source_url
      FROM ranked
      -- Size buckets are a fixed scheme the aggregators emit in full (up to
      -- six), so a top-5 cap would silently drop the smallest bucket rather
      -- than trim a long tail; only open-ended categories (occupation) rank.
      WHERE rn <= 5 OR category_type = 'contribution_size'
      ORDER BY candidate_id, election_id, category_type, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideGroupResult = await input.db.query<StandardStateFinanceOutsideGroupRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      grouped AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          outside_group.${outsideIdColumn} AS committee_id,
          min(outside_group.${outsideNameColumn}) AS committee_name,
          outside_group.support_oppose,
          max(outside_group.amount) AS amount,
          min(outside_group.source_url) FILTER (WHERE outside_group.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.${tables.links} AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.${tables.outsideGroups} AS outside_group
          ON outside_group.link_id = link.id
         AND outside_group.election_year = link.election_year
        GROUP BY selected.candidate_id, selected.election_id, outside_group.${outsideIdColumn}, outside_group.support_oppose
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, committee_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, committee_id, committee_name, support_oppose, amount, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, committee_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideIndustryResult = await input.db.query<StandardStateFinanceOutsideIndustryRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          breakdown.${outsideIdColumn} AS committee_id,
          breakdown.support_oppose,
          breakdown.category_name,
          max(breakdown.amount) AS amount,
          CASE
            WHEN count(breakdown.contributor_count) = 0 THEN NULL
            ELSE max(breakdown.contributor_count)
          END AS contributor_count,
          min(breakdown.source_url) FILTER (WHERE breakdown.source_url IS NOT NULL) AS source_url
        FROM selected
        JOIN public.${tables.links} AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.${tables.outsideGroupBreakdowns} AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        WHERE breakdown.category_type = 'industry'
        GROUP BY
          selected.candidate_id,
          selected.election_id,
          breakdown.${outsideIdColumn},
          breakdown.support_oppose,
          breakdown.category_name
      ),
      grouped AS (
        SELECT
          candidate_id,
          election_id,
          support_oppose,
          category_name,
          sum(amount) AS amount,
          CASE
            WHEN count(contributor_count) = 0 THEN NULL
            ELSE sum(contributor_count)
          END AS contributor_count,
          min(source_url) FILTER (WHERE source_url IS NOT NULL) AS source_url
        FROM per_group
        GROUP BY candidate_id, election_id, support_oppose, category_name
      ),
      ranked AS (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY candidate_id, election_id, support_oppose
            ORDER BY amount DESC, category_name ASC
          ) AS rn
        FROM grouped
      )
      SELECT candidate_id, election_id, support_oppose, category_name, amount, contributor_count, source_url
      FROM ranked
      WHERE rn <= 5
      ORDER BY candidate_id, election_id, support_oppose, amount DESC, category_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const outsideDonorEvidenceResult = await input.db.query<StandardStateFinanceOutsideDonorEvidenceRow>(
    `
      WITH selected AS (
        SELECT
          candidate_id::uuid AS candidate_id,
          election_id::uuid AS election_id
        FROM jsonb_to_recordset($1::jsonb) AS x(
          candidate_id text,
          election_id text
        )
      ),
      top_industries_per_group AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          industry.${outsideIdColumn} AS committee_id,
          industry.category_name AS industry_name,
          max(industry.amount) AS amount
        FROM selected
        JOIN public.${tables.links} AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.${tables.outsideGroupBreakdowns} AS industry
          ON industry.link_id = link.id
         AND industry.election_year = link.election_year
        WHERE industry.support_oppose = 'support'
          AND industry.category_type = 'industry'
        GROUP BY selected.candidate_id, selected.election_id, industry.${outsideIdColumn}, industry.category_name
      ),
      top_industries_grouped AS (
        SELECT
          candidate_id,
          election_id,
          industry_name,
          sum(amount) AS amount
        FROM top_industries_per_group
        GROUP BY candidate_id, election_id, industry_name
      ),
      top_industries AS (
        SELECT candidate_id, election_id, industry_name
        FROM (
          SELECT
            *,
            row_number() OVER (
              PARTITION BY candidate_id, election_id
              ORDER BY amount DESC, industry_name ASC
            ) AS rn
          FROM top_industries_grouped
        ) ranked_industries
        WHERE rn <= 5
      ),
      evidence AS (
        SELECT
          selected.candidate_id::text AS candidate_id,
          selected.election_id::text AS election_id,
          top_industries.industry_name,
          breakdown.${outsideIdColumn} AS committee_id,
          COALESCE(outside_group.${outsideNameColumn}, breakdown.${outsideIdColumn}) AS committee_name,
          breakdown.support_oppose,
          breakdown.category_name AS organization_name,
          breakdown.category_type AS organization_type,
          breakdown.amount,
          breakdown.contributor_count,
          COALESCE(breakdown.source_url, outside_group.source_url) AS source_url,
          row_number() OVER (
            PARTITION BY selected.candidate_id, selected.election_id, top_industries.industry_name
            ORDER BY breakdown.amount DESC, breakdown.category_name ASC, breakdown.${outsideIdColumn} ASC
          ) AS rn
        FROM selected
        JOIN top_industries
          ON top_industries.candidate_id = selected.candidate_id::text
         AND top_industries.election_id = selected.election_id::text
        JOIN public.${tables.links} AS link
          ON link.candidate_id = selected.candidate_id
         AND link.election_id = selected.election_id
         AND link.link_status = 'active'
        JOIN public.${tables.outsideGroupBreakdowns} AS breakdown
          ON breakdown.link_id = link.id
         AND breakdown.election_year = link.election_year
        CROSS JOIN LATERAL (
          SELECT
            public.normalize_finance_label(breakdown.category_name) AS normalized_label
        ) AS normalized_breakdown
        JOIN public.finance_label_classifications AS classification
          ON classification.label_type = breakdown.category_type
         AND classification.normalized_label = normalized_breakdown.normalized_label
         AND classification.industry_slug = top_industries.industry_name
        LEFT JOIN public.${tables.outsideGroups} AS outside_group
          ON outside_group.link_id = breakdown.link_id
         AND outside_group.election_year = breakdown.election_year
         AND outside_group.${outsideIdColumn} = breakdown.${outsideIdColumn}
         AND outside_group.support_oppose = breakdown.support_oppose
        WHERE breakdown.category_type IN (${evidenceLabelTypeList})
          AND breakdown.support_oppose = 'support'
      )
      SELECT candidate_id, election_id, industry_name, committee_id, committee_name, support_oppose, organization_name, organization_type, amount, contributor_count, source_url
      FROM evidence
      WHERE rn <= 3
      ORDER BY candidate_id, election_id, industry_name, amount DESC, organization_name ASC
    `,
    [JSON.stringify(selectedRequests)]
  );

  const directOccupationsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const contributionSizeBucketsByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const summaryByCandidateElection = new Map(
    summaryResult.rows.map((row) => [candidateElectionKey(row.candidate_id, row.election_id), row])
  );
  for (const row of directBreakdownResult.rows) {
    const summary = summaryByCandidateElection.get(candidateElectionKey(row.candidate_id, row.election_id));
    const targetMap =
      row.category_type === "contribution_size"
        ? contributionSizeBucketsByCandidateElection
        : directOccupationsByCandidateElection;
    addFinanceBreakdown(
      targetMap,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, summary?.source_url ?? sourceUrl)
    );
  }

  const supportingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const opposingGroupsByCandidateElection = new Map<string, BallotLookupFinanceOutsideGroup[]>();
  const outsideGroupNameByCandidateElectionCommittee = new Map<string, string>();
  for (const row of outsideGroupResult.rows) {
    const key = candidateElectionKey(row.candidate_id, row.election_id);
    const map = row.support_oppose === "support" ? supportingGroupsByCandidateElection : opposingGroupsByCandidateElection;
    const list = map.get(key) ?? [];
    list.push({
      committee_id: row.committee_id,
      committee_name: row.committee_name,
      support_oppose: row.support_oppose,
      amount: parseFinanceAmount(row.amount) ?? 0,
      source_url: firstNonEmptySourceUrl(row.source_url, sourceUrl),
    });
    map.set(key, list);
    outsideGroupNameByCandidateElectionCommittee.set(`${key}\u0000${row.committee_id}\u0000${row.support_oppose}`, row.committee_name);
  }

  const supportingIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  const opposingIndustriesByCandidateElection = new Map<string, BallotLookupFinanceBreakdown[]>();
  for (const row of outsideIndustryResult.rows) {
    const map =
      row.support_oppose === "support" ? supportingIndustriesByCandidateElection : opposingIndustriesByCandidateElection;
    addFinanceBreakdown(
      map,
      row.candidate_id,
      row.election_id,
      mapFinanceBreakdown(row, sourceUrl)
    );
  }

  const outsideIndustryEvidenceByCandidateElectionAndIndustry = new Map<
    string,
    BallotLookupFinanceOutsideIndustrySupportEvidence[]
  >();
  for (const row of outsideDonorEvidenceResult.rows) {
    const candidateKey = candidateElectionKey(row.candidate_id, row.election_id);
    const evidenceKey = `${candidateKey}\u0000${row.industry_name}`;
    const list = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
    list.push({
      organization_name: row.organization_name,
      organization_type: row.organization_type ?? "donor",
      amount: parseFinanceAmount(row.amount) ?? 0,
      contributor_count: parseFinanceCount(row.contributor_count),
      committee_id: row.committee_id,
      committee_name:
        outsideGroupNameByCandidateElectionCommittee.get(`${candidateKey}\u0000${row.committee_id}\u0000${row.support_oppose}`) ??
        row.committee_name,
      source_url: firstNonEmptySourceUrl(row.source_url, sourceUrl),
    });
    outsideIndustryEvidenceByCandidateElectionAndIndustry.set(
      evidenceKey,
      list.sort((left, right) => right.amount - left.amount || left.organization_name.localeCompare(right.organization_name)).slice(0, 5)
    );
  }

  return new Map(
    summaryResult.rows.map((row) => {
      const key = candidateElectionKey(row.candidate_id, row.election_id);
      const topDirectDonorOccupations = directOccupationsByCandidateElection.get(key) ?? [];
      const contributionSizeBuckets = contributionSizeBucketsByCandidateElection.get(key) ?? [];
      const topOutsideSupportingIndustries = (supportingIndustriesByCandidateElection.get(key) ?? []).map(
        (industry): BallotLookupFinanceOutsideIndustrySupportSummary => {
          const evidenceKey = `${key}\u0000${industry.category_name}`;
          const supportingOrganizations = outsideIndustryEvidenceByCandidateElectionAndIndustry.get(evidenceKey) ?? [];
          return {
            ...industry,
            explanation: input.outsideSupportActionLabel
              ? buildOutsideIndustrySupportExplanation(
                  industry.category_name,
                  supportingOrganizations,
                  input.outsideSupportActionLabel
                )
              : buildOutsideIndustrySupportExplanation(industry.category_name, supportingOrganizations),
            supporting_organizations: supportingOrganizations,
          };
        }
      );
      return [
        key,
        {
          source: source,
          cycle: row.election_year,
          fec_candidate_id: null,
          controlled_committee_id: row.committee_id,
          last_synced_at: row.last_synced_at,
          direct_campaign: {
            total_raised:
              summaryVariant === "illinoisD2"
                ? parseFinanceAmount(row.total_receipts) ?? parseFinanceAmount(row.direct_contribution_total)
                : parseFinanceAmount(row.direct_contribution_total) ?? parseFinanceAmount(row.total_receipts),
            total_spent: parseFinanceAmount(row.total_disbursements),
            cash_on_hand: parseFinanceAmount(row.cash_on_hand),
            debts_owed: summaryVariant === "illinoisD2" ? parseFinanceAmount(row.debts_owed) : null,
            // Omitted entirely (not null) unless the source opted in, so
            // every other state's payload is byte-identical to before.
            ...(fundingColumns.includes("loans_received")
              ? { loans_received: parseFinanceAmount(row.loans_received) }
              : {}),
            ...(fundingColumns.includes("public_funds_received")
              ? { public_funds_received: parseFinanceAmount(row.public_funds_received) }
              : {}),
            top_occupations: topDirectDonorOccupations,
            top_employers: [],
            top_industries: [],
            contribution_size_buckets: contributionSizeBuckets,
            // Omitted entirely (not null) when the source has no known gap,
            // so every other state's payload is byte-identical to before.
            ...(input.directCoverageNote === undefined
              ? {}
              : { direct_coverage_note: input.directCoverageNote }),
          },
          outside_spending: {
            support_total: parseFinanceAmount(row.outside_support_total),
            oppose_total: parseFinanceAmount(row.outside_oppose_total),
            // Omitted entirely (not null) when the source has no known gap,
            // so every other state's payload is byte-identical to before.
            ...(input.outsideCoverageNote === undefined
              ? {}
              : { outside_coverage_note: input.outsideCoverageNote }),
            top_supporting_groups: supportingGroupsByCandidateElection.get(key) ?? [],
            top_opposing_groups: opposingGroupsByCandidateElection.get(key) ?? [],
            top_supporting_industries: supportingIndustriesByCandidateElection.get(key) ?? [],
            top_opposing_industries: opposingIndustriesByCandidateElection.get(key) ?? [],
          },
          backing_summary: {
            top_direct_donor_occupations: topDirectDonorOccupations,
            top_outside_supporting_industries: topOutsideSupportingIndustries,
          },
        } satisfies BallotLookupFinanceSummary,
      ];
    })
  );
}
