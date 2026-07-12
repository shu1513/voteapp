import type { Pool, PoolClient } from "pg";

import type { NewYorkCityFinanceDirectBreakdown } from "./newYorkCityDirectContributionAggregator.js";
import type { NewYorkCityOutsideSpendingGroup } from "./newYorkCityOutsideSpendingAggregator.js";
import type { NewYorkCityOutsideGroupBreakdown } from "./newYorkCityOutsideGroupFunderAggregator.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type Connectable = Queryable & Pick<Pool, "connect">;

export type NewYorkCityFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeCode: "1" | "2" | "3" | "4";
  boroughCode: "X" | "K" | "M" | "Q" | "S" | null;
  cfbCandidateId: string;
  cfbCandidateName: string;
  linkSource: "manual" | "cfb_csv";
  sourceUrl: string | null;
  lastVerifiedAt: Date;
};

export type NewYorkCityFinanceSummaryInput = {
  privateContributions: number | null;
  netExpenditures: number | null;
  outstandingBills: number | null;
  publicFunds: number | null;
  sourceUrl: string | null;
  lastSyncedAt: Date;
};

export type NewYorkCityFinanceSnapshotInput = {
  db: Connectable;
  link: NewYorkCityFinanceLinkInput;
  summary: NewYorkCityFinanceSummaryInput;
  breakdowns: readonly NewYorkCityFinanceDirectBreakdown[];
  outside?: {
    supportTotal: number;
    opposeTotal: number;
    groups: readonly NewYorkCityOutsideSpendingGroup[];
    breakdowns: readonly NewYorkCityOutsideGroupBreakdown[];
  };
};

function required(value: string, field: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new Error(`${field} is required`);
  return trimmed;
}

function assertMoney(value: number | null, field: string): void {
  if (value !== null && (!Number.isFinite(value) || value < 0)) {
    throw new Error(`Invalid NYC finance ${field}: ${value}`);
  }
}

function validateSnapshot(input: NewYorkCityFinanceSnapshotInput): void {
  required(input.link.candidateId, "candidate id");
  required(input.link.electionId, "election id");
  required(input.link.cfbCandidateId, "CFB candidate id");
  required(input.link.cfbCandidateName, "CFB candidate name");
  if (!Number.isInteger(input.link.electionYear) || input.link.electionYear < 2000 || input.link.electionYear > 2100) {
    throw new Error(`Invalid NYC finance election year: ${input.link.electionYear}`);
  }
  assertMoney(input.summary.privateContributions, "private contributions");
  assertMoney(input.summary.netExpenditures, "net expenditures");
  assertMoney(input.summary.outstandingBills, "outstanding bills");
  assertMoney(input.summary.publicFunds, "public funds");
  if (Number.isNaN(input.summary.lastSyncedAt.getTime())) throw new Error("Invalid NYC finance sync timestamp");
  for (const breakdown of input.breakdowns) {
    if (!Number.isFinite(breakdown.amount) || breakdown.amount < 0) throw new Error("Invalid NYC finance breakdown amount");
    required(breakdown.categoryName, "breakdown category name");
  }
  if (!input.outside) return;
  assertMoney(input.outside.supportTotal, "outside support total");
  assertMoney(input.outside.opposeTotal, "outside oppose total");
  for (const group of input.outside.groups) {
    required(group.spenderId, "outside spender id");
    required(group.spenderName, "outside spender name");
    assertMoney(group.amount, "outside group amount");
    if (!Number.isInteger(group.expenditureCount) || group.expenditureCount < 0) {
      throw new Error("Invalid NYC finance outside group expenditure count");
    }
  }
  const groupKeys = new Set(input.outside.groups.map((group) => `${group.spenderId}\u0000${group.supportOppose}`));
  for (const breakdown of input.outside.breakdowns) {
    if (!groupKeys.has(`${breakdown.spenderId}\u0000${breakdown.supportOppose}`)) {
      throw new Error("NYC finance outside breakdown has no matching outside group");
    }
    required(breakdown.categoryName, "outside breakdown category name");
    required(breakdown.sourceUrl, "outside breakdown source URL");
    assertMoney(breakdown.amount, "outside breakdown amount");
    if (breakdown.contributorCount !== null && (!Number.isInteger(breakdown.contributorCount) || breakdown.contributorCount < 0)) {
      throw new Error("Invalid NYC finance outside breakdown contributor count");
    }
  }
}

async function upsertNewYorkCityFinanceLink(input: {
  db: Queryable;
  link: NewYorkCityFinanceLinkInput;
}): Promise<string> {
  await input.db.query(
    `
      UPDATE public.nyc_candidate_finance_links
      SET link_status = 'inactive'
      WHERE candidate_id = $1
        AND election_id = $2
        AND cfb_candidate_id <> $3
        AND link_status = 'active'
        AND (link_source <> 'manual' OR $4 = 'manual')
    `,
    [input.link.candidateId, input.link.electionId, input.link.cfbCandidateId, input.link.linkSource]
  );
  const result = await input.db.query<{ id: string }>(
    `
      INSERT INTO public.nyc_candidate_finance_links (
        candidate_id, election_id, election_year, candidate_name_normalized,
        office_code, borough_code, cfb_candidate_id, cfb_candidate_name,
        link_status, link_source, source_url, last_verified_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9, $10, $11)
      ON CONFLICT (candidate_id, election_id, cfb_candidate_id)
      DO UPDATE SET
        election_year = EXCLUDED.election_year,
        candidate_name_normalized = EXCLUDED.candidate_name_normalized,
        office_code = EXCLUDED.office_code,
        borough_code = EXCLUDED.borough_code,
        cfb_candidate_name = EXCLUDED.cfb_candidate_name,
        link_status = 'active',
        link_source = CASE
          WHEN nyc_candidate_finance_links.link_source = 'manual' THEN 'manual'
          ELSE EXCLUDED.link_source
        END,
        source_url = COALESCE(EXCLUDED.source_url, nyc_candidate_finance_links.source_url),
        last_verified_at = EXCLUDED.last_verified_at
      RETURNING id::text
    `,
    [
      input.link.candidateId,
      input.link.electionId,
      input.link.electionYear,
      input.link.candidateNameNormalized,
      input.link.officeCode,
      input.link.boroughCode,
      input.link.cfbCandidateId,
      input.link.cfbCandidateName,
      input.link.linkSource,
      input.link.sourceUrl,
      input.link.lastVerifiedAt.toISOString(),
    ]
  );
  const id = result.rows[0]?.id;
  if (!id) throw new Error("NYC finance link upsert returned no id");
  return id;
}

export async function replaceNewYorkCityCandidateFinanceSnapshot(
  input: NewYorkCityFinanceSnapshotInput
): Promise<{ linkId: string; breakdownsWritten: number; outsideGroupsWritten: number }> {
  validateSnapshot(input);
  const client = await input.db.connect();
  try {
    await client.query("BEGIN");
    const linkId = await upsertNewYorkCityFinanceLink({ db: client, link: input.link });
    await client.query(
      `
        INSERT INTO public.nyc_candidate_finance_summaries (
          link_id, election_year, private_contributions, net_expenditures,
          outstanding_bills, public_funds, source_url, last_synced_at,
          outside_support_total, outside_oppose_total
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (link_id, election_year)
        DO UPDATE SET
          private_contributions = EXCLUDED.private_contributions,
          net_expenditures = EXCLUDED.net_expenditures,
          outstanding_bills = EXCLUDED.outstanding_bills,
          public_funds = EXCLUDED.public_funds,
          outside_support_total = CASE WHEN $11::boolean THEN EXCLUDED.outside_support_total ELSE nyc_candidate_finance_summaries.outside_support_total END,
          outside_oppose_total = CASE WHEN $11::boolean THEN EXCLUDED.outside_oppose_total ELSE nyc_candidate_finance_summaries.outside_oppose_total END,
          source_url = EXCLUDED.source_url,
          last_synced_at = EXCLUDED.last_synced_at
      `,
      [
        linkId,
        input.link.electionYear,
        input.summary.privateContributions,
        input.summary.netExpenditures,
        input.summary.outstandingBills,
        input.summary.publicFunds,
        input.summary.sourceUrl,
        input.summary.lastSyncedAt.toISOString(),
        input.outside?.supportTotal ?? null,
        input.outside?.opposeTotal ?? null,
        Boolean(input.outside),
      ]
    );
    await client.query(
      "DELETE FROM public.nyc_candidate_finance_direct_breakdowns WHERE link_id = $1 AND election_year = $2",
      [linkId, input.link.electionYear]
    );
    if (input.breakdowns.length > 0) {
      await client.query(
        `
          INSERT INTO public.nyc_candidate_finance_direct_breakdowns (
            link_id, election_year, category_type, category_name, amount,
            contributor_count, source_url, last_synced_at
          )
          SELECT $1, $2, row.category_type, row.category_name, row.amount,
                 row.contributor_count, row.source_url, $3
          FROM jsonb_to_recordset($4::jsonb) AS row(
            category_type text,
            category_name text,
            amount numeric,
            contributor_count integer,
            source_url text
          )
        `,
        [
          linkId,
          input.link.electionYear,
          input.summary.lastSyncedAt.toISOString(),
          JSON.stringify(input.breakdowns.map((row) => ({
            category_type: row.categoryType,
            category_name: row.categoryName,
            amount: row.amount,
            contributor_count: row.contributorCount,
            source_url: row.sourceUrl,
          }))),
        ]
      );
    }
    if (input.outside) {
      await client.query(
        "DELETE FROM public.nyc_candidate_finance_outside_groups WHERE link_id = $1 AND election_year = $2",
        [linkId, input.link.electionYear]
      );
    }
    if (input.outside && input.outside.groups.length > 0) {
      await client.query(
        `
          INSERT INTO public.nyc_candidate_finance_outside_groups (
            link_id, election_year, spender_id, spender_name, support_oppose,
            amount, expenditure_count, source_url, last_synced_at
          )
          SELECT $1, $2, row.spender_id, row.spender_name, row.support_oppose,
                 row.amount, row.expenditure_count, row.source_url, $3
          FROM jsonb_to_recordset($4::jsonb) AS row(
            spender_id text,
            spender_name text,
            support_oppose text,
            amount numeric,
            expenditure_count integer,
            source_url text
          )
        `,
        [
          linkId,
          input.link.electionYear,
          input.summary.lastSyncedAt.toISOString(),
          JSON.stringify(input.outside.groups.map((row) => ({
            spender_id: row.spenderId,
            spender_name: row.spenderName,
            support_oppose: row.supportOppose,
            amount: row.amount,
            expenditure_count: row.expenditureCount,
            source_url: row.sourceUrl,
          }))),
        ]
      );
    }
    if (input.outside && input.outside.breakdowns.length > 0) {
      await client.query(
        `
          INSERT INTO public.nyc_candidate_finance_outside_group_breakdowns (
            link_id, election_year, spender_id, support_oppose, category_type,
            category_name, amount, contributor_count, source_url, last_synced_at
          )
          SELECT $1, $2, row.spender_id, row.support_oppose, row.category_type,
                 row.category_name, row.amount, row.contributor_count, row.source_url, $3
          FROM jsonb_to_recordset($4::jsonb) AS row(
            spender_id text,
            support_oppose text,
            category_type text,
            category_name text,
            amount numeric,
            contributor_count integer,
            source_url text
          )
        `,
        [
          linkId,
          input.link.electionYear,
          input.summary.lastSyncedAt.toISOString(),
          JSON.stringify(input.outside.breakdowns.map((row) => ({
            spender_id: row.spenderId,
            support_oppose: row.supportOppose,
            category_type: row.categoryType,
            category_name: row.categoryName,
            amount: row.amount,
            contributor_count: row.contributorCount,
            source_url: row.sourceUrl,
          }))),
        ]
      );
    }
    await client.query("COMMIT");
    return {
      linkId,
      breakdownsWritten: input.breakdowns.length,
      outsideGroupsWritten: input.outside?.groups.length ?? 0,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
