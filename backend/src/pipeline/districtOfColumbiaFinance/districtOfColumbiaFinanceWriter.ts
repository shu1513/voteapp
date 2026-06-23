import type { Pool, PoolClient } from "pg";

import type { FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import { upsertFinanceLabelClassification } from "../finance/financeIndustryClassificationService.js";

export type DistrictOfColumbiaFinanceLinkStatus = "active" | "inactive";
export type DistrictOfColumbiaFinanceLinkSource = "manual" | "ocf_export";
export type DistrictOfColumbiaFinanceDirectCategoryType = "occupation" | "contribution_size";
export type DistrictOfColumbiaFinanceOutsideCategoryType = "donor" | "industry";
export type DistrictOfColumbiaFinanceSupportOppose = "support" | "oppose";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};
type ClientLikeQueryable = Queryable & {
  release?: () => void;
};

export type DistrictOfColumbiaFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  committeeKey: string;
  committeeName: string;
  linkStatus?: DistrictOfColumbiaFinanceLinkStatus;
  linkSource?: DistrictOfColumbiaFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type DistrictOfColumbiaFinanceSummaryInput = {
  totalReceipts?: number | null;
  directContributionTotal?: number | null;
  totalDisbursements?: number | null;
  cashOnHand?: number | null;
  outsideSupportTotal?: number | null;
  outsideOpposeTotal?: number | null;
  sourceUrl?: string | null;
};

export type DistrictOfColumbiaFinanceDirectBreakdownInput = {
  categoryType: DistrictOfColumbiaFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type DistrictOfColumbiaFinanceOutsideGroupInput = {
  committeeKey: string;
  committeeName: string;
  supportOppose: DistrictOfColumbiaFinanceSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type DistrictOfColumbiaFinanceOutsideGroupBreakdownInput = {
  committeeKey: string;
  supportOppose: DistrictOfColumbiaFinanceSupportOppose;
  categoryType: DistrictOfColumbiaFinanceOutsideCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type DistrictOfColumbiaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: DistrictOfColumbiaFinanceLinkInput;
  syncedAt?: Date;
  summary?: DistrictOfColumbiaFinanceSummaryInput;
  directBreakdowns?: readonly DistrictOfColumbiaFinanceDirectBreakdownInput[];
  outsideGroups?: readonly DistrictOfColumbiaFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly DistrictOfColumbiaFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type DistrictOfColumbiaFinanceSnapshotWriteResult = {
  linkId: string;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid D.C. finance election year: ${value}`);
  }
  return value;
}

function normalizeOptionalText(value: string | null | undefined): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeCommitteeKey(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
}

function normalizeNullableDate(value: Date | null | undefined): string | null {
  if (!value) {
    return null;
  }
  if (Number.isNaN(value.getTime())) {
    throw new Error("Invalid D.C. finance timestamp");
  }
  return value.toISOString();
}

function normalizeAmount(value: number, fieldName: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${fieldName} must be a nonnegative number`);
  }
  return value;
}

function normalizeNullableAmount(value: number | null | undefined, fieldName: string): number | null {
  if (value === undefined || value === null) {
    return null;
  }
  return normalizeAmount(value, fieldName);
}

function normalizeNullableCount(value: number | null | undefined): number | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (!Number.isInteger(value) || value < 0) {
    throw new Error("D.C. finance contributor count must be a nonnegative integer");
  }
  return value;
}

function canOpenTransaction(db: Queryable): db is ConnectableQueryable {
  return (
    typeof (db as ConnectableQueryable).connect === "function" &&
    typeof (db as ClientLikeQueryable).release !== "function"
  );
}

function isClientLikeQueryable(db: Queryable): db is ClientLikeQueryable {
  return typeof (db as ClientLikeQueryable).release === "function";
}

function validateDistrictOfColumbiaFinanceLinkInput(link: DistrictOfColumbiaFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "D.C. finance candidate name");
  requireNonEmpty(link.officeName, "D.C. finance office name");
  requireNonEmpty(link.committeeKey, "D.C. committee key");
  requireNonEmpty(link.committeeName, "D.C. committee name");
  normalizeNullableDate(link.lastVerifiedAt);
}

function outsideGroupKey(group: Pick<DistrictOfColumbiaFinanceOutsideGroupInput, "committeeKey" | "supportOppose">): string {
  return `${normalizeCommitteeKey(group.committeeKey)}\u0000${group.supportOppose}`;
}

function validateDistrictOfColumbiaFinanceSnapshotInput(input: DistrictOfColumbiaFinanceSnapshotInput): void {
  validateDistrictOfColumbiaFinanceLinkInput(input.link);
  if (input.outsideGroupBreakdowns && !input.outsideGroups) {
    throw new Error("D.C. outside group breakdowns require outside groups in the same snapshot");
  }
  if (input.outsideGroupBreakdowns?.length) {
    const groupKeys = new Set((input.outsideGroups ?? []).map(outsideGroupKey));
    for (const breakdown of input.outsideGroupBreakdowns) {
      if (!groupKeys.has(outsideGroupKey(breakdown))) {
        throw new Error("D.C. outside group breakdowns require matching outside groups in the same snapshot");
      }
    }
  }
}

async function withDistrictOfColumbiaFinanceTransaction<T>(
  db: Queryable,
  work: (tx: Queryable) => Promise<T>
): Promise<T> {
  if (!canOpenTransaction(db)) {
    if (isClientLikeQueryable(db)) {
      throw new Error("D.C. finance snapshot writes must receive a Pool, not a PoolClient");
    }
    throw new Error("D.C. finance snapshot writes must receive a Pool");
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await work(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // Preserve the original write failure.
    }
    throw error;
  } finally {
    client.release();
  }
}

export async function upsertDistrictOfColumbiaFinanceLink(input: {
  db: Queryable;
  link: DistrictOfColumbiaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  validateDistrictOfColumbiaFinanceLinkInput(input.link);

  const result = await input.db.query<{ id: string }>(
    `
      INSERT INTO public.dc_candidate_finance_links (
        candidate_id,
        election_id,
        election_year,
        candidate_name_normalized,
        office_name,
        district,
        committee_key,
        committee_name,
        link_status,
        link_source,
        source_url,
        last_verified_at
      )
      VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::timestamptz)
      ON CONFLICT (candidate_id, election_id, committee_key)
      DO UPDATE SET
        election_year = EXCLUDED.election_year,
        candidate_name_normalized = EXCLUDED.candidate_name_normalized,
        office_name = EXCLUDED.office_name,
        district = EXCLUDED.district,
        committee_name = EXCLUDED.committee_name,
        link_status = EXCLUDED.link_status,
        link_source = EXCLUDED.link_source,
        source_url = EXCLUDED.source_url,
        last_verified_at = EXCLUDED.last_verified_at
      RETURNING id
    `,
    [
      requireNonEmpty(input.link.candidateId, "candidate id"),
      requireNonEmpty(input.link.electionId, "election id"),
      normalizeElectionYear(input.link.electionYear),
      requireNonEmpty(input.link.candidateNameNormalized, "D.C. finance candidate name"),
      requireNonEmpty(input.link.officeName, "D.C. finance office name"),
      normalizeOptionalText(input.link.district),
      requireNonEmpty(input.link.committeeKey, "D.C. committee key"),
      requireNonEmpty(input.link.committeeName, "D.C. committee name"),
      input.link.linkStatus ?? "active",
      input.link.linkSource ?? "manual",
      normalizeOptionalText(input.link.sourceUrl),
      normalizeNullableDate(input.link.lastVerifiedAt),
    ]
  );

  const linkId = result.rows[0]?.id;
  if (!linkId) {
    throw new Error("D.C. finance link upsert did not return an id");
  }
  return { linkId };
}

async function upsertSummary(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  summary: DistrictOfColumbiaFinanceSummaryInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.dc_candidate_finance_summaries (
        link_id,
        election_year,
        total_receipts,
        direct_contribution_total,
        total_disbursements,
        cash_on_hand,
        outside_support_total,
        outside_oppose_total,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz)
      ON CONFLICT (link_id, election_year)
      DO UPDATE SET
        total_receipts = COALESCE(EXCLUDED.total_receipts, dc_candidate_finance_summaries.total_receipts),
        direct_contribution_total = COALESCE(EXCLUDED.direct_contribution_total, dc_candidate_finance_summaries.direct_contribution_total),
        total_disbursements = COALESCE(EXCLUDED.total_disbursements, dc_candidate_finance_summaries.total_disbursements),
        cash_on_hand = COALESCE(EXCLUDED.cash_on_hand, dc_candidate_finance_summaries.cash_on_hand),
        outside_support_total = COALESCE(EXCLUDED.outside_support_total, dc_candidate_finance_summaries.outside_support_total),
        outside_oppose_total = COALESCE(EXCLUDED.outside_oppose_total, dc_candidate_finance_summaries.outside_oppose_total),
        source_url = COALESCE(EXCLUDED.source_url, dc_candidate_finance_summaries.source_url),
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "D.C. finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeNullableAmount(input.summary.totalReceipts, "total receipts"),
      normalizeNullableAmount(input.summary.directContributionTotal, "direct contribution total"),
      normalizeNullableAmount(input.summary.totalDisbursements, "total disbursements"),
      normalizeNullableAmount(input.summary.cashOnHand, "cash on hand"),
      normalizeNullableAmount(input.summary.outsideSupportTotal, "outside support total"),
      normalizeNullableAmount(input.summary.outsideOpposeTotal, "outside oppose total"),
      normalizeOptionalText(input.summary.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertDirectBreakdown(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdown: DistrictOfColumbiaFinanceDirectBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.dc_candidate_finance_direct_breakdowns (
        link_id,
        election_year,
        category_type,
        category_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (link_id, election_year, category_type, category_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "D.C. finance link id"),
      normalizeElectionYear(input.electionYear),
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "D.C. finance direct breakdown category"),
      normalizeAmount(input.breakdown.amount, "direct breakdown amount"),
      normalizeNullableCount(input.breakdown.contributorCount),
      normalizeOptionalText(input.breakdown.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertOutsideGroup(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  group: DistrictOfColumbiaFinanceOutsideGroupInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.dc_candidate_finance_outside_groups (
        link_id,
        election_year,
        committee_key,
        committee_name,
        support_oppose,
        amount,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (link_id, election_year, committee_key, support_oppose)
      DO UPDATE SET
        committee_name = EXCLUDED.committee_name,
        amount = EXCLUDED.amount,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "D.C. finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeCommitteeKey(requireNonEmpty(input.group.committeeKey, "D.C. outside group committee key")),
      requireNonEmpty(input.group.committeeName, "D.C. outside group committee name"),
      input.group.supportOppose,
      normalizeAmount(input.group.amount, "outside group amount"),
      normalizeOptionalText(input.group.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertOutsideGroupBreakdown(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdown: DistrictOfColumbiaFinanceOutsideGroupBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.dc_candidate_finance_outside_group_breakdowns (
        link_id,
        election_year,
        committee_key,
        support_oppose,
        category_type,
        category_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz)
      ON CONFLICT (link_id, election_year, committee_key, support_oppose, category_type, category_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "D.C. finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeCommitteeKey(requireNonEmpty(input.breakdown.committeeKey, "D.C. outside breakdown committee key")),
      input.breakdown.supportOppose,
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "D.C. outside breakdown category"),
      normalizeAmount(input.breakdown.amount, "outside group breakdown amount"),
      normalizeNullableCount(input.breakdown.contributorCount),
      normalizeOptionalText(input.breakdown.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function deleteStaleDirectBreakdowns(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdowns: readonly DistrictOfColumbiaFinanceDirectBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "D.C. finance direct breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.dc_candidate_finance_direct_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            category_type text,
            category_name text
          )
          WHERE keep.category_type = dc_candidate_finance_direct_breakdowns.category_type
            AND keep.category_name = dc_candidate_finance_direct_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "D.C. finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function deleteStaleOutsideGroupBreakdowns(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdowns: readonly DistrictOfColumbiaFinanceOutsideGroupBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    committee_key: normalizeCommitteeKey(requireNonEmpty(breakdown.committeeKey, "D.C. outside breakdown committee key")),
    support_oppose: breakdown.supportOppose,
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "D.C. outside breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.dc_candidate_finance_outside_group_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            committee_key text,
            support_oppose text,
            category_type text,
            category_name text
          )
          WHERE keep.committee_key = dc_candidate_finance_outside_group_breakdowns.committee_key
            AND keep.support_oppose = dc_candidate_finance_outside_group_breakdowns.support_oppose
            AND keep.category_type = dc_candidate_finance_outside_group_breakdowns.category_type
            AND keep.category_name = dc_candidate_finance_outside_group_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "D.C. finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function deleteStaleOutsideGroups(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  groups: readonly DistrictOfColumbiaFinanceOutsideGroupInput[];
}): Promise<void> {
  const keys = input.groups.map((group) => ({
    committee_key: normalizeCommitteeKey(requireNonEmpty(group.committeeKey, "D.C. outside group committee key")),
    support_oppose: group.supportOppose,
  }));

  await input.db.query(
    `
      DELETE FROM public.dc_candidate_finance_outside_groups
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            committee_key text,
            support_oppose text
          )
          WHERE keep.committee_key = dc_candidate_finance_outside_groups.committee_key
            AND keep.support_oppose = dc_candidate_finance_outside_groups.support_oppose
        )
    `,
    [requireNonEmpty(input.linkId, "D.C. finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

export async function replaceDistrictOfColumbiaCandidateFinanceSnapshot(
  input: DistrictOfColumbiaFinanceSnapshotInput
): Promise<DistrictOfColumbiaFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid D.C. finance sync timestamp");
  }
  validateDistrictOfColumbiaFinanceSnapshotInput(input);
  const electionYear = normalizeElectionYear(input.link.electionYear);

  return await withDistrictOfColumbiaFinanceTransaction(input.db, async (db) => {
    const { linkId } = await upsertDistrictOfColumbiaFinanceLink({ db, link: input.link });
    if (input.summary) {
      await upsertSummary({ db, linkId, electionYear, summary: input.summary, syncedAt });
    }

    for (const breakdown of input.directBreakdowns ?? []) {
      await upsertDirectBreakdown({ db, linkId, electionYear, breakdown, syncedAt });
    }
    if (input.directBreakdowns) {
      await deleteStaleDirectBreakdowns({ db, linkId, electionYear, breakdowns: input.directBreakdowns });
    }

    for (const group of input.outsideGroups ?? []) {
      await upsertOutsideGroup({ db, linkId, electionYear, group, syncedAt });
    }
    for (const breakdown of input.outsideGroupBreakdowns ?? []) {
      await upsertOutsideGroupBreakdown({ db, linkId, electionYear, breakdown, syncedAt });
    }
    if (input.outsideGroupBreakdowns) {
      await deleteStaleOutsideGroupBreakdowns({ db, linkId, electionYear, breakdowns: input.outsideGroupBreakdowns });
    }
    if (input.outsideGroups) {
      await deleteStaleOutsideGroups({ db, linkId, electionYear, groups: input.outsideGroups });
    }

    for (const classification of input.classifications ?? []) {
      await upsertFinanceLabelClassification({ db, classification });
    }

    return {
      linkId,
      summaryWritten: Boolean(input.summary),
      directBreakdownsWritten: input.directBreakdowns?.length ?? 0,
      outsideGroupsWritten: input.outsideGroups?.length ?? 0,
      outsideGroupBreakdownsWritten: input.outsideGroupBreakdowns?.length ?? 0,
    };
  });
}
