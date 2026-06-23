import type { Pool, PoolClient } from "pg";

import type { FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import { upsertFinanceLabelClassification } from "../finance/financeIndustryClassificationService.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};
type ClientLikeQueryable = Queryable & {
  release?: () => void;
};

export type HawaiiFinanceLinkStatus = "active" | "inactive";
export type HawaiiFinanceLinkSource = "manual" | "csc_api";
export type HawaiiFinanceDirectCategoryType = "occupation" | "contribution_size";
export type HawaiiFinanceOutsideCategoryType = "donor" | "industry";
export type HawaiiFinanceSupportOppose = "support" | "oppose";

export type HawaiiFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  committeeId: string;
  committeeName: string;
  electionPeriod: string;
  linkStatus?: HawaiiFinanceLinkStatus;
  linkSource?: HawaiiFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type HawaiiFinanceSummaryInput = {
  totalReceipts?: number | null;
  directContributionTotal?: number | null;
  totalDisbursements?: number | null;
  cashOnHand?: number | null;
  outsideSupportTotal?: number | null;
  outsideOpposeTotal?: number | null;
  sourceUrl?: string | null;
};

export type HawaiiFinanceDirectBreakdownInput = {
  categoryType: HawaiiFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type HawaiiFinanceOutsideGroupInput = {
  committeeId: string;
  committeeName: string;
  supportOppose: HawaiiFinanceSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type HawaiiFinanceOutsideGroupBreakdownInput = {
  committeeId: string;
  supportOppose: HawaiiFinanceSupportOppose;
  categoryType: HawaiiFinanceOutsideCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type HawaiiFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: HawaiiFinanceLinkInput;
  syncedAt?: Date;
  summary?: HawaiiFinanceSummaryInput;
  directBreakdowns?: readonly HawaiiFinanceDirectBreakdownInput[];
  outsideGroups?: readonly HawaiiFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly HawaiiFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type HawaiiFinanceSnapshotWriteResult = {
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
    throw new Error(`Invalid Hawaii finance election year: ${value}`);
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

function normalizeNullableDate(value: Date | null | undefined): string | null {
  if (!value) {
    return null;
  }
  if (Number.isNaN(value.getTime())) {
    throw new Error("Invalid Hawaii finance timestamp");
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
    throw new Error("Hawaii finance contributor count must be a nonnegative integer");
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

function validateHawaiiFinanceLinkInput(link: HawaiiFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "Hawaii finance candidate name");
  requireNonEmpty(link.officeName, "Hawaii finance office name");
  requireNonEmpty(link.committeeId, "Hawaii committee id");
  requireNonEmpty(link.committeeName, "Hawaii committee name");
  requireNonEmpty(link.electionPeriod, "Hawaii election period");
  normalizeNullableDate(link.lastVerifiedAt);
}

function validateHawaiiFinanceSnapshotInput(input: HawaiiFinanceSnapshotInput): void {
  validateHawaiiFinanceLinkInput(input.link);
  if (input.outsideGroupBreakdowns && !input.outsideGroups) {
    throw new Error("Hawaii outside group breakdowns require outside groups in the same snapshot");
  }
}

async function withHawaiiFinanceTransaction<T>(db: Queryable, work: (tx: Queryable) => Promise<T>): Promise<T> {
  if (!canOpenTransaction(db)) {
    if (isClientLikeQueryable(db)) {
      throw new Error("Hawaii finance snapshot writes must receive a Pool, not a PoolClient");
    }
    throw new Error("Hawaii finance snapshot writes must receive a Pool");
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

export async function upsertHawaiiFinanceLink(input: {
  db: Queryable;
  link: HawaiiFinanceLinkInput;
}): Promise<{ linkId: string }> {
  validateHawaiiFinanceLinkInput(input.link);

  const result = await input.db.query<{ id: string }>(
    `
      INSERT INTO public.hi_candidate_finance_links (
        candidate_id,
        election_id,
        election_year,
        candidate_name_normalized,
        office_name,
        district,
        committee_id,
        committee_name,
        election_period,
        link_status,
        link_source,
        source_url,
        last_verified_at
      )
      VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz)
      ON CONFLICT (candidate_id, election_id, committee_id)
      DO UPDATE SET
        election_year = EXCLUDED.election_year,
        candidate_name_normalized = EXCLUDED.candidate_name_normalized,
        office_name = EXCLUDED.office_name,
        district = EXCLUDED.district,
        committee_name = EXCLUDED.committee_name,
        election_period = EXCLUDED.election_period,
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
      requireNonEmpty(input.link.candidateNameNormalized, "Hawaii finance candidate name"),
      requireNonEmpty(input.link.officeName, "Hawaii finance office name"),
      normalizeOptionalText(input.link.district),
      requireNonEmpty(input.link.committeeId, "Hawaii committee id"),
      requireNonEmpty(input.link.committeeName, "Hawaii committee name"),
      requireNonEmpty(input.link.electionPeriod, "Hawaii election period"),
      input.link.linkStatus ?? "active",
      input.link.linkSource ?? "manual",
      normalizeOptionalText(input.link.sourceUrl),
      normalizeNullableDate(input.link.lastVerifiedAt),
    ]
  );

  const linkId = result.rows[0]?.id;
  if (!linkId) {
    throw new Error("Hawaii finance link upsert did not return an id");
  }
  return { linkId };
}

async function upsertSummary(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  summary: HawaiiFinanceSummaryInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.hi_candidate_finance_summaries (
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
        total_receipts = COALESCE(EXCLUDED.total_receipts, hi_candidate_finance_summaries.total_receipts),
        direct_contribution_total = COALESCE(EXCLUDED.direct_contribution_total, hi_candidate_finance_summaries.direct_contribution_total),
        total_disbursements = COALESCE(EXCLUDED.total_disbursements, hi_candidate_finance_summaries.total_disbursements),
        cash_on_hand = COALESCE(EXCLUDED.cash_on_hand, hi_candidate_finance_summaries.cash_on_hand),
        outside_support_total = EXCLUDED.outside_support_total,
        outside_oppose_total = EXCLUDED.outside_oppose_total,
        source_url = COALESCE(EXCLUDED.source_url, hi_candidate_finance_summaries.source_url),
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Hawaii finance link id"),
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
  breakdown: HawaiiFinanceDirectBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.hi_candidate_finance_direct_breakdowns (
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
      requireNonEmpty(input.linkId, "Hawaii finance link id"),
      normalizeElectionYear(input.electionYear),
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "Hawaii finance direct breakdown category"),
      normalizeAmount(input.breakdown.amount, "direct breakdown amount"),
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
  breakdowns: readonly HawaiiFinanceDirectBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "Hawaii finance direct breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.hi_candidate_finance_direct_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            category_type text,
            category_name text
          )
          WHERE keep.category_type = hi_candidate_finance_direct_breakdowns.category_type
            AND keep.category_name = hi_candidate_finance_direct_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "Hawaii finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertOutsideGroup(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  group: HawaiiFinanceOutsideGroupInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.hi_candidate_finance_outside_groups (
        link_id,
        election_year,
        committee_id,
        committee_name,
        support_oppose,
        amount,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (link_id, election_year, committee_id, support_oppose)
      DO UPDATE SET
        committee_name = EXCLUDED.committee_name,
        amount = EXCLUDED.amount,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Hawaii finance link id"),
      normalizeElectionYear(input.electionYear),
      requireNonEmpty(input.group.committeeId, "Hawaii outside group committee id"),
      requireNonEmpty(input.group.committeeName, "Hawaii outside group committee name"),
      input.group.supportOppose,
      normalizeAmount(input.group.amount, "outside group amount"),
      normalizeOptionalText(input.group.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function deleteStaleOutsideGroups(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  groups: readonly HawaiiFinanceOutsideGroupInput[];
}): Promise<void> {
  const keys = input.groups.map((group) => ({
    committee_id: requireNonEmpty(group.committeeId, "Hawaii outside group committee id"),
    support_oppose: group.supportOppose,
  }));

  await input.db.query(
    `
      DELETE FROM public.hi_candidate_finance_outside_groups
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            committee_id text,
            support_oppose text
          )
          WHERE keep.committee_id = hi_candidate_finance_outside_groups.committee_id
            AND keep.support_oppose = hi_candidate_finance_outside_groups.support_oppose
        )
    `,
    [requireNonEmpty(input.linkId, "Hawaii finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertOutsideGroupBreakdown(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdown: HawaiiFinanceOutsideGroupBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.hi_candidate_finance_outside_group_breakdowns (
        link_id,
        election_year,
        committee_id,
        support_oppose,
        category_type,
        category_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz)
      ON CONFLICT (link_id, election_year, committee_id, support_oppose, category_type, category_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Hawaii finance link id"),
      normalizeElectionYear(input.electionYear),
      requireNonEmpty(input.breakdown.committeeId, "Hawaii outside breakdown committee id"),
      input.breakdown.supportOppose,
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "Hawaii outside breakdown category"),
      normalizeAmount(input.breakdown.amount, "outside group breakdown amount"),
      normalizeNullableCount(input.breakdown.contributorCount),
      normalizeOptionalText(input.breakdown.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function deleteStaleOutsideGroupBreakdowns(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdowns: readonly HawaiiFinanceOutsideGroupBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    committee_id: requireNonEmpty(breakdown.committeeId, "Hawaii outside breakdown committee id"),
    support_oppose: breakdown.supportOppose,
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "Hawaii outside breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.hi_candidate_finance_outside_group_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            committee_id text,
            support_oppose text,
            category_type text,
            category_name text
          )
          WHERE keep.committee_id = hi_candidate_finance_outside_group_breakdowns.committee_id
            AND keep.support_oppose = hi_candidate_finance_outside_group_breakdowns.support_oppose
            AND keep.category_type = hi_candidate_finance_outside_group_breakdowns.category_type
            AND keep.category_name = hi_candidate_finance_outside_group_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "Hawaii finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

export async function replaceHawaiiCandidateFinanceSnapshot(
  input: HawaiiFinanceSnapshotInput
): Promise<HawaiiFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid Hawaii finance sync timestamp");
  }
  validateHawaiiFinanceSnapshotInput(input);
  const electionYear = normalizeElectionYear(input.link.electionYear);

  return await withHawaiiFinanceTransaction(input.db, async (db) => {
    const { linkId } = await upsertHawaiiFinanceLink({ db, link: input.link });
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
      await deleteStaleOutsideGroupBreakdowns({
        db,
        linkId,
        electionYear,
        breakdowns: input.outsideGroupBreakdowns,
      });
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
