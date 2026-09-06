import type { Pool, PoolClient } from "pg";
import {
  MANUAL_PROTECTED_LINK_RETURNING,
  assertLinkWriteNotBlocked,
  manualProtectedLinkAssignments,
  type ManualProtectedLinkRow,
} from "../finance/manualLinkProtection.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};
type ClientLikeQueryable = Queryable & {
  release?: () => void;
};

export type AlaskaFinanceLinkStatus = "active" | "inactive";
export type AlaskaFinanceLinkSource = "manual" | "apoc_csv";
export type AlaskaFinanceDirectCategoryType = "occupation" | "contribution_size";
export type AlaskaFinanceOutsideCategoryType = "donor" | "industry";
export type AlaskaFinanceSupportOppose = "support" | "oppose";

export type AlaskaFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  candidateFilerId: string;
  candidateFilerName: string;
  linkStatus?: AlaskaFinanceLinkStatus;
  linkSource?: AlaskaFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type AlaskaFinanceSummaryInput = {
  totalReceipts?: number | null;
  directContributionTotal?: number | null;
  totalDisbursements?: number | null;
  cashOnHand?: number | null;
  outsideSupportTotal?: number | null;
  outsideOpposeTotal?: number | null;
  sourceUrl?: string | null;
};

export type AlaskaFinanceDirectBreakdownInput = {
  categoryType: AlaskaFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type AlaskaFinanceOutsideGroupInput = {
  outsideGroupId: string;
  outsideGroupName: string;
  supportOppose: AlaskaFinanceSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type AlaskaFinanceOutsideGroupBreakdownInput = {
  outsideGroupId: string;
  supportOppose: AlaskaFinanceSupportOppose;
  categoryType: AlaskaFinanceOutsideCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type AlaskaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: AlaskaFinanceLinkInput;
  syncedAt?: Date;
  summary?: AlaskaFinanceSummaryInput;
  directBreakdowns?: readonly AlaskaFinanceDirectBreakdownInput[];
  outsideGroups?: readonly AlaskaFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly AlaskaFinanceOutsideGroupBreakdownInput[];
};

export type AlaskaFinanceSnapshotWriteResult = {
  linkId: string;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Alaska finance election year: ${value}`);
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
    throw new Error("Invalid Alaska finance timestamp");
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
    throw new Error("Alaska finance contributor count must be a nonnegative integer");
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

function validateAlaskaFinanceLinkInput(link: AlaskaFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "Alaska finance candidate name");
  requireNonEmpty(link.officeName, "Alaska finance office name");
  requireNonEmpty(link.candidateFilerId, "Alaska candidate filer id");
  requireNonEmpty(link.candidateFilerName, "Alaska candidate filer name");
  normalizeNullableDate(link.lastVerifiedAt);
}

function validateAlaskaFinanceSnapshotInput(input: AlaskaFinanceSnapshotInput): void {
  validateAlaskaFinanceLinkInput(input.link);
  const outsideBreakdownCount = input.outsideGroupBreakdowns?.length ?? 0;
  const outsideGroupCount = input.outsideGroups?.length ?? 0;
  if (outsideBreakdownCount > 0 && outsideGroupCount === 0) {
    throw new Error("Alaska outside group breakdowns require outside groups in the same snapshot");
  }
  if (outsideBreakdownCount > 0) {
    const groupKeys = new Set(
      (input.outsideGroups ?? []).map(
        (group) =>
          `${requireNonEmpty(group.outsideGroupId, "Alaska outside group id")}\u0000${group.supportOppose}`
      )
    );
    for (const breakdown of input.outsideGroupBreakdowns ?? []) {
      const key = `${requireNonEmpty(breakdown.outsideGroupId, "Alaska outside breakdown group id")}\u0000${breakdown.supportOppose}`;
      if (!groupKeys.has(key)) {
        throw new Error("Alaska outside group breakdowns must reference outside groups in the same snapshot");
      }
    }
  }
}

async function withAlaskaFinanceTransaction<T>(db: Queryable, work: (tx: Queryable) => Promise<T>): Promise<T> {
  if (!canOpenTransaction(db)) {
    if (isClientLikeQueryable(db)) {
      throw new Error("Alaska finance snapshot writes must receive a Pool, not a PoolClient");
    }
    throw new Error("Alaska finance snapshot writes must receive a Pool");
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

export async function upsertAlaskaFinanceLink(input: {
  db: Queryable;
  link: AlaskaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  validateAlaskaFinanceLinkInput(input.link);

  const result = await input.db.query<ManualProtectedLinkRow>(
    `
      INSERT INTO public.ak_candidate_finance_links (
        candidate_id,
        election_id,
        election_year,
        candidate_name_normalized,
        office_name,
        district,
        candidate_filer_id,
        candidate_filer_name,
        link_status,
        link_source,
        source_url,
        last_verified_at
      )
      VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::timestamptz)
      ON CONFLICT (candidate_id, election_id, candidate_filer_id)
      DO UPDATE SET
        election_year = EXCLUDED.election_year,
        candidate_name_normalized = EXCLUDED.candidate_name_normalized,
        office_name = EXCLUDED.office_name,
        district = EXCLUDED.district,
        candidate_filer_name = EXCLUDED.candidate_filer_name,
        ${manualProtectedLinkAssignments("ak_candidate_finance_links")},
        source_url = EXCLUDED.source_url,
        last_verified_at = EXCLUDED.last_verified_at
      RETURNING ${MANUAL_PROTECTED_LINK_RETURNING}
    `,
    [
      requireNonEmpty(input.link.candidateId, "candidate id"),
      requireNonEmpty(input.link.electionId, "election id"),
      normalizeElectionYear(input.link.electionYear),
      requireNonEmpty(input.link.candidateNameNormalized, "Alaska finance candidate name"),
      requireNonEmpty(input.link.officeName, "Alaska finance office name"),
      normalizeOptionalText(input.link.district),
      requireNonEmpty(input.link.candidateFilerId, "Alaska candidate filer id"),
      requireNonEmpty(input.link.candidateFilerName, "Alaska candidate filer name"),
      input.link.linkStatus ?? "active",
      input.link.linkSource ?? "manual",
      normalizeOptionalText(input.link.sourceUrl),
      normalizeNullableDate(input.link.lastVerifiedAt),
    ]
  );

  assertLinkWriteNotBlocked("Alaska", result.rows[0], input.link.linkSource ?? "manual");
  const linkId = result.rows[0]?.id;
  if (!linkId) {
    throw new Error("Alaska finance link upsert did not return an id");
  }
  return { linkId };
}

async function upsertSummary(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  summary: AlaskaFinanceSummaryInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ak_candidate_finance_summaries (
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
        total_receipts = EXCLUDED.total_receipts,
        direct_contribution_total = EXCLUDED.direct_contribution_total,
        total_disbursements = EXCLUDED.total_disbursements,
        cash_on_hand = EXCLUDED.cash_on_hand,
        outside_support_total = EXCLUDED.outside_support_total,
        outside_oppose_total = EXCLUDED.outside_oppose_total,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Alaska finance link id"),
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

async function deleteSummary(input: { db: Queryable; linkId: string; electionYear: number }): Promise<void> {
  await input.db.query(
    `
      DELETE FROM public.ak_candidate_finance_summaries
      WHERE link_id = $1::uuid
        AND election_year = $2
    `,
    [requireNonEmpty(input.linkId, "Alaska finance link id"), normalizeElectionYear(input.electionYear)]
  );
}

async function upsertDirectBreakdown(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdown: AlaskaFinanceDirectBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ak_candidate_finance_direct_breakdowns (
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
      requireNonEmpty(input.linkId, "Alaska finance link id"),
      normalizeElectionYear(input.electionYear),
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "Alaska finance direct breakdown category"),
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
  breakdowns: readonly AlaskaFinanceDirectBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "Alaska finance direct breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.ak_candidate_finance_direct_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            category_type text,
            category_name text
          )
          WHERE keep.category_type = ak_candidate_finance_direct_breakdowns.category_type
            AND keep.category_name = ak_candidate_finance_direct_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "Alaska finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertOutsideGroup(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  group: AlaskaFinanceOutsideGroupInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ak_candidate_finance_outside_groups (
        link_id,
        election_year,
        outside_group_id,
        outside_group_name,
        support_oppose,
        amount,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (link_id, election_year, outside_group_id, support_oppose)
      DO UPDATE SET
        outside_group_name = EXCLUDED.outside_group_name,
        amount = EXCLUDED.amount,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Alaska finance link id"),
      normalizeElectionYear(input.electionYear),
      requireNonEmpty(input.group.outsideGroupId, "Alaska outside group id"),
      requireNonEmpty(input.group.outsideGroupName, "Alaska outside group name"),
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
  groups: readonly AlaskaFinanceOutsideGroupInput[];
}): Promise<void> {
  const keys = input.groups.map((group) => ({
    outside_group_id: requireNonEmpty(group.outsideGroupId, "Alaska outside group id"),
    support_oppose: group.supportOppose,
  }));

  await input.db.query(
    `
      DELETE FROM public.ak_candidate_finance_outside_groups
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            outside_group_id text,
            support_oppose text
          )
          WHERE keep.outside_group_id = ak_candidate_finance_outside_groups.outside_group_id
            AND keep.support_oppose = ak_candidate_finance_outside_groups.support_oppose
        )
    `,
    [requireNonEmpty(input.linkId, "Alaska finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertOutsideGroupBreakdown(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdown: AlaskaFinanceOutsideGroupBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ak_candidate_finance_outside_group_breakdowns (
        link_id,
        election_year,
        outside_group_id,
        support_oppose,
        category_type,
        category_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz)
      ON CONFLICT (link_id, election_year, outside_group_id, support_oppose, category_type, category_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Alaska finance link id"),
      normalizeElectionYear(input.electionYear),
      requireNonEmpty(input.breakdown.outsideGroupId, "Alaska outside breakdown group id"),
      input.breakdown.supportOppose,
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "Alaska outside breakdown category"),
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
  breakdowns: readonly AlaskaFinanceOutsideGroupBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    outside_group_id: requireNonEmpty(breakdown.outsideGroupId, "Alaska outside breakdown group id"),
    support_oppose: breakdown.supportOppose,
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "Alaska outside breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.ak_candidate_finance_outside_group_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            outside_group_id text,
            support_oppose text,
            category_type text,
            category_name text
          )
          WHERE keep.outside_group_id = ak_candidate_finance_outside_group_breakdowns.outside_group_id
            AND keep.support_oppose = ak_candidate_finance_outside_group_breakdowns.support_oppose
            AND keep.category_type = ak_candidate_finance_outside_group_breakdowns.category_type
            AND keep.category_name = ak_candidate_finance_outside_group_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "Alaska finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

export async function replaceAlaskaCandidateFinanceSnapshot(
  input: AlaskaFinanceSnapshotInput
): Promise<AlaskaFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid Alaska finance sync timestamp");
  }
  validateAlaskaFinanceSnapshotInput(input);
  const electionYear = normalizeElectionYear(input.link.electionYear);
  const directBreakdowns = input.directBreakdowns ?? [];
  const outsideGroups = input.outsideGroups ?? [];
  const outsideGroupBreakdowns = input.outsideGroupBreakdowns ?? [];

  return await withAlaskaFinanceTransaction(input.db, async (db) => {
    const { linkId } = await upsertAlaskaFinanceLink({ db, link: input.link });
    if (input.summary) {
      await upsertSummary({ db, linkId, electionYear, summary: input.summary, syncedAt });
    } else {
      await deleteSummary({ db, linkId, electionYear });
    }

    for (const breakdown of directBreakdowns) {
      await upsertDirectBreakdown({ db, linkId, electionYear, breakdown, syncedAt });
    }
    await deleteStaleDirectBreakdowns({ db, linkId, electionYear, breakdowns: directBreakdowns });

    for (const group of outsideGroups) {
      await upsertOutsideGroup({ db, linkId, electionYear, group, syncedAt });
    }
    for (const breakdown of outsideGroupBreakdowns) {
      await upsertOutsideGroupBreakdown({ db, linkId, electionYear, breakdown, syncedAt });
    }
    await deleteStaleOutsideGroupBreakdowns({ db, linkId, electionYear, breakdowns: outsideGroupBreakdowns });
    await deleteStaleOutsideGroups({ db, linkId, electionYear, groups: outsideGroups });

    return {
      linkId,
      summaryWritten: Boolean(input.summary),
      directBreakdownsWritten: directBreakdowns.length,
      outsideGroupsWritten: outsideGroups.length,
      outsideGroupBreakdownsWritten: outsideGroupBreakdowns.length,
    };
  });
}
