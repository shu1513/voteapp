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

export type NewJerseyFinanceLinkStatus = "active" | "inactive";
export type NewJerseyFinanceLinkSource = "manual" | "elec_api";
export type NewJerseyFinanceDirectCategoryType = "occupation" | "employer" | "contribution_size";
export type NewJerseyFinanceOutsideCategoryType = "donor" | "contributor_type" | "occupation" | "industry";
export type NewJerseyFinanceSupportOppose = "support" | "oppose";

export type NewJerseyFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  candidateEntityS: number;
  entityName: string;
  electionTypeCode?: string | null;
  linkStatus?: NewJerseyFinanceLinkStatus;
  linkSource?: NewJerseyFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type NewJerseyFinanceSummaryInput = {
  totalReceipts?: number | null;
  directContributionTotal?: number | null;
  totalDisbursements?: number | null;
  cashOnHand?: number | null;
  outsideSupportTotal?: number | null;
  outsideOpposeTotal?: number | null;
  sourceUrl?: string | null;
};

export type NewJerseyFinanceDirectBreakdownInput = {
  categoryType: NewJerseyFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type NewJerseyFinanceOutsideGroupInput = {
  outsideEntityS: number;
  outsideEntityName: string;
  supportOppose: NewJerseyFinanceSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type NewJerseyFinanceOutsideGroupBreakdownInput = {
  outsideEntityS: number;
  supportOppose: NewJerseyFinanceSupportOppose;
  categoryType: NewJerseyFinanceOutsideCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type NewJerseyFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: NewJerseyFinanceLinkInput;
  syncedAt?: Date;
  summary?: NewJerseyFinanceSummaryInput;
  directBreakdowns?: readonly NewJerseyFinanceDirectBreakdownInput[];
  outsideGroups?: readonly NewJerseyFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly NewJerseyFinanceOutsideGroupBreakdownInput[];
};

export type NewJerseyFinanceSnapshotWriteResult = {
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
  if (!Number.isInteger(value) || value < 1980 || value > 2100) {
    throw new Error(`Invalid New Jersey finance election year: ${value}`);
  }
  return value;
}

function normalizeEntityS(value: number, fieldName: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${fieldName} must be a positive integer`);
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
    throw new Error("Invalid New Jersey finance timestamp");
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
    throw new Error("New Jersey finance contributor count must be a nonnegative integer");
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

function validateNewJerseyFinanceLinkInput(link: NewJerseyFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "New Jersey finance candidate name");
  requireNonEmpty(link.officeName, "New Jersey finance office name");
  normalizeEntityS(link.candidateEntityS, "New Jersey candidate ENTITY_S");
  requireNonEmpty(link.entityName, "New Jersey candidate entity name");
  normalizeNullableDate(link.lastVerifiedAt);
}

function validateNewJerseyFinanceSnapshotInput(input: NewJerseyFinanceSnapshotInput): void {
  validateNewJerseyFinanceLinkInput(input.link);
  const outsideGroupBreakdowns = input.outsideGroupBreakdowns ?? [];
  const outsideGroups = input.outsideGroups ?? [];
  if (outsideGroupBreakdowns.length > 0 && outsideGroups.length === 0) {
    throw new Error("New Jersey outside group breakdowns require outside groups in the same snapshot");
  }
  const outsideGroupKeys = new Set(
    outsideGroups.map((group) => `${normalizeEntityS(group.outsideEntityS, "New Jersey outside group ENTITY_S")}\u0000${group.supportOppose}`)
  );
  for (const breakdown of outsideGroupBreakdowns) {
    const key = `${normalizeEntityS(
      breakdown.outsideEntityS,
      "New Jersey outside group breakdown ENTITY_S"
    )}\u0000${breakdown.supportOppose}`;
    if (!outsideGroupKeys.has(key)) {
      throw new Error("New Jersey outside group breakdowns require matching outside groups in the same snapshot");
    }
  }
}

async function withNewJerseyFinanceTransaction<T>(db: Queryable, work: (tx: Queryable) => Promise<T>): Promise<T> {
  if (!canOpenTransaction(db)) {
    if (isClientLikeQueryable(db)) {
      throw new Error("New Jersey finance snapshot writes must receive a Pool, not a PoolClient");
    }
    throw new Error("New Jersey finance snapshot writes must receive a Pool");
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

export async function upsertNewJerseyFinanceLink(input: {
  db: Queryable;
  link: NewJerseyFinanceLinkInput;
}): Promise<{ linkId: string }> {
  validateNewJerseyFinanceLinkInput(input.link);

  const result = await input.db.query<ManualProtectedLinkRow>(
    `
      INSERT INTO public.nj_candidate_finance_links (
        candidate_id,
        election_id,
        election_year,
        candidate_name_normalized,
        office_name,
        district,
        candidate_entity_s,
        entity_name,
        election_type_code,
        link_status,
        link_source,
        source_url,
        last_verified_at
      )
      VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz)
      ON CONFLICT (candidate_id, election_id, candidate_entity_s)
      DO UPDATE SET
        candidate_name_normalized = EXCLUDED.candidate_name_normalized,
        office_name = EXCLUDED.office_name,
        district = EXCLUDED.district,
        entity_name = EXCLUDED.entity_name,
        election_type_code = EXCLUDED.election_type_code,
        ${manualProtectedLinkAssignments("nj_candidate_finance_links")},
        source_url = EXCLUDED.source_url,
        last_verified_at = EXCLUDED.last_verified_at
      RETURNING ${MANUAL_PROTECTED_LINK_RETURNING}
    `,
    [
      requireNonEmpty(input.link.candidateId, "candidate id"),
      requireNonEmpty(input.link.electionId, "election id"),
      normalizeElectionYear(input.link.electionYear),
      requireNonEmpty(input.link.candidateNameNormalized, "New Jersey finance candidate name"),
      requireNonEmpty(input.link.officeName, "New Jersey finance office name"),
      normalizeOptionalText(input.link.district),
      normalizeEntityS(input.link.candidateEntityS, "New Jersey candidate ENTITY_S"),
      requireNonEmpty(input.link.entityName, "New Jersey candidate entity name"),
      normalizeOptionalText(input.link.electionTypeCode),
      input.link.linkStatus ?? "active",
      input.link.linkSource ?? "manual",
      normalizeOptionalText(input.link.sourceUrl),
      normalizeNullableDate(input.link.lastVerifiedAt),
    ]
  );

  assertLinkWriteNotBlocked("New Jersey", result.rows[0], input.link.linkSource ?? "manual", input.link.electionYear);
  const linkId = result.rows[0]?.id;
  if (!linkId) {
    throw new Error("New Jersey finance link upsert did not return an id");
  }
  return { linkId };
}

async function upsertSummary(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  summary: NewJerseyFinanceSummaryInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.nj_candidate_finance_summaries (
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
      requireNonEmpty(input.linkId, "New Jersey finance link id"),
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
  breakdown: NewJerseyFinanceDirectBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.nj_candidate_finance_direct_breakdowns (
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
      requireNonEmpty(input.linkId, "New Jersey finance link id"),
      normalizeElectionYear(input.electionYear),
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "New Jersey finance direct breakdown category"),
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
  breakdowns: readonly NewJerseyFinanceDirectBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "New Jersey finance direct breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.nj_candidate_finance_direct_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            category_type text,
            category_name text
          )
          WHERE keep.category_type = nj_candidate_finance_direct_breakdowns.category_type
            AND keep.category_name = nj_candidate_finance_direct_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "New Jersey finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertOutsideGroup(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  group: NewJerseyFinanceOutsideGroupInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.nj_candidate_finance_outside_groups (
        link_id,
        election_year,
        outside_entity_s,
        outside_entity_name,
        support_oppose,
        amount,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (link_id, election_year, outside_entity_s, support_oppose)
      DO UPDATE SET
        outside_entity_name = EXCLUDED.outside_entity_name,
        amount = EXCLUDED.amount,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "New Jersey finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeEntityS(input.group.outsideEntityS, "New Jersey outside group ENTITY_S"),
      requireNonEmpty(input.group.outsideEntityName, "New Jersey outside group name"),
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
  groups: readonly NewJerseyFinanceOutsideGroupInput[];
}): Promise<void> {
  const keys = input.groups.map((group) => ({
    outside_entity_s: normalizeEntityS(group.outsideEntityS, "New Jersey outside group ENTITY_S"),
    support_oppose: group.supportOppose,
  }));

  await input.db.query(
    `
      DELETE FROM public.nj_candidate_finance_outside_groups
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            outside_entity_s integer,
            support_oppose text
          )
          WHERE keep.outside_entity_s = nj_candidate_finance_outside_groups.outside_entity_s
            AND keep.support_oppose = nj_candidate_finance_outside_groups.support_oppose
        )
    `,
    [requireNonEmpty(input.linkId, "New Jersey finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertOutsideGroupBreakdown(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdown: NewJerseyFinanceOutsideGroupBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.nj_candidate_finance_outside_group_breakdowns (
        link_id,
        election_year,
        outside_entity_s,
        support_oppose,
        category_type,
        category_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz)
      ON CONFLICT (link_id, election_year, outside_entity_s, support_oppose, category_type, category_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "New Jersey finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeEntityS(input.breakdown.outsideEntityS, "New Jersey outside breakdown ENTITY_S"),
      input.breakdown.supportOppose,
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "New Jersey outside breakdown category"),
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
  breakdowns: readonly NewJerseyFinanceOutsideGroupBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    outside_entity_s: normalizeEntityS(breakdown.outsideEntityS, "New Jersey outside breakdown ENTITY_S"),
    support_oppose: breakdown.supportOppose,
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "New Jersey outside breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.nj_candidate_finance_outside_group_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            outside_entity_s integer,
            support_oppose text,
            category_type text,
            category_name text
          )
          WHERE keep.outside_entity_s = nj_candidate_finance_outside_group_breakdowns.outside_entity_s
            AND keep.support_oppose = nj_candidate_finance_outside_group_breakdowns.support_oppose
            AND keep.category_type = nj_candidate_finance_outside_group_breakdowns.category_type
            AND keep.category_name = nj_candidate_finance_outside_group_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "New Jersey finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

export async function replaceNewJerseyCandidateFinanceSnapshot(
  input: NewJerseyFinanceSnapshotInput
): Promise<NewJerseyFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid New Jersey finance sync timestamp");
  }
  validateNewJerseyFinanceSnapshotInput(input);
  const electionYear = normalizeElectionYear(input.link.electionYear);

  return await withNewJerseyFinanceTransaction(input.db, async (db) => {
    const { linkId } = await upsertNewJerseyFinanceLink({ db, link: input.link });
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

    return {
      linkId,
      summaryWritten: Boolean(input.summary),
      directBreakdownsWritten: input.directBreakdowns?.length ?? 0,
      outsideGroupsWritten: input.outsideGroups?.length ?? 0,
      outsideGroupBreakdownsWritten: input.outsideGroupBreakdowns?.length ?? 0,
    };
  });
}
