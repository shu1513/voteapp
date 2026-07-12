import type { Pool, PoolClient } from "pg";

import type { FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import { upsertFinanceLabelClassification } from "../finance/financeIndustryClassificationService.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type PoolLikeQueryable = Queryable & {
  connect?: () => Promise<PoolClient>;
};
type ClientLikeQueryable = Queryable & {
  release?: () => void;
};

export type IllinoisFinanceLinkStatus = "active" | "inactive";
export type IllinoisFinanceLinkSource = "manual" | "illinois_sbe";
export type IllinoisFinanceDirectCategoryType = "occupation" | "contribution_size";
export type IllinoisFinanceOutsideCategoryType = "donor" | "industry";
export type IllinoisFinanceSupportOppose = "support" | "oppose";

export type IllinoisFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  sbeCandidateId?: string | null;
  sbeDistrictType?: string | null;
  sbeOffice?: string | null;
  isAtLarge?: boolean | null;
  committeeKey: string;
  committeeName: string;
  linkStatus?: IllinoisFinanceLinkStatus;
  linkSource?: IllinoisFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type IllinoisFinanceSummaryInput = {
  totalReceipts?: number | null;
  directContributionTotal?: number | null;
  totalDisbursements?: number | null;
  cashOnHand?: number | null;
  debtsOwed?: number | null;
  outsideSupportTotal?: number | null;
  outsideOpposeTotal?: number | null;
  sourceUrl?: string | null;
};

export type IllinoisFinanceDirectBreakdownInput = {
  categoryType: IllinoisFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type IllinoisFinanceOutsideGroupInput = {
  committeeKey: string;
  committeeName: string;
  supportOppose: IllinoisFinanceSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type IllinoisFinanceOutsideGroupBreakdownInput = {
  committeeKey: string;
  supportOppose: IllinoisFinanceSupportOppose;
  categoryType: IllinoisFinanceOutsideCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type IllinoisFinanceSnapshotInput = {
  db: Queryable;
  link: IllinoisFinanceLinkInput;
  syncedAt?: Date;
  summary?: IllinoisFinanceSummaryInput;
  directBreakdowns?: readonly IllinoisFinanceDirectBreakdownInput[];
  outsideGroups?: readonly IllinoisFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly IllinoisFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type IllinoisFinanceSnapshotWriteResult = {
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
    throw new Error(`Invalid Illinois finance election year: ${value}`);
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
    throw new Error("Invalid Illinois finance timestamp");
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

function normalizeNullableFiniteAmount(value: number | null | undefined, fieldName: string): number | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (!Number.isFinite(value)) {
    throw new Error(`${fieldName} must be a finite number`);
  }
  return value;
}

function normalizeNullableCount(value: number | null | undefined): number | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (!Number.isInteger(value) || value < 0) {
    throw new Error("Illinois finance contributor count must be a nonnegative integer");
  }
  return value;
}

function canOpenTransaction(db: Queryable): db is PoolLikeQueryable & { connect: () => Promise<PoolClient> } {
  return (
    typeof (db as PoolLikeQueryable).connect === "function" &&
    typeof (db as ClientLikeQueryable).release !== "function"
  );
}

function isClientLikeQueryable(db: Queryable): db is ClientLikeQueryable {
  return typeof (db as ClientLikeQueryable).release === "function";
}

function validateIllinoisFinanceLinkInput(link: IllinoisFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "Illinois finance candidate name");
  requireNonEmpty(link.officeName, "Illinois finance office name");
  normalizeCommitteeKey(requireNonEmpty(link.committeeKey, "Illinois committee key"));
  requireNonEmpty(link.committeeName, "Illinois committee name");
  normalizeNullableDate(link.lastVerifiedAt);
}

function validateIllinoisFinanceSnapshotInput(input: IllinoisFinanceSnapshotInput): void {
  validateIllinoisFinanceLinkInput(input.link);
  const outsideBreakdownCount = input.outsideGroupBreakdowns?.length ?? 0;
  const outsideGroupCount = input.outsideGroups?.length ?? 0;
  if (outsideBreakdownCount > 0 && outsideGroupCount === 0) {
    throw new Error("Illinois outside group breakdowns require outside groups in the same snapshot");
  }
  if (outsideBreakdownCount > 0) {
    const groupKeys = new Set(
      (input.outsideGroups ?? []).map(
        (group) =>
          `${normalizeCommitteeKey(requireNonEmpty(group.committeeKey, "Illinois outside group committee key"))}\u0000${group.supportOppose}`
      )
    );
    for (const breakdown of input.outsideGroupBreakdowns ?? []) {
      const key = `${normalizeCommitteeKey(requireNonEmpty(breakdown.committeeKey, "Illinois outside breakdown committee key"))}\u0000${breakdown.supportOppose}`;
      if (!groupKeys.has(key)) {
        throw new Error("Illinois outside group breakdowns must reference outside groups in the same snapshot");
      }
    }
  }
}

async function withIllinoisFinanceTransaction<T>(db: Queryable, work: (tx: Queryable) => Promise<T>): Promise<T> {
  if (!canOpenTransaction(db)) {
    if (isClientLikeQueryable(db)) {
      throw new Error("Illinois finance snapshot writes must receive a Pool, not a PoolClient");
    }
    try {
      await db.query("BEGIN");
      const result = await work(db);
      await db.query("COMMIT");
      return result;
    } catch (error) {
      try {
        await db.query("ROLLBACK");
      } catch {
        // Preserve the original write failure.
      }
      throw error;
    }
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

export async function upsertIllinoisFinanceLink(input: {
  db: Queryable;
  link: IllinoisFinanceLinkInput;
}): Promise<{ linkId: string }> {
  validateIllinoisFinanceLinkInput(input.link);

  const result = await input.db.query<{ id: string }>(
    `
      INSERT INTO public.il_candidate_finance_links (
        candidate_id,
        election_id,
        election_year,
        candidate_name_normalized,
        office_name,
        district,
        sbe_candidate_id,
        sbe_district_type,
        sbe_office,
        is_at_large,
        committee_key,
        committee_name,
        link_status,
        link_source,
        source_url,
        last_verified_at
      )
      VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::timestamptz)
      ON CONFLICT (candidate_id, election_id, committee_key)
      DO UPDATE SET
        election_year = EXCLUDED.election_year,
        candidate_name_normalized = EXCLUDED.candidate_name_normalized,
        office_name = EXCLUDED.office_name,
        district = EXCLUDED.district,
        sbe_candidate_id = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.sbe_candidate_id
          ELSE EXCLUDED.sbe_candidate_id
        END,
        sbe_district_type = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.sbe_district_type
          ELSE EXCLUDED.sbe_district_type
        END,
        sbe_office = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.sbe_office
          ELSE EXCLUDED.sbe_office
        END,
        is_at_large = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.is_at_large
          ELSE EXCLUDED.is_at_large
        END,
        committee_name = EXCLUDED.committee_name,
        link_status = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.link_status
          ELSE EXCLUDED.link_status
        END,
        link_source = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.link_source
          ELSE EXCLUDED.link_source
        END,
        source_url = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.source_url
          ELSE EXCLUDED.source_url
        END,
        last_verified_at = CASE
          WHEN il_candidate_finance_links.link_source = 'manual' THEN il_candidate_finance_links.last_verified_at
          ELSE EXCLUDED.last_verified_at
        END
      RETURNING id
    `,
    [
      requireNonEmpty(input.link.candidateId, "candidate id"),
      requireNonEmpty(input.link.electionId, "election id"),
      normalizeElectionYear(input.link.electionYear),
      requireNonEmpty(input.link.candidateNameNormalized, "Illinois finance candidate name"),
      requireNonEmpty(input.link.officeName, "Illinois finance office name"),
      normalizeOptionalText(input.link.district),
      normalizeOptionalText(input.link.sbeCandidateId),
      normalizeOptionalText(input.link.sbeDistrictType),
      normalizeOptionalText(input.link.sbeOffice),
      input.link.isAtLarge ?? null,
      normalizeCommitteeKey(requireNonEmpty(input.link.committeeKey, "Illinois committee key")),
      requireNonEmpty(input.link.committeeName, "Illinois committee name"),
      input.link.linkStatus ?? "active",
      input.link.linkSource ?? "manual",
      normalizeOptionalText(input.link.sourceUrl),
      normalizeNullableDate(input.link.lastVerifiedAt),
    ]
  );

  const linkId = result.rows[0]?.id;
  if (!linkId) {
    throw new Error("Illinois finance link upsert did not return an id");
  }
  return { linkId };
}

export async function deactivateIllinoisFinanceLinksForCandidateElection(input: {
  db: Queryable;
  candidateId: string;
  electionId: string;
  electionYear: number;
  verifiedAt?: Date | null;
}): Promise<number> {
  const result = await input.db.query(
    `
      UPDATE public.il_candidate_finance_links
      SET link_status = 'inactive',
          last_verified_at = $4::timestamptz
      WHERE candidate_id = $1::uuid
        AND election_id = $2::uuid
        AND election_year = $3
        AND link_status = 'active'
        AND link_source IS DISTINCT FROM 'manual'
    `,
    [
      requireNonEmpty(input.candidateId, "candidate id"),
      requireNonEmpty(input.electionId, "election id"),
      normalizeElectionYear(input.electionYear),
      normalizeNullableDate(input.verifiedAt),
    ]
  );
  return typeof result.rowCount === "number" ? result.rowCount : 0;
}

export async function deactivateIllinoisFinanceLinksExcept(input: {
  db: Queryable;
  candidateId: string;
  electionId: string;
  electionYear: number;
  activeCommitteeKeys: readonly string[];
  verifiedAt?: Date | null;
}): Promise<number> {
  const activeCommitteeKeys = [...new Set(input.activeCommitteeKeys.map(normalizeCommitteeKey))];
  if (activeCommitteeKeys.length === 0) {
    throw new Error("Illinois finance active committee keys are required");
  }
  const result = await input.db.query(
    `
      UPDATE public.il_candidate_finance_links
      SET link_status = 'inactive',
          last_verified_at = $5::timestamptz
      WHERE candidate_id = $1::uuid
        AND election_id = $2::uuid
        AND election_year = $3
        AND link_status = 'active'
        AND link_source IS DISTINCT FROM 'manual'
        AND NOT (committee_key = ANY($4::text[]))
    `,
    [
      requireNonEmpty(input.candidateId, "candidate id"),
      requireNonEmpty(input.electionId, "election id"),
      normalizeElectionYear(input.electionYear),
      activeCommitteeKeys,
      normalizeNullableDate(input.verifiedAt),
    ]
  );
  return typeof result.rowCount === "number" ? result.rowCount : 0;
}

export async function replaceIllinoisAutoLinkedFinanceLinks(input: {
  db: Queryable;
  links: readonly IllinoisFinanceLinkInput[];
  verifiedAt: Date;
}): Promise<void> {
  if (input.links.length === 0) {
    throw new Error("Illinois auto-linked finance links are required");
  }
  const first = input.links[0]!;
  validateIllinoisFinanceLinkInput(first);
  const candidateId = requireNonEmpty(first.candidateId, "candidate id");
  const electionId = requireNonEmpty(first.electionId, "election id");
  const electionYear = normalizeElectionYear(first.electionYear);
  const committeeKeys: string[] = [];

  for (const link of input.links) {
    validateIllinoisFinanceLinkInput(link);
    if (
      requireNonEmpty(link.candidateId, "candidate id") !== candidateId ||
      requireNonEmpty(link.electionId, "election id") !== electionId ||
      normalizeElectionYear(link.electionYear) !== electionYear
    ) {
      throw new Error("Illinois auto-linked finance links must share one candidate election");
    }
    if (link.linkSource !== "illinois_sbe" || link.linkStatus !== "active") {
      throw new Error("Illinois auto-linked finance links must be active Illinois SBE links");
    }
    committeeKeys.push(normalizeCommitteeKey(requireNonEmpty(link.committeeKey, "Illinois committee key")));
  }

  await withIllinoisFinanceTransaction(input.db, async (db) => {
    for (const link of input.links) {
      await upsertIllinoisFinanceLink({ db, link });
    }
    await deactivateIllinoisFinanceLinksExcept({
      db,
      candidateId,
      electionId,
      electionYear,
      activeCommitteeKeys: committeeKeys,
      verifiedAt: input.verifiedAt,
    });
  });
}

async function upsertSummary(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  summary: IllinoisFinanceSummaryInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.il_candidate_finance_summaries (
        link_id,
        election_year,
        total_receipts,
        direct_contribution_total,
        total_disbursements,
        cash_on_hand,
        debts_owed,
        outside_support_total,
        outside_oppose_total,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::timestamptz)
      ON CONFLICT (link_id, election_year)
      DO UPDATE SET
        total_receipts = EXCLUDED.total_receipts,
        direct_contribution_total = EXCLUDED.direct_contribution_total,
        total_disbursements = EXCLUDED.total_disbursements,
        cash_on_hand = EXCLUDED.cash_on_hand,
        debts_owed = EXCLUDED.debts_owed,
        outside_support_total = COALESCE(
          EXCLUDED.outside_support_total,
          il_candidate_finance_summaries.outside_support_total
        ),
        outside_oppose_total = COALESCE(
          EXCLUDED.outside_oppose_total,
          il_candidate_finance_summaries.outside_oppose_total
        ),
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Illinois finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeNullableAmount(input.summary.totalReceipts, "total receipts"),
      normalizeNullableAmount(input.summary.directContributionTotal, "direct contribution total"),
      normalizeNullableAmount(input.summary.totalDisbursements, "total disbursements"),
      normalizeNullableFiniteAmount(input.summary.cashOnHand, "cash on hand"),
      normalizeNullableAmount(input.summary.debtsOwed, "debts owed"),
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
  breakdown: IllinoisFinanceDirectBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.il_candidate_finance_direct_breakdowns (
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
      requireNonEmpty(input.linkId, "Illinois finance link id"),
      normalizeElectionYear(input.electionYear),
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "Illinois finance direct breakdown category"),
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
  group: IllinoisFinanceOutsideGroupInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.il_candidate_finance_outside_groups (
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
      requireNonEmpty(input.linkId, "Illinois finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeCommitteeKey(requireNonEmpty(input.group.committeeKey, "Illinois outside group committee key")),
      requireNonEmpty(input.group.committeeName, "Illinois outside group committee name"),
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
  breakdown: IllinoisFinanceOutsideGroupBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.il_candidate_finance_outside_group_breakdowns (
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
      requireNonEmpty(input.linkId, "Illinois finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeCommitteeKey(requireNonEmpty(input.breakdown.committeeKey, "Illinois outside breakdown committee key")),
      input.breakdown.supportOppose,
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "Illinois outside breakdown category"),
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
  breakdowns: readonly IllinoisFinanceDirectBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "Illinois finance direct breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.il_candidate_finance_direct_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            category_type text,
            category_name text
          )
          WHERE keep.category_type = il_candidate_finance_direct_breakdowns.category_type
            AND keep.category_name = il_candidate_finance_direct_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "Illinois finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function deleteStaleOutsideGroupBreakdowns(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdowns: readonly IllinoisFinanceOutsideGroupBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    committee_key: normalizeCommitteeKey(requireNonEmpty(breakdown.committeeKey, "Illinois outside breakdown committee key")),
    support_oppose: breakdown.supportOppose,
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "Illinois outside breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.il_candidate_finance_outside_group_breakdowns
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
          WHERE keep.committee_key = il_candidate_finance_outside_group_breakdowns.committee_key
            AND keep.support_oppose = il_candidate_finance_outside_group_breakdowns.support_oppose
            AND keep.category_type = il_candidate_finance_outside_group_breakdowns.category_type
            AND keep.category_name = il_candidate_finance_outside_group_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "Illinois finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function deleteStaleOutsideGroups(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  groups: readonly IllinoisFinanceOutsideGroupInput[];
}): Promise<void> {
  const keys = input.groups.map((group) => ({
    committee_key: normalizeCommitteeKey(requireNonEmpty(group.committeeKey, "Illinois outside group committee key")),
    support_oppose: group.supportOppose,
  }));

  await input.db.query(
    `
      DELETE FROM public.il_candidate_finance_outside_groups
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            committee_key text,
            support_oppose text
          )
          WHERE keep.committee_key = il_candidate_finance_outside_groups.committee_key
            AND keep.support_oppose = il_candidate_finance_outside_groups.support_oppose
        )
    `,
    [requireNonEmpty(input.linkId, "Illinois finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

export async function replaceIllinoisCandidateFinanceSnapshot(
  input: IllinoisFinanceSnapshotInput
): Promise<IllinoisFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid Illinois finance sync timestamp");
  }
  validateIllinoisFinanceSnapshotInput(input);
  const electionYear = normalizeElectionYear(input.link.electionYear);

  return await withIllinoisFinanceTransaction(input.db, async (db) => {
    const { linkId } = await upsertIllinoisFinanceLink({ db, link: input.link });
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
