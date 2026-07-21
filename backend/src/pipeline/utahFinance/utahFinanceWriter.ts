import type { Pool, PoolClient } from "pg";

import { upsertFinanceLabelClassification } from "../finance/financeIndustryClassificationService.js";
import type { FinanceLabelClassification } from "../finance/financeLabelClassifier.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect?: () => Promise<PoolClient>;
};

export type UtahFinanceLinkStatus = "active" | "inactive";
export type UtahFinanceLinkSource = "manual" | "disclosures_advanced_search";
export type UtahFinanceDirectCategoryType = "contribution_size";

export type UtahFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  folderId: string;
  committeeName: string;
  linkStatus?: UtahFinanceLinkStatus;
  linkSource?: UtahFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type UtahFinanceSummaryInput = {
  totalReceipts?: number | null;
  directContributionTotal?: number | null;
  totalDisbursements?: number | null;
  cashOnHand?: number | null;
  sourceUrl?: string | null;
};

export type UtahFinanceDirectBreakdownInput = {
  categoryType: UtahFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type UtahFinanceSupportingCommitteeInput = {
  committeeName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type UtahFinanceSupportingCommitteeIndustryInput = {
  supportingCommitteeName: string;
  industrySlug: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type UtahFinanceSnapshotInput = {
  db: Queryable;
  link: UtahFinanceLinkInput;
  syncedAt?: Date;
  summary?: UtahFinanceSummaryInput;
  directBreakdowns?: readonly UtahFinanceDirectBreakdownInput[];
  supportingCommittees?: readonly UtahFinanceSupportingCommitteeInput[];
  supportingCommitteeIndustries?: readonly UtahFinanceSupportingCommitteeIndustryInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type UtahFinanceSnapshotWriteResult = {
  linkId: string;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  supportingCommitteesWritten: number;
  supportingCommitteeIndustriesWritten: number;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1998 || value > 2100) {
    throw new Error(`Invalid Utah finance election year: ${value}`);
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
    throw new Error("Invalid Utah finance timestamp");
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
    throw new Error("Utah finance contributor count must be a nonnegative integer");
  }
  return value;
}

function canOpenTransaction(db: Queryable): db is ConnectableQueryable & { connect: () => Promise<PoolClient> } {
  return typeof (db as ConnectableQueryable).connect === "function";
}

function validateUtahFinanceLinkInput(link: UtahFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "Utah finance candidate name");
  requireNonEmpty(link.officeName, "Utah finance office name");
  requireNonEmpty(link.folderId, "Utah disclosures folder id");
  requireNonEmpty(link.committeeName, "Utah committee name");
  normalizeNullableDate(link.lastVerifiedAt);
}

function supportingCommitteeKey(value: string): string {
  return requireNonEmpty(value, "Utah supporting committee name").replace(/\s+/g, " ").toUpperCase();
}

function validateUtahFinanceSnapshotInput(input: UtahFinanceSnapshotInput): void {
  validateUtahFinanceLinkInput(input.link);
  if (input.supportingCommitteeIndustries?.length) {
    const committees = new Set((input.supportingCommittees ?? []).map((committee) => supportingCommitteeKey(committee.committeeName)));
    for (const industry of input.supportingCommitteeIndustries) {
      if (!committees.has(supportingCommitteeKey(industry.supportingCommitteeName))) {
        throw new Error("Utah supporting committee industries require matching supporting committees in the same snapshot");
      }
    }
  }
}

async function withUtahFinanceTransaction<T>(db: Queryable, work: (tx: Queryable) => Promise<T>): Promise<T> {
  if (!canOpenTransaction(db)) {
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

export async function upsertUtahFinanceLink(input: {
  db: Queryable;
  link: UtahFinanceLinkInput;
}): Promise<{ linkId: string }> {
  validateUtahFinanceLinkInput(input.link);

  const result = await input.db.query<{ id: string }>(
    `
      INSERT INTO public.ut_candidate_finance_links (
        candidate_id,
        election_id,
        election_year,
        candidate_name_normalized,
        office_name,
        district,
        folder_id,
        committee_name,
        link_status,
        link_source,
        source_url,
        last_verified_at
      )
      VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::timestamptz)
      ON CONFLICT (candidate_id, election_id, folder_id)
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
      requireNonEmpty(input.link.candidateNameNormalized, "Utah finance candidate name"),
      requireNonEmpty(input.link.officeName, "Utah finance office name"),
      normalizeOptionalText(input.link.district),
      requireNonEmpty(input.link.folderId, "Utah disclosures folder id"),
      requireNonEmpty(input.link.committeeName, "Utah committee name"),
      input.link.linkStatus ?? "active",
      input.link.linkSource ?? "manual",
      normalizeOptionalText(input.link.sourceUrl),
      normalizeNullableDate(input.link.lastVerifiedAt),
    ]
  );

  const linkId = result.rows[0]?.id;
  if (!linkId) {
    throw new Error("Utah finance link upsert did not return an id");
  }
  return { linkId };
}

async function upsertSummary(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  summary: UtahFinanceSummaryInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ut_candidate_finance_summaries (
        link_id,
        election_year,
        total_receipts,
        direct_contribution_total,
        total_disbursements,
        cash_on_hand,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (link_id, election_year)
      DO UPDATE SET
        total_receipts = EXCLUDED.total_receipts,
        direct_contribution_total = EXCLUDED.direct_contribution_total,
        total_disbursements = EXCLUDED.total_disbursements,
        cash_on_hand = EXCLUDED.cash_on_hand,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Utah finance link id"),
      normalizeElectionYear(input.electionYear),
      normalizeNullableAmount(input.summary.totalReceipts, "total receipts"),
      normalizeNullableAmount(input.summary.directContributionTotal, "direct contribution total"),
      normalizeNullableAmount(input.summary.totalDisbursements, "total disbursements"),
      normalizeNullableAmount(input.summary.cashOnHand, "cash on hand"),
      normalizeOptionalText(input.summary.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertDirectBreakdown(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  breakdown: UtahFinanceDirectBreakdownInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ut_candidate_finance_direct_breakdowns (
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
      requireNonEmpty(input.linkId, "Utah finance link id"),
      normalizeElectionYear(input.electionYear),
      input.breakdown.categoryType,
      requireNonEmpty(input.breakdown.categoryName, "Utah finance direct breakdown category"),
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
  breakdowns: readonly UtahFinanceDirectBreakdownInput[];
}): Promise<void> {
  const keys = input.breakdowns.map((breakdown) => ({
    category_type: breakdown.categoryType,
    category_name: requireNonEmpty(breakdown.categoryName, "Utah finance direct breakdown category"),
  }));

  await input.db.query(
    `
      DELETE FROM public.ut_candidate_finance_direct_breakdowns
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            category_type text,
            category_name text
          )
          WHERE keep.category_type = ut_candidate_finance_direct_breakdowns.category_type
            AND keep.category_name = ut_candidate_finance_direct_breakdowns.category_name
        )
    `,
    [requireNonEmpty(input.linkId, "Utah finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertSupportingCommittee(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  committee: UtahFinanceSupportingCommitteeInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ut_candidate_finance_supporting_committees (
        link_id,
        election_year,
        committee_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::timestamptz)
      ON CONFLICT (link_id, election_year, committee_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Utah finance link id"),
      normalizeElectionYear(input.electionYear),
      requireNonEmpty(input.committee.committeeName, "Utah supporting committee name"),
      normalizeAmount(input.committee.amount, "supporting committee amount"),
      normalizeNullableCount(input.committee.contributorCount),
      normalizeOptionalText(input.committee.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function deleteStaleSupportingCommittees(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  committees: readonly UtahFinanceSupportingCommitteeInput[];
}): Promise<void> {
  const keys = input.committees.map((committee) => ({
    committee_name: requireNonEmpty(committee.committeeName, "Utah supporting committee name"),
  }));

  await input.db.query(
    `
      DELETE FROM public.ut_candidate_finance_supporting_committees
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            committee_name text
          )
          WHERE keep.committee_name = ut_candidate_finance_supporting_committees.committee_name
        )
    `,
    [requireNonEmpty(input.linkId, "Utah finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

async function upsertSupportingCommitteeIndustry(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  industry: UtahFinanceSupportingCommitteeIndustryInput;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.ut_candidate_finance_supporting_committee_industries (
        link_id,
        election_year,
        supporting_committee_name,
        industry_slug,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (link_id, election_year, supporting_committee_name, industry_slug)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      requireNonEmpty(input.linkId, "Utah finance link id"),
      normalizeElectionYear(input.electionYear),
      requireNonEmpty(input.industry.supportingCommitteeName, "Utah supporting committee name"),
      requireNonEmpty(input.industry.industrySlug, "Utah supporting committee industry slug"),
      normalizeAmount(input.industry.amount, "supporting committee industry amount"),
      normalizeNullableCount(input.industry.contributorCount),
      normalizeOptionalText(input.industry.sourceUrl),
      input.syncedAt.toISOString(),
    ]
  );
}

async function deleteStaleSupportingCommitteeIndustries(input: {
  db: Queryable;
  linkId: string;
  electionYear: number;
  industries: readonly UtahFinanceSupportingCommitteeIndustryInput[];
}): Promise<void> {
  const keys = input.industries.map((industry) => ({
    supporting_committee_name: requireNonEmpty(industry.supportingCommitteeName, "Utah supporting committee name"),
    industry_slug: requireNonEmpty(industry.industrySlug, "Utah supporting committee industry slug"),
  }));

  await input.db.query(
    `
      DELETE FROM public.ut_candidate_finance_supporting_committee_industries
      WHERE link_id = $1::uuid
        AND election_year = $2
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset($3::jsonb) AS keep(
            supporting_committee_name text,
            industry_slug text
          )
          WHERE keep.supporting_committee_name = ut_candidate_finance_supporting_committee_industries.supporting_committee_name
            AND keep.industry_slug = ut_candidate_finance_supporting_committee_industries.industry_slug
        )
    `,
    [requireNonEmpty(input.linkId, "Utah finance link id"), normalizeElectionYear(input.electionYear), JSON.stringify(keys)]
  );
}

export async function replaceUtahCandidateFinanceSnapshot(
  input: UtahFinanceSnapshotInput
): Promise<UtahFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid Utah finance sync timestamp");
  }
  validateUtahFinanceSnapshotInput(input);
  const electionYear = normalizeElectionYear(input.link.electionYear);

  return await withUtahFinanceTransaction(input.db, async (db) => {
    const { linkId } = await upsertUtahFinanceLink({ db, link: input.link });
    if (input.summary) {
      await upsertSummary({ db, linkId, electionYear, summary: input.summary, syncedAt });
    }

    for (const breakdown of input.directBreakdowns ?? []) {
      await upsertDirectBreakdown({ db, linkId, electionYear, breakdown, syncedAt });
    }
    if (input.directBreakdowns) {
      await deleteStaleDirectBreakdowns({ db, linkId, electionYear, breakdowns: input.directBreakdowns });
    }

    for (const committee of input.supportingCommittees ?? []) {
      await upsertSupportingCommittee({ db, linkId, electionYear, committee, syncedAt });
    }
    if (input.supportingCommittees) {
      await deleteStaleSupportingCommittees({ db, linkId, electionYear, committees: input.supportingCommittees });
    }

    for (const industry of input.supportingCommitteeIndustries ?? []) {
      await upsertSupportingCommitteeIndustry({ db, linkId, electionYear, industry, syncedAt });
    }
    if (input.supportingCommitteeIndustries) {
      await deleteStaleSupportingCommitteeIndustries({
        db,
        linkId,
        electionYear,
        industries: input.supportingCommitteeIndustries,
      });
    }

    // Persist donor classifications to the shared cache so unresolved Utah
    // labels enter the manual industry-label queue; the upsert's conflict
    // guard keeps manual rows authoritative.
    for (const classification of input.classifications ?? []) {
      await upsertFinanceLabelClassification({ db, classification });
    }

    return {
      linkId,
      summaryWritten: Boolean(input.summary),
      directBreakdownsWritten: input.directBreakdowns?.length ?? 0,
      supportingCommitteesWritten: input.supportingCommittees?.length ?? 0,
      supportingCommitteeIndustriesWritten: input.supportingCommitteeIndustries?.length ?? 0,
    };
  });
}
