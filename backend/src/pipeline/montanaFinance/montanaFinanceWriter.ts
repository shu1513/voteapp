// Montana finance snapshot writer (docs/plans/montana-finance.md, Phase 2a).
// Thin wrapper over the standard state-finance writer. Identity: the numeric
// CERS candidateId (per candidate per election cycle), stored as text in
// committee_id; committee_name holds the CERS candidate display name.
//
// Outside totals use preserveWhenNull: Phase 2b writes them from the IE
// sweep, and a later direct-only sync passing null must not erase a good
// outside snapshot (the plan's "NULL when none disclosed — never 0" rule).

import type { Pool, PoolClient } from "pg";

import type { FinanceLabelClassification } from "../finance/financeLabelClassifier.js";
import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceDirectBreakdownInput,
  type StandardStateFinanceDirectCategoryType,
  type StandardStateFinanceLinkInput,
  type StandardStateFinanceLinkStatus,
  type StandardStateFinanceOutsideGroupBreakdownInput,
  type StandardStateFinanceOutsideGroupInput,
  type StandardStateFinanceSnapshotWriteResult,
  type StandardStateFinanceSummaryInput,
} from "../finance/standardStateFinanceSnapshotWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type MontanaFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type MontanaFinanceLinkSource = "manual" | "cers_portal";
export type MontanaFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;

export type MontanaFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: MontanaFinanceLinkSource;
};
export type MontanaFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type MontanaFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type MontanaFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type MontanaFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type MontanaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: MontanaFinanceLinkInput;
  syncedAt?: Date;
  summary?: MontanaFinanceSummaryInput;
  directBreakdowns?: readonly MontanaFinanceDirectBreakdownInput[];
  outsideGroups?: readonly MontanaFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly MontanaFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type MontanaFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

export function normalizeMontanaCersEntityId(value: string): string {
  const normalized = value.trim();
  if (!/^[1-9]\d*$/.test(normalized) || !Number.isSafeInteger(Number(normalized))) {
    throw new Error(`Invalid Montana CERS entity id: ${value}`);
  }
  return normalized;
}

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Montana",
  minElectionYear: 2024,
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    // Phase 2b's IE sweep owns these; a direct-only sync must not erase them.
    outside_support_total: "preserveWhenNull",
    outside_oppose_total: "preserveWhenNull",
    source_url: "replace",
  },
  outsideGroupValidation: "pairing",
  normalizeCommitteeId: normalizeMontanaCersEntityId,
  supersededLinkSource: "cers_portal",
  manualLinkProtection: true,
  tables: {
    links: "mt_candidate_finance_links",
    summaries: "mt_candidate_finance_summaries",
    directBreakdowns: "mt_candidate_finance_direct_breakdowns",
    outsideGroups: "mt_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "mt_candidate_finance_outside_group_breakdowns",
  },
});

export async function upsertMontanaFinanceLink(input: {
  db: Queryable;
  link: MontanaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

/**
 * Stamps a "checked, nothing filed" state for a linked candidate: upserts
 * the link and writes/refreshes an all-NULL summary row whose
 * last_synced_at rotates the candidate out of the due list. Without this,
 * the hundreds of sub-$500 registrations that never file a C-5 would sort
 * NULLS-FIRST forever and starve real filers out of every batch. The stamp
 * never touches a summary carrying data — if CERS suddenly reports no
 * filings for a candidate with a stored snapshot, the snapshot is
 * preserved and the anomaly stays visibly due. An all-NULL summary renders
 * nothing (null money means "not reported", and the cards hide it).
 */
export async function recordMontanaCandidateFinanceNoFiledReports(input: {
  db: Queryable;
  link: MontanaFinanceLinkInput;
  syncedAt: Date;
}): Promise<{ linkId: string; checkedAtStamped: boolean }> {
  const { linkId } = await writer.upsertLink({ db: input.db, link: input.link });
  const result = await input.db.query(
    `INSERT INTO public.mt_candidate_finance_summaries (link_id, election_year, last_synced_at)
     VALUES ($1, $2, $3)
     ON CONFLICT (link_id, election_year) DO UPDATE
       SET last_synced_at = EXCLUDED.last_synced_at
     WHERE mt_candidate_finance_summaries.total_receipts IS NULL
       AND mt_candidate_finance_summaries.direct_contribution_total IS NULL
       AND mt_candidate_finance_summaries.total_disbursements IS NULL
       AND mt_candidate_finance_summaries.cash_on_hand IS NULL
       AND mt_candidate_finance_summaries.outside_support_total IS NULL
       AND mt_candidate_finance_summaries.outside_oppose_total IS NULL`,
    [linkId, input.link.electionYear, input.syncedAt]
  );
  return { linkId, checkedAtStamped: (result.rowCount ?? 0) > 0 };
}

export async function replaceMontanaCandidateFinanceSnapshot(
  input: MontanaFinanceSnapshotInput
): Promise<MontanaFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot(input);
}
