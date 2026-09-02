// Delaware finance snapshot writer (plan-delaware-finance.md, Phase 1).
// Thin wrapper over the standard state-finance writer. Identity: the public
// CF_ID (8 digits) is canonical — Phase 0 proved zero conflicts across the
// full registry. The portal's MemberID is an acquisition key only and never
// reaches these tables. Outside groups/breakdowns are never written (plan
// hard fact 7: no expenditure -> candidate + position edge exists), so the
// snapshot input deliberately omits them; the stub tables satisfy the shared
// contract and stay empty.

import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceDirectBreakdownInput,
  type StandardStateFinanceDirectCategoryType,
  type StandardStateFinanceLinkInput,
  type StandardStateFinanceLinkStatus,
  type StandardStateFinanceSnapshotWriteResult,
  type StandardStateFinanceSummaryInput,
} from "../finance/standardStateFinanceSnapshotWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type DelawareFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type DelawareFinanceLinkSource = "manual" | "cfrs_portal";
export type DelawareFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;

export type DelawareFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: DelawareFinanceLinkSource;
};
export type DelawareFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type DelawareFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;

export type DelawareFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: DelawareFinanceLinkInput;
  syncedAt?: Date;
  summary?: DelawareFinanceSummaryInput;
  directBreakdowns?: readonly DelawareFinanceDirectBreakdownInput[];
};

export type DelawareFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

/** CF_ID: exactly eight digits as issued by CFRS (e.g. "01005311"). */
export function normalizeDelawareCfId(value: string): string {
  const normalized = value.trim();
  if (!/^\d{8}$/.test(normalized)) {
    throw new Error(`Invalid Delaware CF_ID: ${value}`);
  }
  return normalized;
}

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Delaware",
  minElectionYear: 2026,
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    // Outside totals are ALWAYS null for Delaware (hard fact 7 — no
    // expenditure -> candidate edge exists, so unavailable, never $0).
    // "replace" makes every snapshot force the columns back to NULL, so a
    // stray historical value can never survive a sync.
    outside_support_total: "replace",
    outside_oppose_total: "replace",
    source_url: "replace",
  },
  normalizeCommitteeId: normalizeDelawareCfId,
  supersededLinkSource: "cfrs_portal",
  manualLinkProtection: true,
  tables: {
    links: "de_candidate_finance_links",
    summaries: "de_candidate_finance_summaries",
    directBreakdowns: "de_candidate_finance_direct_breakdowns",
    outsideGroups: "de_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "de_candidate_finance_outside_group_breakdowns",
  },
});

export async function upsertDelawareFinanceLink(input: {
  db: Queryable;
  link: DelawareFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

export async function replaceDelawareCandidateFinanceSnapshot(
  input: DelawareFinanceSnapshotInput
): Promise<DelawareFinanceSnapshotWriteResult> {
  // Enforce the hard-fact-7 contract at the single write chokepoint: outside
  // totals are forced to NULL, and the empty outside arrays make the shared
  // writer delete any stray outside rows during the snapshot transaction.
  return writer.replaceSnapshot({
    ...input,
    summary:
      input.summary === undefined
        ? undefined
        : { ...input.summary, outsideSupportTotal: null, outsideOpposeTotal: null },
    outsideGroups: [],
    outsideGroupBreakdowns: [],
  });
}
