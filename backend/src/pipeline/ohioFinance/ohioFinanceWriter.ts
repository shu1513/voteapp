import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceDirectBreakdownInput,
  type StandardStateFinanceDirectCategoryType,
  type StandardStateFinanceLinkInput,
  type StandardStateFinanceLinkStatus,
  type StandardStateFinanceOutsideCategoryType,
  type StandardStateFinanceOutsideGroupBreakdownInput,
  type StandardStateFinanceOutsideGroupInput,
  type StandardStateFinanceSnapshotWriteResult,
  type StandardStateFinanceSummaryInput,
  type StandardStateFinanceSupportOppose,
} from "../finance/standardStateFinanceSnapshotWriter.js";
import type { FinanceLabelClassification } from "../finance/financeLabelClassifier.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type OhioFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type OhioFinanceLinkSource = "manual" | "sos_bulk_export";
export type OhioFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type OhioFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type OhioFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type OhioFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: OhioFinanceLinkSource;
};

export type OhioFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type OhioFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type OhioFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type OhioFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type OhioFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: OhioFinanceLinkInput;
  syncedAt?: Date;
  summary?: OhioFinanceSummaryInput;
  directBreakdowns?: readonly OhioFinanceDirectBreakdownInput[];
  outsideGroups?: readonly OhioFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly OhioFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type OhioFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Ohio",
  minElectionYear: 2000,
  // Ohio replaces every summary column except the outside totals, which keep
  // the stored value when the incoming value is NULL so a direct-only refresh
  // without Form 31-U data does not wipe them (preserveWhenNull default).
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    source_url: "replace",
  },
  outsideGroupValidation: "pairing",
  supersededLinkSource: "sos_bulk_export",
  tables: {
    links: "oh_candidate_finance_links",
    summaries: "oh_candidate_finance_summaries",
    directBreakdowns: "oh_candidate_finance_direct_breakdowns",
    outsideGroups: "oh_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "oh_candidate_finance_outside_group_breakdowns",
  },
});

// Ohio committee identity is the Secretary of State's stable numeric entity
// MASTER_KEY. Anything non-numeric is a parsing or mapping bug upstream, so
// it is rejected here instead of becoming a permanent identity value.
function requireOhioMasterKey(value: string, fieldName: string): void {
  if (!/^[0-9]+$/.test(value.trim())) {
    throw new Error(`${fieldName} must be a numeric Ohio SOS master key`);
  }
}

function validateOhioFinanceSnapshotInput(input: {
  link: OhioFinanceLinkInput;
  outsideGroups?: readonly OhioFinanceOutsideGroupInput[];
}): void {
  requireOhioMasterKey(input.link.committeeId, "Ohio committee id");
  for (const group of input.outsideGroups ?? []) {
    requireOhioMasterKey(group.committeeId, "Ohio outside group committee id");
  }
}

export async function upsertOhioFinanceLink(input: {
  db: Queryable;
  link: OhioFinanceLinkInput;
}): Promise<{ linkId: string }> {
  requireOhioMasterKey(input.link.committeeId, "Ohio committee id");
  return writer.upsertLink(input);
}

export async function replaceOhioCandidateFinanceSnapshot(
  input: OhioFinanceSnapshotInput
): Promise<OhioFinanceSnapshotWriteResult> {
  validateOhioFinanceSnapshotInput(input);
  return writer.replaceSnapshot(input);
}
