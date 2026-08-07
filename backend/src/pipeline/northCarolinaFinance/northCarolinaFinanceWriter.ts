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

export type NorthCarolinaFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type NorthCarolinaFinanceLinkSource = "manual" | "ncsbe_portal";
export type NorthCarolinaFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type NorthCarolinaFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type NorthCarolinaFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type NorthCarolinaFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: NorthCarolinaFinanceLinkSource;
};

export type NorthCarolinaFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type NorthCarolinaFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type NorthCarolinaFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type NorthCarolinaFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type NorthCarolinaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: NorthCarolinaFinanceLinkInput;
  syncedAt?: Date;
  summary?: NorthCarolinaFinanceSummaryInput;
  directBreakdowns?: readonly NorthCarolinaFinanceDirectBreakdownInput[];
  outsideGroups?: readonly NorthCarolinaFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly NorthCarolinaFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type NorthCarolinaFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

// NCSBE identity values (SBoEID like "STA-JV516O-C-001", plus the synthetic
// NC-OGID:/NC-IE-FILER: keys) are defined uppercase, so the normalizer is
// unconditional. The exact SBoEID validation regex is pinned from acquisition
// spike bytes (nc_plan decision on PR 4); until then the factory's nonempty
// check is the only structural validation.
function normalizeNorthCarolinaCommitteeId(value: string): string {
  return value.toUpperCase();
}

const writer = createStandardStateFinanceSnapshotWriter({
  label: "North Carolina",
  minElectionYear: 2000,
  // North Carolina replaces every summary column except the outside totals,
  // which keep the stored value when the incoming value is NULL so a
  // direct-only refresh without IE report data does not wipe them
  // (preserveWhenNull default).
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    source_url: "replace",
  },
  outsideGroupValidation: "pairing",
  normalizeCommitteeId: normalizeNorthCarolinaCommitteeId,
  supersededLinkSource: "ncsbe_portal",
  tables: {
    links: "nc_candidate_finance_links",
    summaries: "nc_candidate_finance_summaries",
    directBreakdowns: "nc_candidate_finance_direct_breakdowns",
    outsideGroups: "nc_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "nc_candidate_finance_outside_group_breakdowns",
  },
});

export async function upsertNorthCarolinaFinanceLink(input: {
  db: Queryable;
  link: NorthCarolinaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

export async function replaceNorthCarolinaCandidateFinanceSnapshot(
  input: NorthCarolinaFinanceSnapshotInput
): Promise<NorthCarolinaFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot(input);
}
