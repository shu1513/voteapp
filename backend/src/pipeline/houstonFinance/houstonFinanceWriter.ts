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

export type HoustonFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type HoustonFinanceLinkSource = "manual" | "houston_reports";
export type HoustonFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type HoustonFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type HoustonFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type HoustonFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: HoustonFinanceLinkSource;
};

export type HoustonFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type HoustonFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type HoustonFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type HoustonFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type HoustonFinanceSnapshotInput = {
  db: Queryable;
  link: HoustonFinanceLinkInput;
  syncedAt?: Date;
  summary?: HoustonFinanceSummaryInput;
  directBreakdowns?: readonly HoustonFinanceDirectBreakdownInput[];
  outsideGroups?: readonly HoustonFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly HoustonFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type HoustonFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Houston",
  minElectionYear: 2014,
  tables: {
    links: "hou_candidate_finance_links",
    summaries: "hou_candidate_finance_summaries",
    directBreakdowns: "hou_candidate_finance_direct_breakdowns",
    outsideGroups: "hou_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "hou_candidate_finance_outside_group_breakdowns",
  },
});

export async function upsertHoustonFinanceLink(input: {
  db: Queryable;
  link: HoustonFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

export async function replaceHoustonCandidateFinanceSnapshot(
  input: HoustonFinanceSnapshotInput
): Promise<HoustonFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot(input);
}
