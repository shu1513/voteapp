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

export type TexasFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type TexasFinanceLinkSource = "manual" | "tec_bulk";
export type TexasFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type TexasFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type TexasFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type TexasFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: TexasFinanceLinkSource;
};

export type TexasFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type TexasFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type TexasFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type TexasFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type TexasFinanceSnapshotInput = {
  db: Queryable;
  link: TexasFinanceLinkInput;
  syncedAt?: Date;
  summary?: TexasFinanceSummaryInput;
  directBreakdowns?: readonly TexasFinanceDirectBreakdownInput[];
  outsideGroups?: readonly TexasFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly TexasFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type TexasFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Texas",
  tables: {
    links: "tx_candidate_finance_links",
    summaries: "tx_candidate_finance_summaries",
    directBreakdowns: "tx_candidate_finance_direct_breakdowns",
    outsideGroups: "tx_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "tx_candidate_finance_outside_group_breakdowns",
  },
});

export async function upsertTexasFinanceLink(input: {
  db: Queryable;
  link: TexasFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

export async function replaceTexasCandidateFinanceSnapshot(
  input: TexasFinanceSnapshotInput
): Promise<TexasFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot(input);
}
