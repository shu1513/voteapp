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

type Queryable = Pick<Pool | PoolClient, "query">;

export type ArizonaFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type ArizonaFinanceLinkSource = "manual" | "spotlight";
export type ArizonaFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type ArizonaFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type ArizonaFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type ArizonaFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: ArizonaFinanceLinkSource;
};

export type ArizonaFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type ArizonaFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type ArizonaFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type ArizonaFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type ArizonaFinanceSnapshotInput = {
  db: Queryable;
  link: ArizonaFinanceLinkInput;
  syncedAt?: Date;
  summary?: ArizonaFinanceSummaryInput;
  directBreakdowns?: readonly ArizonaFinanceDirectBreakdownInput[];
  outsideGroups?: readonly ArizonaFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly ArizonaFinanceOutsideGroupBreakdownInput[];
};

export type ArizonaFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Arizona",
  minElectionYear: 2002,
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    outside_support_total: "replace",
    outside_oppose_total: "replace",
    source_url: "replace",
  },
  outsideGroupValidation: "presence",
  tables: {
    links: "az_candidate_finance_links",
    summaries: "az_candidate_finance_summaries",
    directBreakdowns: "az_candidate_finance_direct_breakdowns",
    outsideGroups: "az_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "az_candidate_finance_outside_group_breakdowns",
  },
});

export async function upsertArizonaFinanceLink(input: {
  db: Queryable;
  link: ArizonaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

// Arizona snapshot writes must receive a Pool: a bare queryable without
// connect() is rejected instead of taking the factory's inline-transaction
// path. The stub throws on first use rather than up front so that input
// validation errors still surface before the transaction-contract error.
function requireArizonaPool(db: Queryable): Queryable {
  const candidate = db as { connect?: unknown; release?: unknown };
  if (typeof candidate.connect === "function" || typeof candidate.release === "function") {
    return db;
  }
  const rejectQuery = () => {
    throw new Error("Arizona finance snapshot writes must receive a Pool with connect()");
  };
  return { query: rejectQuery as unknown as Queryable["query"] };
}

export async function replaceArizonaCandidateFinanceSnapshot(
  input: ArizonaFinanceSnapshotInput
): Promise<ArizonaFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot({ ...input, db: requireArizonaPool(input.db) });
}
