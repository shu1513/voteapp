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

export type GeorgiaFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type GeorgiaFinanceLinkSource = "manual" | "peachfile_api";
export type GeorgiaFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type GeorgiaFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type GeorgiaFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type GeorgiaFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: GeorgiaFinanceLinkSource;
};

export type GeorgiaFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type GeorgiaFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type GeorgiaFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type GeorgiaFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type GeorgiaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: GeorgiaFinanceLinkInput;
  syncedAt?: Date;
  summary?: GeorgiaFinanceSummaryInput;
  directBreakdowns?: readonly GeorgiaFinanceDirectBreakdownInput[];
  outsideGroups?: readonly GeorgiaFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly GeorgiaFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type GeorgiaFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Georgia",
  // Link identity is the PeachFile filerEntityId, which only exists for
  // PeachFile-era (2026-cycle) filers — archive-only 2022–2025 entities are
  // out of v1 link scope (georgia_plan.md D7). The exact filerEntityId
  // validation shape is pinned from acquisition-spike bytes; until then the
  // factory's nonempty check is the only structural validation.
  minElectionYear: 2026,
  // Georgia replaces every summary column except the outside totals, which
  // keep the stored value when the incoming value is NULL so a direct-only
  // refresh without IE report data does not wipe them (preserveWhenNull
  // default). direct_contribution_total stays NULL in v1 (georgia_plan.md
  // D4: the official candidate-index totals include loans, interest, and
  // unitemized money, so the shared loader must fall through to
  // total_receipts) — "replace" keeps a stray stored value from surviving a
  // later sync.
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    source_url: "replace",
  },
  outsideGroupValidation: "pairing",
  supersededLinkSource: "peachfile_api",
  tables: {
    links: "ga_candidate_finance_links",
    summaries: "ga_candidate_finance_summaries",
    directBreakdowns: "ga_candidate_finance_direct_breakdowns",
    outsideGroups: "ga_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "ga_candidate_finance_outside_group_breakdowns",
  },
});

export async function upsertGeorgiaFinanceLink(input: {
  db: Queryable;
  link: GeorgiaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

export async function replaceGeorgiaCandidateFinanceSnapshot(
  input: GeorgiaFinanceSnapshotInput
): Promise<GeorgiaFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot(input);
}
