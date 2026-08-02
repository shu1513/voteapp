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

export type MaineFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type MaineFinanceLinkSource = "manual" | "cfis_bulk";
export type MaineFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type MaineFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type MaineFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type MaineFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: MaineFinanceLinkSource;
};

export type MaineFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type MaineFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type MaineFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type MaineFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type MaineFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: MaineFinanceLinkInput;
  syncedAt?: Date;
  summary?: MaineFinanceSummaryInput;
  directBreakdowns?: readonly MaineFinanceDirectBreakdownInput[];
  outsideGroups?: readonly MaineFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly MaineFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type MaineFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

function normalizeMaineCommitteeId(value: string): string {
  return value.replace(/\s+/g, " ").toUpperCase();
}

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Maine",
  minElectionYear: 2000,
  // Maine replaces every summary column except the outside totals, which keep
  // the stored value when the incoming value is NULL so a partial refresh
  // without expenditure data does not wipe them (preserveWhenNull default).
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    source_url: "replace",
  },
  outsideGroupValidation: "pairing",
  normalizeCommitteeId: normalizeMaineCommitteeId,
  supersededLinkSource: "cfis_bulk",
  tables: {
    links: "me_candidate_finance_links",
    summaries: "me_candidate_finance_summaries",
    directBreakdowns: "me_candidate_finance_direct_breakdowns",
    outsideGroups: "me_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "me_candidate_finance_outside_group_breakdowns",
  },
});

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Maine finance election year: ${value}`);
  }
  return value;
}

function validateNullableDate(value: Date | null | undefined): void {
  if (value && Number.isNaN(value.getTime())) {
    throw new Error("Invalid Maine finance timestamp");
  }
}

function validateMaineFinanceLinkInput(link: MaineFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "Maine finance candidate name");
  requireNonEmpty(link.officeName, "Maine finance office name");
  requireNonEmpty(link.committeeId, "Maine committee id");
  requireNonEmpty(link.committeeName, "Maine committee name");
  validateNullableDate(link.lastVerifiedAt);
}

function outsideGroupKey(group: Pick<MaineFinanceOutsideGroupInput, "committeeId" | "supportOppose">): string {
  return `${normalizeMaineCommitteeId(requireNonEmpty(group.committeeId, "Maine committee id"))}\u0000${group.supportOppose}`;
}

// The factory's pairing validation covers the same inputs, but Maine's
// original writer phrased its errors differently ("require matching outside
// groups...") and rejected a defined-but-empty breakdown list with no groups
// key at all. Per-state tests pin both behaviors, so the wrapper validates
// first with the legacy semantics; the factory's own pairing check then acts
// as a backstop and never fires on input that passes here.
function validateMaineFinanceSnapshotInput(input: MaineFinanceSnapshotInput): void {
  validateMaineFinanceLinkInput(input.link);
  if (input.outsideGroupBreakdowns && !input.outsideGroups) {
    throw new Error("Maine outside group breakdowns require outside groups in the same snapshot");
  }
  if (input.outsideGroupBreakdowns?.length) {
    const groupKeys = new Set((input.outsideGroups ?? []).map(outsideGroupKey));
    for (const breakdown of input.outsideGroupBreakdowns) {
      if (!groupKeys.has(outsideGroupKey(breakdown))) {
        throw new Error("Maine outside group breakdowns require matching outside groups in the same snapshot");
      }
    }
  }
}

// Maine snapshot writes must receive a Pool: a bare queryable without
// connect() is rejected instead of taking the factory's inline-transaction
// path. The stub throws on first use rather than up front so that input
// validation errors still surface before the transaction-contract error.
function requireMainePool(db: Queryable): Queryable {
  const candidate = db as { connect?: unknown; release?: unknown };
  if (typeof candidate.connect === "function" || typeof candidate.release === "function") {
    return db;
  }
  const rejectQuery = () => {
    throw new Error("Maine finance snapshot writes must receive a Pool");
  };
  return { query: rejectQuery as unknown as Queryable["query"] };
}

export async function upsertMaineFinanceLink(input: {
  db: Queryable;
  link: MaineFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

export async function replaceMaineCandidateFinanceSnapshot(
  input: MaineFinanceSnapshotInput
): Promise<MaineFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid Maine finance sync timestamp");
  }
  validateMaineFinanceSnapshotInput(input);
  return writer.replaceSnapshot({ ...input, syncedAt, db: requireMainePool(input.db) });
}
