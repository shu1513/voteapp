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

export type MarylandFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type MarylandFinanceLinkSource = "manual" | "cfs_public_export";
export type MarylandFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;
export type MarylandFinanceOutsideCategoryType = StandardStateFinanceOutsideCategoryType;
export type MarylandFinanceSupportOppose = StandardStateFinanceSupportOppose;

export type MarylandFinanceLinkInput = Omit<StandardStateFinanceLinkInput, "linkSource"> & {
  linkSource?: MarylandFinanceLinkSource;
};

export type MarylandFinanceSummaryInput = StandardStateFinanceSummaryInput;
export type MarylandFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput;
export type MarylandFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type MarylandFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type MarylandFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: MarylandFinanceLinkInput;
  syncedAt?: Date;
  summary?: MarylandFinanceSummaryInput;
  directBreakdowns?: readonly MarylandFinanceDirectBreakdownInput[];
  outsideGroups?: readonly MarylandFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly MarylandFinanceOutsideGroupBreakdownInput[];
  classifications?: readonly FinanceLabelClassification[];
};

export type MarylandFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

// Unlike Maine's normalizer, Maryland's collapses whitespace without
// uppercasing.
function normalizeMarylandCommitteeId(value: string): string {
  return value.replace(/\s+/g, " ");
}

const writer = createStandardStateFinanceSnapshotWriter({
  label: "Maryland",
  minElectionYear: 2000,
  // Maryland replaces every summary column except the outside totals, which
  // keep the stored value when the incoming value is NULL so a partial refresh
  // without expenditure data does not wipe them (preserveWhenNull default).
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    source_url: "replace",
  },
  outsideGroupValidation: "pairing",
  normalizeCommitteeId: normalizeMarylandCommitteeId,
  supersededLinkSource: "cfs_public_export",
  tables: {
    links: "md_candidate_finance_links",
    summaries: "md_candidate_finance_summaries",
    directBreakdowns: "md_candidate_finance_direct_breakdowns",
    outsideGroups: "md_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "md_candidate_finance_outside_group_breakdowns",
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
    throw new Error(`Invalid Maryland finance election year: ${value}`);
  }
  return value;
}

function validateNullableDate(value: Date | null | undefined): void {
  if (value && Number.isNaN(value.getTime())) {
    throw new Error("Invalid Maryland finance timestamp");
  }
}

function validateMarylandFinanceLinkInput(link: MarylandFinanceLinkInput): void {
  requireNonEmpty(link.candidateId, "candidate id");
  requireNonEmpty(link.electionId, "election id");
  normalizeElectionYear(link.electionYear);
  requireNonEmpty(link.candidateNameNormalized, "Maryland finance candidate name");
  requireNonEmpty(link.officeName, "Maryland finance office name");
  requireNonEmpty(link.committeeId, "Maryland committee id");
  requireNonEmpty(link.committeeName, "Maryland committee name");
  validateNullableDate(link.lastVerifiedAt);
}

function outsideGroupKey(group: Pick<MarylandFinanceOutsideGroupInput, "committeeId" | "supportOppose">): string {
  return `${normalizeMarylandCommitteeId(requireNonEmpty(group.committeeId, "Maryland committee id"))}\u0000${group.supportOppose}`;
}

// The factory's pairing validation covers the same inputs, but Maryland's
// original writer phrased its errors differently ("require matching outside
// groups...") and rejected a defined-but-empty breakdown list with no groups
// key at all. Per-state tests pin both behaviors, so the wrapper validates
// first with the legacy semantics; the factory's own pairing check then acts
// as a backstop and never fires on input that passes here.
function validateMarylandFinanceSnapshotInput(input: MarylandFinanceSnapshotInput): void {
  validateMarylandFinanceLinkInput(input.link);
  if (input.outsideGroupBreakdowns && !input.outsideGroups) {
    throw new Error("Maryland outside group breakdowns require outside groups in the same snapshot");
  }
  if (input.outsideGroupBreakdowns?.length) {
    const groupKeys = new Set((input.outsideGroups ?? []).map(outsideGroupKey));
    for (const breakdown of input.outsideGroupBreakdowns) {
      if (!groupKeys.has(outsideGroupKey(breakdown))) {
        throw new Error("Maryland outside group breakdowns require matching outside groups in the same snapshot");
      }
    }
  }
}

// Maryland snapshot writes must receive a Pool: a bare queryable without
// connect() is rejected instead of taking the factory's inline-transaction
// path. The stub throws on first use rather than up front so that input
// validation errors still surface before the transaction-contract error.
function requireMarylandPool(db: Queryable): Queryable {
  const candidate = db as { connect?: unknown; release?: unknown };
  if (typeof candidate.connect === "function" || typeof candidate.release === "function") {
    return db;
  }
  const rejectQuery = () => {
    throw new Error("Maryland finance snapshot writes must receive a Pool");
  };
  return { query: rejectQuery as unknown as Queryable["query"] };
}

export async function upsertMarylandFinanceLink(input: {
  db: Queryable;
  link: MarylandFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink(input);
}

export async function replaceMarylandCandidateFinanceSnapshot(
  input: MarylandFinanceSnapshotInput
): Promise<MarylandFinanceSnapshotWriteResult> {
  const syncedAt = input.syncedAt ?? new Date();
  if (Number.isNaN(syncedAt.getTime())) {
    throw new Error("Invalid Maryland finance sync timestamp");
  }
  validateMarylandFinanceSnapshotInput(input);
  return writer.replaceSnapshot({ ...input, syncedAt, db: requireMarylandPool(input.db) });
}
