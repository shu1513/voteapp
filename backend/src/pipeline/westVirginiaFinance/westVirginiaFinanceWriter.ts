// West Virginia finance snapshot writer (plan-west-virginia-finance.md,
// Phase 1). Thin wrapper over the standard state-finance writer. Identity:
// the registry `entityId` (10 digits, == bulk-CSV RegistrantID) in
// committee_id; the internal orgID is an acquisition key only.
//
// Phase 1 publishes direct money only. cash_on_hand needs filed-report
// cover extraction (not built yet) and outside support/oppose lives in
// scanned F-7b PDFs (Phase 3), so both are forced to NULL on every write —
// unavailable, never $0 — and the empty outside arrays make the shared
// writer delete any stray outside rows.

import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceDirectBreakdownInput,
  type StandardStateFinanceLinkStatus,
  type StandardStateFinanceSnapshotWriteResult,
} from "../finance/standardStateFinanceSnapshotWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export const WEST_VIRGINIA_CFRS_SOURCE_URL = "https://cfrs.wvsos.gov/";

export type WestVirginiaFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type WestVirginiaFinanceLinkSource = "manual" | "cfrs_registry";
export type WestVirginiaFinanceDirectCategoryType = "occupation" | "industry" | "contribution_size";

export type WestVirginiaFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  /** Registry entityId == bulk-CSV RegistrantID (10 digits). */
  entityId: string;
  committeeName: string;
  linkStatus?: WestVirginiaFinanceLinkStatus;
  linkSource?: WestVirginiaFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type WestVirginiaFinanceSummaryInput = {
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  sourceUrl: string | null;
};

export type WestVirginiaFinanceDirectBreakdownInput =
  StandardStateFinanceDirectBreakdownInput<WestVirginiaFinanceDirectCategoryType>;

export type WestVirginiaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: WestVirginiaFinanceLinkInput;
  syncedAt?: Date;
  summary?: WestVirginiaFinanceSummaryInput;
  /** Omit to leave stored breakdowns alone; pass [] to clear them. */
  directBreakdowns?: readonly WestVirginiaFinanceDirectBreakdownInput[];
};

export type WestVirginiaFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

/** Registry entityId / bulk RegistrantID: exactly ten digits ("1010003610"). */
export function normalizeWestVirginiaEntityId(value: string): string {
  const normalized = value.trim();
  if (!/^\d{10}$/.test(normalized)) {
    throw new Error(`Invalid West Virginia CFRS entityId: ${value}`);
  }
  return normalized;
}

const writer = createStandardStateFinanceSnapshotWriter<WestVirginiaFinanceDirectCategoryType>({
  label: "West Virginia",
  // Migration 267 constrains election_year to 2026+ (Nov-2026 scope).
  minElectionYear: 2026,
  directCategoryTypes: ["occupation", "industry", "contribution_size"],
  summaryUpdatePolicy: {
    // Every snapshot is a full recomputation from the cached artifacts, so
    // every column replaces — including the always-NULL cash and outside
    // columns, so a stray historical value can never survive a sync.
    total_receipts: "replace",
    direct_contribution_total: "replace",
    total_disbursements: "replace",
    cash_on_hand: "replace",
    outside_support_total: "replace",
    outside_oppose_total: "replace",
    source_url: "replace",
  },
  normalizeCommitteeId: normalizeWestVirginiaEntityId,
  manualLinkProtection: true,
  supersededLinkSource: "cfrs_registry",
  tables: {
    links: "wv_candidate_finance_links",
    summaries: "wv_candidate_finance_summaries",
    directBreakdowns: "wv_candidate_finance_direct_breakdowns",
    outsideGroups: "wv_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "wv_candidate_finance_outside_group_breakdowns",
  },
});

function toStandardLink(link: WestVirginiaFinanceLinkInput) {
  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear: link.electionYear,
    candidateNameNormalized: link.candidateNameNormalized,
    officeName: link.officeName,
    district: link.district,
    committeeId: normalizeWestVirginiaEntityId(link.entityId),
    committeeName: link.committeeName,
    linkStatus: link.linkStatus,
    linkSource: link.linkSource,
    sourceUrl: link.sourceUrl,
    lastVerifiedAt: link.lastVerifiedAt,
  };
}

export async function upsertWestVirginiaFinanceLink(input: {
  db: Queryable;
  link: WestVirginiaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink({ db: input.db, link: toStandardLink(input.link) });
}

export async function replaceWestVirginiaCandidateFinanceSnapshot(
  input: WestVirginiaFinanceSnapshotInput
): Promise<WestVirginiaFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot({
    db: input.db,
    link: toStandardLink(input.link),
    syncedAt: input.syncedAt,
    summary:
      input.summary === undefined
        ? undefined
        : {
            ...input.summary,
            cashOnHand: null,
            outsideSupportTotal: null,
            outsideOpposeTotal: null,
          },
    directBreakdowns: input.directBreakdowns,
    outsideGroups: [],
    outsideGroupBreakdowns: [],
  });
}
