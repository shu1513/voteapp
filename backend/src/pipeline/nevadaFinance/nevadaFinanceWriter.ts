import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceSnapshotWriteResult,
} from "../finance/standardStateFinanceSnapshotWriter.js";
import { nevadaFilerKey } from "./nevadaAuroraCsv.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type NevadaFinanceLinkStatus = "active" | "inactive";
export type NevadaFinanceLinkSource = "manual" | "aurora_search";
export type NevadaFinanceDirectCategoryType = "industry" | "contribution_size";

export type NevadaFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  /** AURORA filer display name; the stored key is its normalized form. */
  filerName: string;
  linkStatus?: NevadaFinanceLinkStatus;
  linkSource?: NevadaFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

/** NULL amounts preserve stored data; zero means a successful empty result. */
export type NevadaFinanceSummaryInput = {
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  cashOnHand: number | null;
  sourceUrl: string | null;
};

export type NevadaFinanceDirectBreakdownInput = {
  categoryType: NevadaFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type NevadaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: NevadaFinanceLinkInput;
  syncedAt?: Date;
  summary?: NevadaFinanceSummaryInput;
  /** Omit when unavailable; pass [] after a successful run with no breakdowns. */
  directBreakdowns?: readonly NevadaFinanceDirectBreakdownInput[];
};

export type NevadaFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

function normalizeStoredFilerKey(value: string): string {
  const normalized = nevadaFilerKey(value);
  if (normalized.length === 0) {
    throw new Error(`Invalid Nevada filer key ${JSON.stringify(value)}`);
  }
  return normalized;
}

// Nevada SOS report data carries no candidate target or support/oppose
// direction, so outside groups are never written; the outside tables exist
// only to satisfy the shared table contract.
const writer = createStandardStateFinanceSnapshotWriter<NevadaFinanceDirectCategoryType>({
  label: "Nevada",
  minElectionYear: 2004,
  directCategoryTypes: ["industry", "contribution_size"],
  summaryUpdatePolicy: {
    total_receipts: "preserveWhenNull",
    direct_contribution_total: "preserveWhenNull",
    total_disbursements: "preserveWhenNull",
    cash_on_hand: "preserveWhenNull",
    source_url: "preserveWhenNull",
  },
  normalizeCommitteeId: normalizeStoredFilerKey,
  manualLinkProtection: true,
  supersededLinkSource: "aurora_search",
  linkIdentityColumns: {
    id: "filer_key",
    name: "filer_name",
  },
  outsideGroupIdentityColumns: {
    id: "filer_key",
    name: "filer_name",
  },
  tables: {
    links: "nv_candidate_finance_links",
    summaries: "nv_candidate_finance_summaries",
    directBreakdowns: "nv_candidate_finance_direct_breakdowns",
    outsideGroups: "nv_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "nv_candidate_finance_outside_group_breakdowns",
  },
});

function toStandardLink(link: NevadaFinanceLinkInput) {
  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear: link.electionYear,
    candidateNameNormalized: link.candidateNameNormalized,
    officeName: link.officeName,
    district: link.district,
    committeeId: normalizeStoredFilerKey(link.filerName),
    committeeName: link.filerName,
    linkStatus: link.linkStatus,
    linkSource: link.linkSource,
    sourceUrl: link.sourceUrl,
    lastVerifiedAt: link.lastVerifiedAt,
  };
}

export async function upsertNevadaFinanceLink(input: {
  db: Queryable;
  link: NevadaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink({ db: input.db, link: toStandardLink(input.link) });
}

export async function replaceNevadaCandidateFinanceSnapshot(
  input: NevadaFinanceSnapshotInput
): Promise<NevadaFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot({
    db: input.db,
    link: toStandardLink(input.link),
    syncedAt: input.syncedAt,
    summary: input.summary,
    directBreakdowns: input.directBreakdowns,
  });
}
