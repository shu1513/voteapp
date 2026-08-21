import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceSnapshotWriteResult,
} from "../finance/standardStateFinanceSnapshotWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type NewHampshireFinanceLinkStatus = "active" | "inactive";
export type NewHampshireFinanceLinkSource = "manual" | "cfs_registration";
export type NewHampshireFinanceDirectCategoryType = "industry" | "contribution_size";
export type NewHampshireFinanceSupportOppose = "support" | "oppose";

export type NewHampshireFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  filingEntityId: number;
  filerName: string;
  linkStatus?: NewHampshireFinanceLinkStatus;
  linkSource?: NewHampshireFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

export type NewHampshireFinanceSummaryInput = {
  totalReceipts: number | null;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  sourceUrl: string | null;
};

export type NewHampshireFinanceDirectBreakdownInput = {
  categoryType: NewHampshireFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount?: number | null;
  sourceUrl?: string | null;
};

export type NewHampshireFinanceOutsideGroupInput = {
  filingEntityId: number;
  filerName: string;
  supportOppose: NewHampshireFinanceSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type NewHampshireFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: NewHampshireFinanceLinkInput;
  syncedAt?: Date;
  summary?: NewHampshireFinanceSummaryInput;
  directBreakdowns?: readonly NewHampshireFinanceDirectBreakdownInput[];
  outsideGroups?: readonly NewHampshireFinanceOutsideGroupInput[];
};

export type NewHampshireFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

function normalizeFilingEntityId(value: number): string {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid New Hampshire filing entity ID: ${value}`);
  }
  return String(value);
}

function normalizeStoredFilingEntityId(value: string): string {
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid New Hampshire filing entity ID: ${value}`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid New Hampshire filing entity ID: ${value}`);
  }
  return String(parsed);
}

const writer = createStandardStateFinanceSnapshotWriter<NewHampshireFinanceDirectCategoryType>({
  label: "New Hampshire",
  minElectionYear: 2016,
  directCategoryTypes: ["industry", "contribution_size"],
  summaryUpdatePolicy: {
    total_receipts: "replace",
    direct_contribution_total: "replace",
    outside_support_total: "replace",
    outside_oppose_total: "replace",
    source_url: "replace",
  },
  normalizeCommitteeId: normalizeStoredFilingEntityId,
  manualLinkProtection: true,
  supersededLinkSource: "cfs_registration",
  linkIdentityColumns: {
    id: "filing_entity_id",
    name: "filer_name",
  },
  outsideGroupIdentityColumns: {
    id: "filing_entity_id",
    name: "filer_name",
  },
  tables: {
    links: "nh_candidate_finance_links",
    summaries: "nh_candidate_finance_summaries",
    directBreakdowns: "nh_candidate_finance_direct_breakdowns",
    outsideGroups: "nh_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "nh_candidate_finance_outside_group_breakdowns",
  },
});

function toStandardLink(link: NewHampshireFinanceLinkInput) {
  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear: link.electionYear,
    candidateNameNormalized: link.candidateNameNormalized,
    officeName: link.officeName,
    district: link.district,
    committeeId: normalizeFilingEntityId(link.filingEntityId),
    committeeName: link.filerName,
    linkStatus: link.linkStatus,
    linkSource: link.linkSource,
    sourceUrl: link.sourceUrl,
    lastVerifiedAt: link.lastVerifiedAt,
  };
}

export async function upsertNewHampshireFinanceLink(input: {
  db: Queryable;
  link: NewHampshireFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink({ db: input.db, link: toStandardLink(input.link) });
}

export async function replaceNewHampshireCandidateFinanceSnapshot(
  input: NewHampshireFinanceSnapshotInput
): Promise<NewHampshireFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot({
    db: input.db,
    link: toStandardLink(input.link),
    syncedAt: input.syncedAt,
    summary: input.summary,
    directBreakdowns: input.directBreakdowns,
    outsideGroups: input.outsideGroups?.map((group) => ({
      committeeId: normalizeFilingEntityId(group.filingEntityId),
      committeeName: group.filerName,
      supportOppose: group.supportOppose,
      amount: group.amount,
      sourceUrl: group.sourceUrl,
    })),
  });
}
