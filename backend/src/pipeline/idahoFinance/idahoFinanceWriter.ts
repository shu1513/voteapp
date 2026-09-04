// Idaho finance snapshot writer (docs/plans/idaho-finance.md, Phase 1). Thin
// wrapper over the standard state-finance writer, mirroring New Hampshire
// (same Civix CFIS build) with Idaho's identities:
//
// - link identity = the Sunshine registration guid (registration_guid,
//   lowercase uuid text) with the grid filerName in filer_name;
// - direct breakdowns are size buckets or contributor source types
//   (migration 268) — Idaho collects no occupation or employer;
// - outside groups are IE filers keyed by filer_key (registration guid when
//   the filer is registered in Idaho, otherwise "fec:<id>" or
//   "name:<normalized name>" — idahoOutsideFilerKey) with the filer display
//   name in filer_name;
// - cash_on_hand is a signed balance (the grid reports negative
//   balanceOfFunds for indebted campaigns).

import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceDirectBreakdownInput,
  type StandardStateFinanceLinkStatus,
  type StandardStateFinanceSnapshotWriteResult,
  type StandardStateFinanceSupportOppose,
} from "../finance/standardStateFinanceSnapshotWriter.js";
import { normalizeIdahoRegistrationGuid } from "./idahoCfsClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export type IdahoFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type IdahoFinanceLinkSource = "manual" | "sunshine_grid";
export type IdahoFinanceDirectCategoryType = "contribution_size" | "contributor_source_type";
export type IdahoFinanceSupportOppose = StandardStateFinanceSupportOppose;

export const IDAHO_FINANCE_AUTOMATIC_LINK_SOURCE: IdahoFinanceLinkSource = "sunshine_grid";

export type IdahoFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  /** Sunshine registration guid (grid `guid`). */
  registrationGuid: string;
  filerName: string;
  linkStatus?: IdahoFinanceLinkStatus;
  linkSource?: IdahoFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

/** NULL amounts preserve stored data; zero means a successful empty result. */
export type IdahoFinanceSummaryInput = {
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  /** Signed balance; negative means the campaign is indebted. */
  cashOnHand: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  sourceUrl: string | null;
};

export type IdahoFinanceDirectBreakdownInput = StandardStateFinanceDirectBreakdownInput<IdahoFinanceDirectCategoryType>;

export type IdahoFinanceOutsideGroupInput = {
  filerKey: string;
  filerName: string;
  supportOppose: IdahoFinanceSupportOppose;
  amount: number;
  sourceUrl?: string | null;
};

export type IdahoFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: IdahoFinanceLinkInput;
  syncedAt?: Date;
  summary?: IdahoFinanceSummaryInput;
  /** Omit when unavailable; pass [] after a successful fetch with no breakdowns. */
  directBreakdowns?: readonly IdahoFinanceDirectBreakdownInput[];
  /** Omit when unavailable; pass [] after a successful fetch with no outside groups. */
  outsideGroups?: readonly IdahoFinanceOutsideGroupInput[];
};

export type IdahoFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

/** Storage form of a roster name: uppercase ASCII tokens, suffixes kept. */
export function normalizeIdahoCandidateNameForStorage(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .trim();
}

const writer = createStandardStateFinanceSnapshotWriter<IdahoFinanceDirectCategoryType>({
  label: "Idaho",
  // Migration 268 constrains election_year to 2026+ (Nov-2026 scope).
  minElectionYear: 2026,
  directCategoryTypes: ["contribution_size", "contributor_source_type"],
  allowNegativeCashOnHand: true,
  manualLinkProtection: true,
  supersededLinkSource: IDAHO_FINANCE_AUTOMATIC_LINK_SOURCE,
  linkIdentityColumns: {
    id: "registration_guid",
    name: "filer_name",
  },
  outsideGroupIdentityColumns: {
    id: "filer_key",
    name: "filer_name",
  },
  tables: {
    links: "id_candidate_finance_links",
    summaries: "id_candidate_finance_summaries",
    directBreakdowns: "id_candidate_finance_direct_breakdowns",
    outsideGroups: "id_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "id_candidate_finance_outside_group_breakdowns",
  },
});

function toStandardLink(link: IdahoFinanceLinkInput) {
  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear: link.electionYear,
    candidateNameNormalized: link.candidateNameNormalized,
    officeName: link.officeName,
    district: link.district,
    committeeId: normalizeIdahoRegistrationGuid(link.registrationGuid),
    committeeName: link.filerName,
    linkStatus: link.linkStatus,
    linkSource: link.linkSource,
    sourceUrl: link.sourceUrl,
    lastVerifiedAt: link.lastVerifiedAt,
  };
}

export async function upsertIdahoFinanceLink(input: {
  db: Queryable;
  link: IdahoFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink({ db: input.db, link: toStandardLink(input.link) });
}

export async function replaceIdahoCandidateFinanceSnapshot(
  input: IdahoFinanceSnapshotInput
): Promise<IdahoFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot({
    db: input.db,
    link: toStandardLink(input.link),
    syncedAt: input.syncedAt,
    summary: input.summary,
    directBreakdowns: input.directBreakdowns,
    outsideGroups: input.outsideGroups?.map((group) => ({
      committeeId: group.filerKey,
      committeeName: group.filerName,
      supportOppose: group.supportOppose,
      amount: group.amount,
      sourceUrl: group.sourceUrl,
    })),
  });
}
