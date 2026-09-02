// North Dakota finance snapshot writer (plan-north-dakota-finance.md,
// Phase 1). Thin wrapper over the standard state-finance writer. Identity:
// the registry `entityId` (10 digits, == bulk-CSV RegistrantID == API
// entityID) in committee_id; the internal orgID is an acquisition key only.
//
// Component isolation (plan "Fail-closed rules"): the direct component,
// the year-end disbursement lump, statewide cash and the IE component are
// published by different phases and fail independently, so every summary
// column keeps the writer's default preserve-when-NULL policy — a caller
// passes only the components it recomputed and NULL leaves the rest alone.
// NULL is "unavailable", never $0. Outside breakdowns must pair with a
// supplied group (pairing validation) so a funder row can never outlive its
// spender row.

import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceSnapshotWriter,
  type StandardStateFinanceDirectBreakdownInput,
  type StandardStateFinanceDirectCategoryType,
  type StandardStateFinanceLinkStatus,
  type StandardStateFinanceOutsideGroupBreakdownInput,
  type StandardStateFinanceOutsideGroupInput,
  type StandardStateFinanceSnapshotWriteResult,
} from "../finance/standardStateFinanceSnapshotWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect: () => Promise<PoolClient>;
};

export const NORTH_DAKOTA_CFRS_SOURCE_URL = "https://cfrs.sos.nd.gov/";

export type NorthDakotaFinanceLinkStatus = StandardStateFinanceLinkStatus;
export type NorthDakotaFinanceLinkSource = "manual" | "cfrs_registry";
export type NorthDakotaFinanceDirectCategoryType = StandardStateFinanceDirectCategoryType;

export const NORTH_DAKOTA_FINANCE_AUTOMATIC_LINK_SOURCE: NorthDakotaFinanceLinkSource = "cfrs_registry";

export type NorthDakotaFinanceLinkInput = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  candidateNameNormalized: string;
  officeName: string;
  district?: string | null;
  /** Registry entityId == bulk-CSV RegistrantID == API entityID (10 digits). */
  entityId: string;
  committeeName: string;
  linkStatus?: NorthDakotaFinanceLinkStatus;
  linkSource?: NorthDakotaFinanceLinkSource;
  sourceUrl?: string | null;
  lastVerifiedAt?: Date | null;
};

/** NULL preserves the stored value (component not recomputed); zero is a real total. */
export type NorthDakotaFinanceSummaryInput = {
  totalReceipts?: number | null;
  directContributionTotal?: number | null;
  totalDisbursements?: number | null;
  /** Statewide filers only report fund balances; everyone else stays NULL. */
  cashOnHand?: number | null;
  outsideSupportTotal?: number | null;
  outsideOpposeTotal?: number | null;
  sourceUrl?: string | null;
};

export type NorthDakotaFinanceDirectBreakdownInput =
  StandardStateFinanceDirectBreakdownInput<NorthDakotaFinanceDirectCategoryType>;
export type NorthDakotaFinanceOutsideGroupInput = StandardStateFinanceOutsideGroupInput;
export type NorthDakotaFinanceOutsideGroupBreakdownInput = StandardStateFinanceOutsideGroupBreakdownInput;

export type NorthDakotaFinanceSnapshotInput = {
  db: ConnectableQueryable;
  link: NorthDakotaFinanceLinkInput;
  syncedAt?: Date;
  summary?: NorthDakotaFinanceSummaryInput;
  /** Omit to leave stored rows alone; pass [] to clear them. */
  directBreakdowns?: readonly NorthDakotaFinanceDirectBreakdownInput[];
  outsideGroups?: readonly NorthDakotaFinanceOutsideGroupInput[];
  outsideGroupBreakdowns?: readonly NorthDakotaFinanceOutsideGroupBreakdownInput[];
};

export type NorthDakotaFinanceSnapshotWriteResult = StandardStateFinanceSnapshotWriteResult;

/** Registry entityId / bulk RegistrantID: exactly ten digits ("1010001478"). */
export function normalizeNorthDakotaEntityId(value: string): string {
  const normalized = value.trim();
  if (!/^\d{10}$/.test(normalized)) {
    throw new Error(`Invalid North Dakota CFRS entityId: ${value}`);
  }
  return normalized;
}

const writer = createStandardStateFinanceSnapshotWriter({
  label: "North Dakota",
  // Migration 269 constrains election_year to 2026+ (Phase 0A gate 4).
  minElectionYear: 2026,
  outsideGroupValidation: "pairing",
  normalizeCommitteeId: normalizeNorthDakotaEntityId,
  manualLinkProtection: true,
  supersededLinkSource: NORTH_DAKOTA_FINANCE_AUTOMATIC_LINK_SOURCE,
  tables: {
    links: "nd_candidate_finance_links",
    summaries: "nd_candidate_finance_summaries",
    directBreakdowns: "nd_candidate_finance_direct_breakdowns",
    outsideGroups: "nd_candidate_finance_outside_groups",
    outsideGroupBreakdowns: "nd_candidate_finance_outside_group_breakdowns",
  },
});

function toStandardLink(link: NorthDakotaFinanceLinkInput) {
  return {
    candidateId: link.candidateId,
    electionId: link.electionId,
    electionYear: link.electionYear,
    candidateNameNormalized: link.candidateNameNormalized,
    officeName: link.officeName,
    district: link.district,
    committeeId: normalizeNorthDakotaEntityId(link.entityId),
    committeeName: link.committeeName,
    linkStatus: link.linkStatus,
    linkSource: link.linkSource,
    sourceUrl: link.sourceUrl,
    lastVerifiedAt: link.lastVerifiedAt,
  };
}

export async function upsertNorthDakotaFinanceLink(input: {
  db: Queryable;
  link: NorthDakotaFinanceLinkInput;
}): Promise<{ linkId: string }> {
  return writer.upsertLink({ db: input.db, link: toStandardLink(input.link) });
}

export async function replaceNorthDakotaCandidateFinanceSnapshot(
  input: NorthDakotaFinanceSnapshotInput
): Promise<NorthDakotaFinanceSnapshotWriteResult> {
  return writer.replaceSnapshot({
    db: input.db,
    link: toStandardLink(input.link),
    syncedAt: input.syncedAt,
    summary: input.summary,
    directBreakdowns: input.directBreakdowns,
    outsideGroups: input.outsideGroups,
    outsideGroupBreakdowns: input.outsideGroupBreakdowns,
  });
}
