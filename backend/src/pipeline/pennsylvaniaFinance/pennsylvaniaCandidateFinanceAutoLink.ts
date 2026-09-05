import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import {
  normalizePennsylvaniaCandidateNameForStorage,
  resolvePennsylvaniaCandidateCommittee,
  type PennsylvaniaCandidateCommitteeResolution,
} from "./pennsylvaniaCandidateCommitteeResolver.js";
import { PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./pennsylvaniaFinanceEligibleOffices.js";
import { upsertPennsylvaniaFinanceLink } from "./pennsylvaniaFinanceWriter.js";
import type { PennsylvaniaCampaignFinanceFilerRow } from "./pennsylvaniaCampaignFinanceReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type PennsylvaniaFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type PennsylvaniaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: PennsylvaniaCandidateCommitteeResolution["status"] | "linked";
      filerId?: string;
      reason?: string;
    }
  | {
      candidateId: string;
      electionId: string;
      status: "error";
      reason: "auto_link_failed";
      error: string;
    };

// Auto-link intentionally passes no maxCandidates (LIMIT ALL): unmatched
// candidates never get a link, so a capped, stably-ordered prefix would be
// retried every run and starve the tail. maxCandidates still caps the sync.
export const listPennsylvaniaCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "PA",
    linksTable: "pa_candidate_finance_links",
    eligibleOfficeKeys: [...PENNSYLVANIA_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkPennsylvaniaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: PennsylvaniaFinanceAutoLinkCandidateElection;
  filerRows: readonly PennsylvaniaCampaignFinanceFilerRow[];
  sourceUrl: string | null;
  now: Date;
}): Promise<PennsylvaniaFinanceAutoLinkResult> {
  const resolution = resolvePennsylvaniaCandidateCommittee({
    candidateName: input.candidateElection.candidateName,
    officeScope: input.candidateElection.officeScope,
    officeName: input.candidateElection.officeName,
    electionYear: input.candidateElection.electionYear,
    district: input.candidateElection.district,
    filerRows: input.filerRows,
    sourceUrl: input.sourceUrl,
  });

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertPennsylvaniaFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizePennsylvaniaCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: input.candidateElection.officeName,
      district: input.candidateElection.district,
      filerId: resolution.filerId,
      filerName: resolution.filerName,
      linkStatus: "active",
      linkSource: "pa_bulk",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });

  return {
    candidateId: input.candidateElection.candidateId,
    electionId: input.candidateElection.electionId,
    status: "linked",
    filerId: resolution.filerId,
  };
}

export async function autoLinkMissingPennsylvaniaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  candidateElections: readonly PennsylvaniaFinanceAutoLinkCandidateElection[];
  filerRowsByElectionYear: ReadonlyMap<number, readonly PennsylvaniaCampaignFinanceFilerRow[]>;
  sourceUrlByElectionYear?: ReadonlyMap<number, string>;
}): Promise<PennsylvaniaFinanceAutoLinkResult[]> {
  const results: PennsylvaniaFinanceAutoLinkResult[] = [];
  for (const candidateElection of input.candidateElections) {
    try {
      results.push(
        await autoLinkPennsylvaniaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          filerRows: input.filerRowsByElectionYear.get(candidateElection.electionYear) ?? [],
          sourceUrl: input.sourceUrlByElectionYear?.get(candidateElection.electionYear) ?? null,
          now: input.now,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Pennsylvania finance auto-link failed for candidate election; continuing:", {
        candidateId: candidateElection.candidateId,
        electionId: candidateElection.electionId,
        electionYear: candidateElection.electionYear,
        error: message,
      });
      results.push({
        candidateId: candidateElection.candidateId,
        electionId: candidateElection.electionId,
        status: "error",
        reason: "auto_link_failed",
        error: message,
      });
    }
  }
  return results;
}
