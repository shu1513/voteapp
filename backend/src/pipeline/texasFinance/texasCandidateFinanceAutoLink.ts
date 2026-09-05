import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import {
  normalizeTexasCandidateNameKeys,
  resolveTexasCandidateCommittee,
  type TexasCandidateCommitteeResolution,
} from "./texasCandidateCommitteeResolver.js";
import { TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./texasFinanceEligibleOffices.js";
import { upsertTexasFinanceLink } from "./texasFinanceWriter.js";
import type { TexasTecFilerRow } from "./texasTecCsvDatabaseReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type TexasFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type TexasFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: TexasCandidateCommitteeResolution["status"] | "linked";
      committeeId?: string;
      reason?: string;
    }
  | {
      candidateId: string;
      electionId: string;
      status: "error";
      reason: "auto_link_failed";
      error: string;
    };

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeTexasCandidateNameKeys(value);
  return [...keys][0] ?? value.trim().replace(/\s+/g, " ").toUpperCase();
}

export const listTexasCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "TX",
    linksTable: "tx_candidate_finance_links",
    eligibleOfficeKeys: [...TEXAS_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkTexasCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: TexasFinanceAutoLinkCandidateElection;
  filerRows: readonly TexasTecFilerRow[];
  sourceUrl: string | null;
  now: Date;
}): Promise<TexasFinanceAutoLinkResult> {
  const resolution = resolveTexasCandidateCommittee({
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

  await upsertTexasFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: input.candidateElection.officeName,
      district: input.candidateElection.district,
      committeeId: resolution.committeeId,
      committeeName: resolution.committeeName,
      linkStatus: "active",
      linkSource: "tec_bulk",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });

  return {
    candidateId: input.candidateElection.candidateId,
    electionId: input.candidateElection.electionId,
    status: "linked",
    committeeId: resolution.committeeId,
  };
}

export async function autoLinkMissingTexasCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  filerRows: readonly TexasTecFilerRow[];
  sourceUrl?: string | null;
  candidateElections?: readonly TexasFinanceAutoLinkCandidateElection[];
}): Promise<TexasFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listTexasCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: TexasFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkTexasCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          filerRows: input.filerRows,
          sourceUrl: input.sourceUrl ?? null,
          now: input.now,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Texas finance auto-link failed for candidate election; continuing:", {
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
