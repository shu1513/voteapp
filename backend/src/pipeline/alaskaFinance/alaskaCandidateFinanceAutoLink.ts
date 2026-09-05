import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import {
  normalizeAlaskaCandidateNameForStorage,
  resolveAlaskaCandidateCommittee,
  type AlaskaCandidateCommitteeResolution,
} from "./alaskaCandidateCommitteeResolver.js";
import { ALASKA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./alaskaFinanceEligibleOffices.js";
import { upsertAlaskaFinanceLink } from "./alaskaFinanceWriter.js";
import type { AlaskaApocCampaignIncomeRow } from "./alaskaApocClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type AlaskaFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type AlaskaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: AlaskaCandidateCommitteeResolution["status"] | "linked";
      candidateFilerId?: string;
      reason?: string;
    }
  | {
      candidateId: string;
      electionId: string;
      status: "error";
      reason: "auto_link_failed";
      error: string;
    };

export const listAlaskaCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "AK",
    linksTable: "ak_candidate_finance_links",
    eligibleOfficeKeys: [...ALASKA_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkAlaskaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: AlaskaFinanceAutoLinkCandidateElection;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
  sourceUrl: string | null;
  now: Date;
}): Promise<AlaskaFinanceAutoLinkResult> {
  const resolution = resolveAlaskaCandidateCommittee({
    candidateName: input.candidateElection.candidateName,
    electionYear: input.candidateElection.electionYear,
    incomeRows: input.incomeRows,
    officeName: input.candidateElection.officeName,
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

  await upsertAlaskaFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeAlaskaCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: input.candidateElection.officeName,
      district: input.candidateElection.district,
      candidateFilerId: resolution.candidateFilerId,
      candidateFilerName: resolution.candidateFilerName,
      linkStatus: "active",
      linkSource: "apoc_csv",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });

  return {
    candidateId: input.candidateElection.candidateId,
    electionId: input.candidateElection.electionId,
    status: "linked",
    candidateFilerId: resolution.candidateFilerId,
  };
}

export async function autoLinkMissingAlaskaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  incomeRows: readonly AlaskaApocCampaignIncomeRow[];
  sourceUrl: string | null;
  candidateElections?: readonly AlaskaFinanceAutoLinkCandidateElection[];
}): Promise<AlaskaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listAlaskaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: AlaskaFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkAlaskaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          incomeRows: input.incomeRows,
          sourceUrl: input.sourceUrl,
          now: input.now,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Alaska finance auto-link failed for candidate election; continuing:", {
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
