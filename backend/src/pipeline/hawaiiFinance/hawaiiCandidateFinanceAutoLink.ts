import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import {
  normalizeHawaiiCandidateNameKeys,
  searchAndResolveHawaiiCandidateCommittee,
  type HawaiiCandidateCommitteeResolution,
} from "./hawaiiCandidateCommitteeResolver.js";
import {
  HAWAII_FINANCE_ELIGIBLE_OFFICE_KEYS,
  normalizeHawaiiCscDistrict,
} from "./hawaiiFinanceEligibleOffices.js";
import { upsertHawaiiFinanceLink } from "./hawaiiFinanceWriter.js";
import type { HawaiiCscClientOptions } from "./hawaiiCscClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type HawaiiFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type HawaiiFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: HawaiiCandidateCommitteeResolution["status"] | "linked";
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

export type HawaiiCandidateCommitteeResolver = (
  input: {
    candidateName: string;
    officeScope: string;
    officeName: string;
    electionYear: number;
    district?: string | null;
  },
  options?: HawaiiCscClientOptions
) => Promise<HawaiiCandidateCommitteeResolution>;

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeHawaiiCandidateNameKeys(value);
  return [...keys][0] ?? value.trim().replace(/\s+/g, " ").toUpperCase();
}

export const listHawaiiCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "HI",
    linksTable: "hi_candidate_finance_links",
    eligibleOfficeKeys: [...HAWAII_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkHawaiiCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: HawaiiFinanceAutoLinkCandidateElection;
  now: Date;
  resolveCandidateCommittee?: HawaiiCandidateCommitteeResolver;
  cscClientOptions?: HawaiiCscClientOptions;
}): Promise<HawaiiFinanceAutoLinkResult> {
  const resolveCandidateCommittee = input.resolveCandidateCommittee ?? searchAndResolveHawaiiCandidateCommittee;
  const resolution = await resolveCandidateCommittee(
    {
      candidateName: input.candidateElection.candidateName,
      officeScope: input.candidateElection.officeScope,
      officeName: input.candidateElection.officeName,
      electionYear: input.candidateElection.electionYear,
      district: input.candidateElection.district,
    },
    input.cscClientOptions
  );

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertHawaiiFinanceLink({
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
      electionPeriod: resolution.electionPeriod,
      linkStatus: "active",
      linkSource: "csc_api",
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

export async function autoLinkMissingHawaiiCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly HawaiiFinanceAutoLinkCandidateElection[];
  resolveCandidateCommittee?: HawaiiCandidateCommitteeResolver;
  cscClientOptions?: HawaiiCscClientOptions;
}): Promise<HawaiiFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listHawaiiCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: HawaiiFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkHawaiiCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          now: input.now,
          resolveCandidateCommittee: input.resolveCandidateCommittee,
          cscClientOptions: input.cscClientOptions,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Hawaii finance auto-link failed for candidate election; continuing:", {
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
