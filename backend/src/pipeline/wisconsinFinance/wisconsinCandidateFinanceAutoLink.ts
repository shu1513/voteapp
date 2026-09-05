import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import {
  normalizeWisconsinCandidateNameForStorage,
  searchAndResolveWisconsinCandidateCommittee,
  type WisconsinCandidateCommitteeResolution,
} from "./wisconsinCandidateCommitteeResolver.js";
import {
  normalizeWisconsinSunshineLegislativeDistrict,
  WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEYS,
} from "./wisconsinFinanceEligibleOffices.js";
import { upsertWisconsinFinanceLink } from "./wisconsinFinanceWriter.js";
import type { WisconsinSunshineClientOptions } from "./wisconsinSunshineClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type WisconsinFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type WisconsinFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: WisconsinCandidateCommitteeResolution["status"] | "linked";
      entityId?: string;
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

export type WisconsinCandidateCommitteeResolver = (
  input: {
    candidateName: string;
    officeScope: string;
    officeName: string;
    electionYear: number;
    district?: string | null;
  },
  options?: WisconsinSunshineClientOptions
) => Promise<WisconsinCandidateCommitteeResolution>;

export const listWisconsinCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "WI",
    linksTable: "wi_candidate_finance_links",
    eligibleOfficeKeys: [...WISCONSIN_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkWisconsinCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: WisconsinFinanceAutoLinkCandidateElection;
  now: Date;
  resolveCandidateCommittee?: WisconsinCandidateCommitteeResolver;
  sunshineClientOptions?: WisconsinSunshineClientOptions;
}): Promise<WisconsinFinanceAutoLinkResult> {
  const resolveCandidateCommittee = input.resolveCandidateCommittee ?? searchAndResolveWisconsinCandidateCommittee;
  const resolution = await resolveCandidateCommittee(
    {
      candidateName: input.candidateElection.candidateName,
      officeScope: input.candidateElection.officeScope,
      officeName: input.candidateElection.officeName,
      electionYear: input.candidateElection.electionYear,
      district: input.candidateElection.district,
    },
    input.sunshineClientOptions
  );

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertWisconsinFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeWisconsinCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: input.candidateElection.officeName,
      district: input.candidateElection.district,
      entityId: resolution.entityId,
      committeeId: resolution.committeeId,
      committeeName: resolution.committeeName,
      assignedCommitteeId: resolution.assignedCommitteeId ?? null,
      linkStatus: "active",
      linkSource: "sunshine_api",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });

  return {
    candidateId: input.candidateElection.candidateId,
    electionId: input.candidateElection.electionId,
    status: "linked",
    entityId: resolution.entityId,
    committeeId: resolution.committeeId,
  };
}

export async function autoLinkMissingWisconsinCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly WisconsinFinanceAutoLinkCandidateElection[];
  resolveCandidateCommittee?: WisconsinCandidateCommitteeResolver;
  sunshineClientOptions?: WisconsinSunshineClientOptions;
}): Promise<WisconsinFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listWisconsinCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: WisconsinFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkWisconsinCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          now: input.now,
          resolveCandidateCommittee: input.resolveCandidateCommittee,
          sunshineClientOptions: input.sunshineClientOptions,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Wisconsin finance auto-link failed for candidate election; continuing:", {
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
