import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import {
  normalizeNewYorkCandidateNameKeys,
  searchAndResolveNewYorkCandidateCommittee,
  type NewYorkCandidateCommitteeResolution,
} from "./newYorkCandidateCommitteeResolver.js";
import {
  normalizeNewYorkDistrict,
  NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEYS,
} from "./newYorkFinanceEligibleOffices.js";
import { upsertNewYorkFinanceLink } from "./newYorkFinanceWriter.js";
import type { NewYorkSodaClientOptions } from "./newYorkSodaClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type NewYorkFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type NewYorkFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: NewYorkCandidateCommitteeResolution["status"] | "linked";
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

export type NewYorkCandidateCommitteeResolver = (
  input: {
    candidateName: string;
    officeScope: string;
    officeName: string;
    electionYear: number;
    district?: string | null;
  },
  options?: NewYorkSodaClientOptions
) => Promise<NewYorkCandidateCommitteeResolution>;

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeNewYorkCandidateNameKeys(value);
  return [...keys][0] ?? value.trim().replace(/\s+/g, " ").toUpperCase();
}

export const listNewYorkCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "NY",
    linksTable: "ny_candidate_finance_links",
    eligibleOfficeKeys: [...NEW_YORK_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkNewYorkCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: NewYorkFinanceAutoLinkCandidateElection;
  now: Date;
  resolveCandidateCommittee?: NewYorkCandidateCommitteeResolver;
  sodaClientOptions?: NewYorkSodaClientOptions;
}): Promise<NewYorkFinanceAutoLinkResult> {
  const resolveCandidateCommittee = input.resolveCandidateCommittee ?? searchAndResolveNewYorkCandidateCommittee;
  const resolution = await resolveCandidateCommittee(
    {
      candidateName: input.candidateElection.candidateName,
      officeScope: input.candidateElection.officeScope,
      officeName: input.candidateElection.officeName,
      electionYear: input.candidateElection.electionYear,
      district: input.candidateElection.district,
    },
    input.sodaClientOptions
  );

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertNewYorkFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: input.candidateElection.officeName,
      district: input.candidateElection.district,
      filerId: resolution.filerId,
      filerName: resolution.filerName,
      linkStatus: "active",
      linkSource: "ny_soda_api",
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

export async function autoLinkMissingNewYorkCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly NewYorkFinanceAutoLinkCandidateElection[];
  resolveCandidateCommittee?: NewYorkCandidateCommitteeResolver;
  sodaClientOptions?: NewYorkSodaClientOptions;
}): Promise<NewYorkFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listNewYorkCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: NewYorkFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkNewYorkCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          now: input.now,
          resolveCandidateCommittee: input.resolveCandidateCommittee,
          sodaClientOptions: input.sodaClientOptions,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("New York finance auto-link failed for candidate election; continuing:", {
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
