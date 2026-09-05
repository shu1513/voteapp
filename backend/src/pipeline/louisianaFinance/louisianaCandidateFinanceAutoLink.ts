import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import type { LouisianaCampaignFinanceCsvRow } from "./louisianaCampaignFinanceArtifactReader.js";
import {
  normalizeLouisianaCandidateNameForStorage,
  normalizeLouisianaCandidateNameKeys,
  resolveLouisianaCandidateCommittee,
  type LouisianaCandidateCommitteeResolution,
} from "./louisianaCandidateCommitteeResolver.js";
import { LOUISIANA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./louisianaFinanceEligibleOffices.js";
import { upsertLouisianaFinanceLink } from "./louisianaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type LouisianaFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type LouisianaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: LouisianaCandidateCommitteeResolution["status"] | "linked";
      filerNumber?: string;
      reason?: string;
    }
  | {
      candidateId: string;
      electionId: string;
      status: "error";
      reason: "auto_link_failed";
      error: string;
    };

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function candidateNameFromRow(row: LouisianaCampaignFinanceCsvRow): string {
  const firstName = row.FilerFirstName?.trim() ?? "";
  const lastName = row.FilerLastName?.trim() ?? "";
  return [firstName, lastName].filter(Boolean).join(" ").trim();
}

export function buildLouisianaCandidateNamePredicate(
  candidates: readonly LouisianaFinanceAutoLinkCandidateElection[]
): (row: LouisianaCampaignFinanceCsvRow) => boolean {
  const candidateNameKeys = new Set<string>();
  for (const candidate of candidates) {
    for (const key of normalizeLouisianaCandidateNameKeys(candidate.candidateName)) {
      candidateNameKeys.add(key);
    }
  }

  return (row) => {
    const rowName = candidateNameFromRow(row);
    if (!rowName) {
      return false;
    }
    for (const key of normalizeLouisianaCandidateNameKeys(rowName)) {
      if (candidateNameKeys.has(key)) {
        return true;
      }
    }
    return false;
  };
}

export const listLouisianaCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "LA",
    linksTable: "la_candidate_finance_links",
    eligibleOfficeKeys: [...LOUISIANA_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkLouisianaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: LouisianaFinanceAutoLinkCandidateElection;
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
  now: Date;
}): Promise<LouisianaFinanceAutoLinkResult> {
  const resolution = resolveLouisianaCandidateCommittee({
    candidateName: input.candidateElection.candidateName,
    officeScope: input.candidateElection.officeScope,
    officeName: input.candidateElection.officeName,
    electionYear: input.candidateElection.electionYear,
    district: input.candidateElection.district,
    candidateRows: input.contributionRows,
    sourceUrl: input.sourceUrl ?? null,
  });

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertLouisianaFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeLouisianaCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: requireNonEmpty(input.candidateElection.officeName, "office name"),
      district: input.candidateElection.district,
      filerNumber: resolution.filerNumber,
      filerName: resolution.filerName,
      linkStatus: "active",
      linkSource: "la_ethics_search",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });

  return {
    candidateId: input.candidateElection.candidateId,
    electionId: input.candidateElection.electionId,
    status: "linked",
    filerNumber: resolution.filerNumber,
  };
}

export async function autoLinkMissingLouisianaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly LouisianaFinanceAutoLinkCandidateElection[];
  contributionRows: readonly LouisianaCampaignFinanceCsvRow[];
  sourceUrl?: string | null;
}): Promise<LouisianaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listLouisianaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: LouisianaFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkLouisianaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          contributionRows: input.contributionRows,
          sourceUrl: input.sourceUrl,
          now: input.now,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Louisiana finance auto-link failed for candidate election; continuing:", {
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
