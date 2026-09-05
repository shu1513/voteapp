import type { Pool, PoolClient } from "pg";

import {
  createStandardStateFinanceMissingLinksQuery,
  type StandardStateFinanceMissingLinkCandidateElection,
  type StandardStateFinanceMissingLinksQuery,
} from "../finance/standardStateFinanceMissingLinksQuery.js";

import {
  normalizeGeorgiaCandidateNameForStorage,
  searchAndResolveGeorgiaCandidateCommittee,
  type GeorgiaCandidateCommitteeResolution,
  type GeorgiaCandidateCommitteeSearchInput,
} from "./georgiaCandidateCommitteeResolver.js";
import type { GeorgiaEthicsTransport } from "./georgiaEthicsClient.js";
import { GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./georgiaFinanceEligibleOffices.js";
import { upsertGeorgiaFinanceLink } from "./georgiaFinanceWriter.js";

// Auto-link for Georgia candidate finance, tennessee pattern (per-candidate
// index search) with one deviation: ambiguous resolutions are reported but
// never written — the ga_candidate_finance_links status vocabulary is
// active/inactive only (migration 213), matching the fail-closed D3 rule
// that ambiguous identity goes to manual review instead of the DB.

type Queryable = Pick<Pool | PoolClient, "query">;

export type GeorgiaFinanceAutoLinkCandidateElection = StandardStateFinanceMissingLinkCandidateElection;

export type GeorgiaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: GeorgiaCandidateCommitteeResolution["status"] | "linked";
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

export type GeorgiaCandidateCommitteeResolver = (
  input: GeorgiaCandidateCommitteeSearchInput,
  transport: GeorgiaEthicsTransport
) => Promise<GeorgiaCandidateCommitteeResolution>;

function normalizeDistrict(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

export const listGeorgiaCandidateElectionsMissingFinanceLinks: StandardStateFinanceMissingLinksQuery =
  createStandardStateFinanceMissingLinksQuery({
    state: "GA",
    linksTable: "ga_candidate_finance_links",
    eligibleOfficeKeys: [...GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS],
  });

export async function autoLinkGeorgiaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: GeorgiaFinanceAutoLinkCandidateElection;
  transport: GeorgiaEthicsTransport;
  now: Date;
  resolveCandidateCommittee?: GeorgiaCandidateCommitteeResolver;
}): Promise<GeorgiaFinanceAutoLinkResult> {
  const resolveCandidateCommittee = input.resolveCandidateCommittee ?? searchAndResolveGeorgiaCandidateCommittee;
  const resolution = await resolveCandidateCommittee(
    {
      candidateName: input.candidateElection.candidateName,
      officeScope: input.candidateElection.officeScope,
      officeName: input.candidateElection.officeName,
      electionYear: input.candidateElection.electionYear,
      district: input.candidateElection.district,
    },
    input.transport
  );

  if (resolution.status === "ambiguous") {
    console.warn("Georgia finance auto-link found ambiguous PeachFile registrations; leaving for manual review:", {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      candidateName: input.candidateElection.candidateName,
      matches: resolution.matches.map((match) => ({
        filerEntityId: match.filerEntityId,
        registrationGuid: match.registrationGuid,
        committeeName: match.committeeName,
        office: match.office,
        districtName: match.districtName,
      })),
    });
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertGeorgiaFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeGeorgiaCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: input.candidateElection.officeName,
      district: input.candidateElection.district,
      committeeId: resolution.filerEntityId,
      committeeName: resolution.committeeName,
      linkStatus: "active",
      linkSource: "peachfile_api",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });

  return {
    candidateId: input.candidateElection.candidateId,
    electionId: input.candidateElection.electionId,
    status: "linked",
    committeeId: resolution.filerEntityId,
  };
}

export async function autoLinkMissingGeorgiaCandidateFinanceLinks(input: {
  db: Queryable;
  transport: GeorgiaEthicsTransport;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly GeorgiaFinanceAutoLinkCandidateElection[];
  resolveCandidateCommittee?: GeorgiaCandidateCommitteeResolver;
}): Promise<GeorgiaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listGeorgiaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: GeorgiaFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkGeorgiaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          transport: input.transport,
          now: input.now,
          resolveCandidateCommittee: input.resolveCandidateCommittee,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Georgia finance auto-link failed for candidate election; continuing:", {
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
