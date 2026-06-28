import type { Pool, PoolClient } from "pg";

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

export type LouisianaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

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

type CandidateElectionQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function mapCandidateElectionRow(row: CandidateElectionQueryRow): LouisianaFinanceAutoLinkCandidateElection {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
  };
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

export async function listLouisianaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<LouisianaFinanceAutoLinkCandidateElection[]> {
  const result = await db.query<CandidateElectionQueryRow>(
    `
      SELECT
        candidate.id::text AS candidate_id,
        election.id::text AS election_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')
        ) AS candidate_name,
        extract(year from election.election_date)::int AS election_year,
        office.scope AS office_scope,
        COALESCE(NULLIF(trim(office.canonical_name), ''), election.official_ballot_title) AS office_name,
        CASE
          WHEN district.district_type IN ('state_upper', 'state_lower') THEN
            NULLIF(
              regexp_replace(
                substring(district.geoid_compact from char_length(district.state_fips) + 1),
                '^0+',
                ''
              ),
              ''
            )
          ELSE NULL
        END AS district
      FROM public.candidate_elections AS candidate_election
      JOIN public.candidates AS candidate
        ON candidate.id = candidate_election.candidate_id
      JOIN public.elections AS election
        ON election.id = candidate_election.election_id
      JOIN public.districts AS district
        ON district.id = election.district_id
      LEFT JOIN public.offices AS office
        ON office.id = election.office_id
      WHERE candidate.deleted_at IS NULL
        AND district.state = 'LA'
        AND election.race_type = 'office'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.la_candidate_finance_links AS link
          WHERE link.candidate_id = candidate.id
            AND link.election_id = election.id
            AND link.link_status = 'active'
        )
      ORDER BY election.election_date ASC, candidate.display_name ASC NULLS LAST, candidate.id ASC
      LIMIT $2::int
    `,
    [
      input.now.toISOString(),
      input.maxCandidates,
      input.electionLookbackDays,
      input.electionLookaheadDays,
      [...LOUISIANA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return result.rows.map(mapCandidateElectionRow);
}

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
