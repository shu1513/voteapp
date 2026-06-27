import type { Pool, PoolClient } from "pg";

import {
  normalizeMinnesotaCandidateNameKeys,
  resolveMinnesotaCandidateCommittee,
  type MinnesotaCandidateCommitteeResolution,
} from "./minnesotaCandidateCommitteeResolver.js";
import { MINNESOTA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./minnesotaFinanceEligibleOffices.js";
import { upsertMinnesotaFinanceLink } from "./minnesotaFinanceWriter.js";
import type { MinnesotaCampaignFinanceCsvRow } from "./minnesotaCampaignFinanceArtifactReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type MinnesotaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

export type MinnesotaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: MinnesotaCandidateCommitteeResolution["status"] | "linked";
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
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeMinnesotaCandidateNameKeys(value);
  return [...keys][0] ?? value.trim().replace(/\s+/g, " ").toUpperCase();
}

function mapCandidateElectionRow(row: CandidateElectionQueryRow): MinnesotaFinanceAutoLinkCandidateElection {
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

function candidateNameFromContributionRow(row: MinnesotaCampaignFinanceCsvRow): string {
  const candidates = [
    row["Candidate"],
    row["Candidate Name"],
    row["candidate"],
    row["candidate_name"],
    row["Recipient"],
  ];
  for (const value of candidates) {
    const trimmed = value?.trim() ?? "";
    if (trimmed.length > 0) {
      return trimmed;
    }
  }
  return "";
}

export function buildMinnesotaCandidateNamePredicate(
  candidates: readonly MinnesotaFinanceAutoLinkCandidateElection[]
): (row: MinnesotaCampaignFinanceCsvRow) => boolean {
  const candidateNameKeys = new Set<string>();
  for (const candidate of candidates) {
    for (const key of normalizeMinnesotaCandidateNameKeys(candidate.candidateName)) {
      candidateNameKeys.add(key);
    }
  }

  return (row) => {
    const rawName = candidateNameFromContributionRow(row);
    if (!rawName) {
      return false;
    }
    for (const rowKey of normalizeMinnesotaCandidateNameKeys(rawName)) {
      if (candidateNameKeys.has(rowKey)) {
        return true;
      }
    }
    return false;
  };
}

export async function listMinnesotaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<MinnesotaFinanceAutoLinkCandidateElection[]> {
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
        AND district.state = 'MN'
        AND election.race_type = 'office'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
      AND candidate_election.status NOT IN ('withdrawn', 'lost')
      AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.mn_candidate_finance_links AS link
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
      [...MINNESOTA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return result.rows.map(mapCandidateElectionRow);
}

export async function autoLinkMinnesotaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: MinnesotaFinanceAutoLinkCandidateElection;
  contributionRows: readonly MinnesotaCampaignFinanceCsvRow[];
  now: Date;
}): Promise<MinnesotaFinanceAutoLinkResult> {
  const resolution = resolveMinnesotaCandidateCommittee({
    candidateName: input.candidateElection.candidateName,
    officeScope: input.candidateElection.officeScope,
    officeName: input.candidateElection.officeName,
    electionYear: input.candidateElection.electionYear,
    district: input.candidateElection.district,
    candidateRows: input.contributionRows,
    sourceUrl: null,
  });

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertMinnesotaFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: requireNonEmpty(input.candidateElection.officeName, "office name"),
      district: input.candidateElection.district,
      committeeId: resolution.committeeId,
      committeeName: resolution.committeeName,
      linkStatus: "active",
      linkSource: "mn_board",
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

export async function autoLinkMissingMinnesotaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly MinnesotaFinanceAutoLinkCandidateElection[];
  contributionRows: readonly MinnesotaCampaignFinanceCsvRow[];
}): Promise<MinnesotaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listMinnesotaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: MinnesotaFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkMinnesotaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          contributionRows: input.contributionRows,
          now: input.now,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Minnesota finance auto-link failed for candidate election; continuing:", {
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
