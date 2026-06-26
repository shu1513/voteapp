import type { Pool, PoolClient } from "pg";

import {
  normalizeAlaskaCandidateNameForStorage,
  resolveAlaskaCandidateCommittee,
  type AlaskaCandidateCommitteeResolution,
} from "./alaskaCandidateCommitteeResolver.js";
import { ALASKA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./alaskaFinanceEligibleOffices.js";
import { upsertAlaskaFinanceLink } from "./alaskaFinanceWriter.js";
import type { AlaskaApocCampaignIncomeRow } from "./alaskaApocClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type AlaskaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

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

type CandidateElectionQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
};

function mapCandidateElectionRow(row: CandidateElectionQueryRow): AlaskaFinanceAutoLinkCandidateElection {
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

export async function listAlaskaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<AlaskaFinanceAutoLinkCandidateElection[]> {
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
        AND district.state = 'AK'
        AND election.race_type = 'office'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.ak_candidate_finance_links AS link
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
      [...ALASKA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return result.rows.map(mapCandidateElectionRow);
}

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
