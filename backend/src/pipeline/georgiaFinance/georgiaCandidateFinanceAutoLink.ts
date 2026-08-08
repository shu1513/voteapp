import type { Pool, PoolClient } from "pg";

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

export type GeorgiaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

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

type CandidateElectionQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
};

export type GeorgiaCandidateCommitteeResolver = (
  input: GeorgiaCandidateCommitteeSearchInput,
  transport: GeorgiaEthicsTransport
) => Promise<GeorgiaCandidateCommitteeResolution>;

function normalizeDistrict(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function mapCandidateElectionRow(row: CandidateElectionQueryRow): GeorgiaFinanceAutoLinkCandidateElection {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: normalizeDistrict(row.district),
  };
}

export async function listGeorgiaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<GeorgiaFinanceAutoLinkCandidateElection[]> {
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
        AND district.state = 'GA'
        AND election.race_type = 'office'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.ga_candidate_finance_links AS link
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
      [...GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return result.rows.map(mapCandidateElectionRow);
}

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
