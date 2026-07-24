import type { Pool, PoolClient } from "pg";

import {
  canonicalOklahomaCandidateOfficeName,
  normalizeOklahomaCandidateDistrict,
  normalizeOklahomaCandidateNameKeys,
  resolveOklahomaCandidateCommittee,
  type OklahomaCandidateCommitteeResolution,
} from "./oklahomaCandidateCommitteeResolver.js";
import { OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./oklahomaFinanceEligibleOffices.js";
import { upsertOklahomaFinanceLink } from "./oklahomaFinanceWriter.js";
import {
  fetchOklahomaGuardianCandidateDetail,
  type OklahomaGuardianCandidateDetail,
} from "./oklahomaGuardianCandidateDetail.js";
import type { OklahomaGuardianContributionRow } from "./oklahomaGuardianContributionReader.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type OklahomaGuardianCandidateDetailFetcher = (input: {
  organizationId: string;
}) => Promise<OklahomaGuardianCandidateDetail>;

export type OklahomaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

export type OklahomaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: OklahomaCandidateCommitteeResolution["status"] | "linked";
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

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeOklahomaCandidateNameKeys(value);
  return [...keys][0] ?? value.trim().replace(/\s+/g, " ").toUpperCase();
}

function mapCandidateElectionRow(row: CandidateElectionQueryRow): OklahomaFinanceAutoLinkCandidateElection {
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

function candidateNameKeysIntersect(left: string, right: string): boolean {
  const rightKeys = normalizeOklahomaCandidateNameKeys(right);
  for (const leftKey of normalizeOklahomaCandidateNameKeys(left)) {
    if (rightKeys.has(leftKey)) {
      return true;
    }
  }
  return false;
}

function guardianDetailMatchesCandidateElection(input: {
  detail: OklahomaGuardianCandidateDetail;
  candidateElection: OklahomaFinanceAutoLinkCandidateElection;
}): boolean {
  if (!candidateNameKeysIntersect(input.detail.candidateName, input.candidateElection.candidateName)) {
    return false;
  }
  const expectedOffice = canonicalOklahomaCandidateOfficeName(input.candidateElection.officeName);
  const detailOffice = canonicalOklahomaCandidateOfficeName(input.detail.officeName);
  if (!expectedOffice || detailOffice !== expectedOffice) {
    return false;
  }
  if (!input.detail.electionYears.includes(input.candidateElection.electionYear)) {
    return false;
  }
  if (input.candidateElection.officeScope === "state_upper" || input.candidateElection.officeScope === "state_lower") {
    const expectedDistrict = normalizeOklahomaCandidateDistrict(input.candidateElection.district);
    const detailDistrict = normalizeOklahomaCandidateDistrict(input.detail.district);
    return Boolean(expectedDistrict) && detailDistrict === expectedDistrict;
  }
  return true;
}

async function disambiguateOklahomaCandidateCommittee(input: {
  resolution: Extract<OklahomaCandidateCommitteeResolution, { status: "ambiguous" }>;
  candidateElection: OklahomaFinanceAutoLinkCandidateElection;
  fetchCandidateDetail: OklahomaGuardianCandidateDetailFetcher;
}): Promise<OklahomaCandidateCommitteeResolution> {
  let details: OklahomaGuardianCandidateDetail[];
  try {
    details = await Promise.all(
      input.resolution.matches.map((match) => input.fetchCandidateDetail({ organizationId: match.committeeId }))
    );
  } catch (error) {
    console.warn("Oklahoma Guardian candidate-detail lookup failed; preserving ambiguous committee resolution:", {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      committeeIds: input.resolution.matches.map((match) => match.committeeId),
      error: error instanceof Error ? error.message : String(error),
    });
    return input.resolution;
  }

  const matchingIds = new Set(
    details
      .filter((detail) => guardianDetailMatchesCandidateElection({ detail, candidateElection: input.candidateElection }))
      .map((detail) => detail.organizationId)
  );
  const matches = input.resolution.matches.filter((match) => matchingIds.has(match.committeeId));
  if (matches.length !== 1) {
    return input.resolution;
  }

  return { status: "matched", ...matches[0]! };
}

export function buildOklahomaCandidateNamePredicate(
  candidates: readonly OklahomaFinanceAutoLinkCandidateElection[]
): (row: OklahomaGuardianContributionRow) => boolean {
  const candidateNameKeys = new Set<string>();
  for (const candidate of candidates) {
    for (const key of normalizeOklahomaCandidateNameKeys(candidate.candidateName)) {
      candidateNameKeys.add(key);
    }
  }

  return (row) => {
    for (const rowKey of normalizeOklahomaCandidateNameKeys(row["Candidate Name"])) {
      if (candidateNameKeys.has(rowKey)) {
        return true;
      }
    }
    return false;
  };
}

export async function listOklahomaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<OklahomaFinanceAutoLinkCandidateElection[]> {
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
        AND district.state = 'OK'
        AND election.race_type = 'office'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.ok_candidate_finance_links AS link
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
      [...OKLAHOMA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return result.rows.map(mapCandidateElectionRow);
}

export async function autoLinkOklahomaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: OklahomaFinanceAutoLinkCandidateElection;
  contributionRows: readonly OklahomaGuardianContributionRow[];
  sourceUrl: string | null;
  now: Date;
  fetchCandidateDetail?: OklahomaGuardianCandidateDetailFetcher;
}): Promise<OklahomaFinanceAutoLinkResult> {
  let resolution = resolveOklahomaCandidateCommittee({
    candidateName: input.candidateElection.candidateName,
    officeScope: input.candidateElection.officeScope,
    officeName: input.candidateElection.officeName,
    electionYear: input.candidateElection.electionYear,
    district: input.candidateElection.district,
    contributionRows: input.contributionRows,
    sourceUrl: input.sourceUrl,
  });

  if (resolution.status === "ambiguous") {
    resolution = await disambiguateOklahomaCandidateCommittee({
      resolution,
      candidateElection: input.candidateElection,
      fetchCandidateDetail: input.fetchCandidateDetail ?? fetchOklahomaGuardianCandidateDetail,
    });
  }

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertOklahomaFinanceLink({
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
      linkStatus: "active",
      linkSource: "guardian_bulk",
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

export async function autoLinkMissingOklahomaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  contributionRowsByYear: ReadonlyMap<number, readonly OklahomaGuardianContributionRow[]>;
  sourceUrlByYear?: ReadonlyMap<number, string>;
  candidateElections?: readonly OklahomaFinanceAutoLinkCandidateElection[];
  fetchCandidateDetail?: OklahomaGuardianCandidateDetailFetcher;
}): Promise<OklahomaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listOklahomaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: OklahomaFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkOklahomaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          contributionRows: input.contributionRowsByYear.get(candidateElection.electionYear) ?? [],
          sourceUrl: input.sourceUrlByYear?.get(candidateElection.electionYear) ?? null,
          now: input.now,
          fetchCandidateDetail: input.fetchCandidateDetail,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Oklahoma finance auto-link failed for candidate election; continuing:", {
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
