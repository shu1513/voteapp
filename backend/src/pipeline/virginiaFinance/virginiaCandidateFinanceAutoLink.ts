import type { Pool, PoolClient } from "pg";

import {
  fetchVirginiaCampaignFinanceReport,
  fetchVirginiaCommitteeReportList,
  searchVirginiaCandidateCommittees,
  type VirginiaCampaignFinanceClientOptions,
  type VirginiaReportHeader,
} from "./virginiaCampaignFinanceClient.js";
import {
  normalizeVirginiaCandidateNameKeys,
  resolveVirginiaCandidateCommittee,
  type VirginiaCandidateCommitteeResolution,
} from "./virginiaCandidateCommitteeResolver.js";
import { VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./virginiaFinanceEligibleOffices.js";
import { upsertVirginiaFinanceLink } from "./virginiaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type VirginiaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

export type VirginiaFinanceAutoLinkResult =
  | {
      candidateId: string;
      electionId: string;
      status: VirginiaCandidateCommitteeResolution["status"] | "linked";
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

export type VirginiaCandidateCommitteeResolver = (
  input: {
    candidateName: string;
    officeScope: string;
    officeName: string;
    electionYear: number;
  },
  options?: VirginiaCampaignFinanceClientOptions
) => Promise<VirginiaCandidateCommitteeResolution>;

const DEFAULT_MAX_REPORT_HEADERS_PER_COMMITTEE = 3;

function normalizeCandidateNameForStorage(value: string): string {
  const keys = normalizeVirginiaCandidateNameKeys(value);
  return [...keys][0] ?? value.trim().replace(/\s+/g, " ").toUpperCase();
}

function normalizeDistrict(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function mapCandidateElectionRow(row: CandidateElectionQueryRow): VirginiaFinanceAutoLinkCandidateElection {
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

async function fetchReportHeadersForCommittee(input: {
  committeeId: string;
  options?: VirginiaCampaignFinanceClientOptions;
  maxReportHeadersPerCommittee?: number;
}): Promise<VirginiaReportHeader[]> {
  const reportList = await fetchVirginiaCommitteeReportList(input.committeeId, input.options);
  const maxReportHeadersPerCommittee = input.maxReportHeadersPerCommittee ?? DEFAULT_MAX_REPORT_HEADERS_PER_COMMITTEE;
  const reportIds = reportList.scheduledReportIds.slice(0, maxReportHeadersPerCommittee);
  const headers: VirginiaReportHeader[] = [];
  for (const reportId of reportIds) {
    const report = await fetchVirginiaCampaignFinanceReport(reportId, input.options);
    headers.push(report.header);
  }
  return headers;
}

export async function searchAndResolveVirginiaCandidateCommitteeWithReportHeaders(
  input: {
    candidateName: string;
    officeScope: string;
    officeName: string;
    electionYear: number;
    maxReportHeadersPerCommittee?: number;
  },
  options?: VirginiaCampaignFinanceClientOptions
): Promise<VirginiaCandidateCommitteeResolution> {
  const committeeResults = await searchVirginiaCandidateCommittees({ committeeName: input.candidateName }, options);
  const reportHeaders: VirginiaReportHeader[] = [];

  for (const result of committeeResults) {
    try {
      reportHeaders.push(
        ...(await fetchReportHeadersForCommittee({
          committeeId: result.committeeId,
          options,
          maxReportHeadersPerCommittee: input.maxReportHeadersPerCommittee,
        }))
      );
    } catch (error) {
      console.warn("Virginia finance committee report metadata lookup failed; continuing without this committee's headers:", {
        committeeId: result.committeeId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return resolveVirginiaCandidateCommittee({
    candidateName: input.candidateName,
    officeScope: input.officeScope,
    officeName: input.officeName,
    electionYear: input.electionYear,
    committeeResults,
    ...(reportHeaders.length > 0 ? { reportHeaders } : {}),
  });
}

export async function listVirginiaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<VirginiaFinanceAutoLinkCandidateElection[]> {
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
        AND district.state = 'VA'
        AND election.race_type = 'office'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.va_candidate_finance_links AS link
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
      [...VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return result.rows.map(mapCandidateElectionRow);
}

export async function autoLinkVirginiaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: VirginiaFinanceAutoLinkCandidateElection;
  now: Date;
  resolveCandidateCommittee?: VirginiaCandidateCommitteeResolver;
  clientOptions?: VirginiaCampaignFinanceClientOptions;
}): Promise<VirginiaFinanceAutoLinkResult> {
  const resolveCandidateCommittee =
    input.resolveCandidateCommittee ?? searchAndResolveVirginiaCandidateCommitteeWithReportHeaders;
  const resolution = await resolveCandidateCommittee(
    {
      candidateName: input.candidateElection.candidateName,
      officeScope: input.candidateElection.officeScope,
      officeName: input.candidateElection.officeName,
      electionYear: input.candidateElection.electionYear,
    },
    input.clientOptions
  );

  if (resolution.status !== "matched") {
    return {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      status: resolution.status,
      reason: resolution.reason,
    };
  }

  await upsertVirginiaFinanceLink({
    db: input.db,
    link: {
      candidateId: input.candidateElection.candidateId,
      electionId: input.candidateElection.electionId,
      electionYear: input.candidateElection.electionYear,
      candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateElection.candidateName),
      officeName: input.candidateElection.officeName,
      district: input.candidateElection.district,
      committeeId: resolution.committeeId,
      committeeCode: resolution.committeeCode,
      committeeName: resolution.committeeName,
      linkStatus: "active",
      linkSource: "cfreports_search",
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

export async function autoLinkMissingVirginiaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly VirginiaFinanceAutoLinkCandidateElection[];
  resolveCandidateCommittee?: VirginiaCandidateCommitteeResolver;
  clientOptions?: VirginiaCampaignFinanceClientOptions;
}): Promise<VirginiaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listVirginiaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));

  const results: VirginiaFinanceAutoLinkResult[] = [];
  for (const candidateElection of candidates) {
    try {
      results.push(
        await autoLinkVirginiaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection,
          now: input.now,
          resolveCandidateCommittee: input.resolveCandidateCommittee,
          clientOptions: input.clientOptions,
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn("Virginia finance auto-link failed for candidate election; continuing:", {
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
