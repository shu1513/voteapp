// South Carolina finance auto-link: creates missing candidate -> filer links
// (links only, never summaries). Mirrors the Missouri/Delaware shape: list
// candidate elections in eligible offices with no active link, resolve each
// through the filer-name search + exact-surname filter + cycle-evidence
// resolver, and write only full-name matches with linkSource
// "ethics_filer_search" — the writer's manual-link protection guarantees
// operator links always win. Surname-only matches (the Alan Wilson
// legal-name divergence) and ambiguity are reported, never linked.

import type { Pool, PoolClient } from "pg";

import {
  getSouthCarolinaCandidateReports,
  searchSouthCarolinaFilersByName,
  SouthCarolinaEthicsClientError,
  type SouthCarolinaEthicsClientOptions,
  type SouthCarolinaFilerSearchRow,
} from "./southCarolinaEthicsClient.js";
import {
  filterSouthCarolinaFilersByExactSurname,
  resolveSouthCarolinaCandidateFiler,
  southCarolinaFilerSearchTerm,
  type SouthCarolinaCandidateFilerMatch,
  type SouthCarolinaFilerReportSet,
} from "./southCarolinaCandidateFilerResolver.js";
import { SOUTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./southCarolinaFinanceEligibleOffices.js";
import { upsertSouthCarolinaFinanceLink } from "./southCarolinaFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type SouthCarolinaFinanceAutoLinkCandidateElection = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionDate: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
};

export type SouthCarolinaFinanceAutoLinkResult = {
  candidateId: string;
  electionId: string;
  status: "linked" | "manual_confirm_required" | "ambiguous" | "unmatched" | "error";
  reason?: string;
  candidateFilerId?: number;
  filerName?: string;
  /** Filers whose report fetch failed (stale ids 500 live) — skipped, visible. */
  skippedFilerIds?: number[];
  candidates?: SouthCarolinaCandidateFilerMatch[];
  error?: string;
};

type CandidateElectionQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_date: string | Date;
  election_year: number;
  office_scope: string;
  office_name: string;
  district_name: string | null;
};

// Stored-name normalization (the Delaware convention): diacritics stripped,
// uppercased, non-alphanumerics collapsed to single spaces.
export function normalizeSouthCarolinaCandidateNameForStorage(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toIsoDate(value: string | Date): string {
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  const match = /^(\d{4}-\d{2}-\d{2})/.exec(value);
  if (match?.[1]) {
    return match[1];
  }
  throw new Error(`Invalid South Carolina candidate election date from database: ${value}`);
}

export async function listSouthCarolinaCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: { now: Date; maxCandidates: number; electionLookbackDays: number; electionLookaheadDays: number }
): Promise<SouthCarolinaFinanceAutoLinkCandidateElection[]> {
  const result = await db.query<CandidateElectionQueryRow>(
    `
      SELECT
        candidate.id::text AS candidate_id,
        election.id::text AS election_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')
        ) AS candidate_name,
        election.election_date,
        extract(year from election.election_date)::int AS election_year,
        office.scope AS office_scope,
        office.canonical_name AS office_name,
        district.name AS district_name
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
        AND district.state = 'SC'
        AND election.race_type = 'office'
        AND election.election_stage = 'general'
        AND election.election_date >= ($1::date - make_interval(days => $3::int))
        AND election.election_date <= ($1::date + make_interval(days => $4::int))
        AND candidate_election.status NOT IN ('withdrawn', 'lost')
        AND (office.scope || '::' || office.canonical_name) = ANY($5::text[])
        AND COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')) IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.sc_candidate_finance_links AS link
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
      [...SOUTH_CAROLINA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );
  return result.rows.map((row) => ({
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionDate: toIsoDate(row.election_date),
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district_name,
  }));
}

// Common surnames return 100+ filers. A filer whose newest campaign
// disclosure predates the prior calendar year cannot carry cycle evidence,
// so its report fetch is skipped; a NULL lastCampaignDisclosureReport is
// KEPT — registered-not-yet-filed filers carry placeholder cycle rows that
// are exactly the evidence the resolver needs.
export function southCarolinaFilerNeedsReportFetch(
  filer: SouthCarolinaFilerSearchRow,
  electionYear: number
): boolean {
  const last = filer.lastCampaignDisclosureReport?.trim();
  if (!last) {
    return true;
  }
  const yearText = last.slice(-4);
  const year = Number.parseInt(yearText, 10);
  if (!/^\d{4}$/.test(yearText) || !Number.isSafeInteger(year)) {
    return true;
  }
  return year >= electionYear - 1;
}

export type SouthCarolinaCandidateFilerReportSetLoader = (input: {
  candidateName: string;
  electionYear: number;
  clientOptions?: SouthCarolinaEthicsClientOptions;
}) => Promise<{ filerReportSets: SouthCarolinaFilerReportSet[]; skippedFilerIds: number[] }>;

export const loadSouthCarolinaFilerReportSets: SouthCarolinaCandidateFilerReportSetLoader = async (
  input
) => {
  const term = southCarolinaFilerSearchTerm(input.candidateName);
  if (term === null) {
    return { filerReportSets: [], skippedFilerIds: [] };
  }
  const searchRows = await searchSouthCarolinaFilersByName(term, input.clientOptions);
  const filers = filterSouthCarolinaFilersByExactSurname(input.candidateName, searchRows);
  const filerReportSets: SouthCarolinaFilerReportSet[] = [];
  const skippedFilerIds: number[] = [];
  for (const filer of filers) {
    if (!southCarolinaFilerNeedsReportFetch(filer, input.electionYear)) {
      continue;
    }
    try {
      const reports = await getSouthCarolinaCandidateReports(filer.candidateFilerId, input.clientOptions);
      filerReportSets.push({ filer, reports });
    } catch (error) {
      // The reports endpoint 500s for some stale filer ids (live-proven).
      // Skip that filer visibly rather than failing the whole candidate.
      if (error instanceof SouthCarolinaEthicsClientError && error.code === "http_error") {
        skippedFilerIds.push(filer.candidateFilerId);
        continue;
      }
      throw error;
    }
  }
  return { filerReportSets, skippedFilerIds };
};

export async function autoLinkSouthCarolinaCandidateFinanceForCandidateElection(input: {
  db: Queryable;
  candidateElection: SouthCarolinaFinanceAutoLinkCandidateElection;
  now: Date;
  loadFilerReportSets?: SouthCarolinaCandidateFilerReportSetLoader;
  clientOptions?: SouthCarolinaEthicsClientOptions;
}): Promise<SouthCarolinaFinanceAutoLinkResult> {
  const candidate = input.candidateElection;
  const load = input.loadFilerReportSets ?? loadSouthCarolinaFilerReportSets;
  const { filerReportSets, skippedFilerIds } = await load({
    candidateName: candidate.candidateName,
    electionYear: candidate.electionYear,
    clientOptions: input.clientOptions,
  });
  const skipped = skippedFilerIds.length > 0 ? { skippedFilerIds } : {};
  const resolution = resolveSouthCarolinaCandidateFiler({
    candidateName: candidate.candidateName,
    electionDate: candidate.electionDate,
    filerReportSets,
  });
  if (resolution.status === "unmatched") {
    return {
      candidateId: candidate.candidateId,
      electionId: candidate.electionId,
      status: "unmatched",
      reason: resolution.reason,
      ...skipped,
    };
  }
  if (resolution.status === "manual_confirm_required") {
    return {
      candidateId: candidate.candidateId,
      electionId: candidate.electionId,
      status: "manual_confirm_required",
      candidates: resolution.candidates,
      ...skipped,
    };
  }
  if (resolution.status === "ambiguous") {
    return {
      candidateId: candidate.candidateId,
      electionId: candidate.electionId,
      status: "ambiguous",
      reason: resolution.reason,
      candidates: resolution.matches,
      ...skipped,
    };
  }
  await upsertSouthCarolinaFinanceLink({
    db: input.db,
    link: {
      candidateId: candidate.candidateId,
      electionId: candidate.electionId,
      electionYear: candidate.electionYear,
      candidateNameNormalized: normalizeSouthCarolinaCandidateNameForStorage(candidate.candidateName),
      officeName: candidate.officeName,
      district: candidate.district,
      candidateFilerId: resolution.candidateFilerId,
      filerName: resolution.filerName,
      linkStatus: "active",
      linkSource: "ethics_filer_search",
      sourceUrl: resolution.sourceUrl,
      lastVerifiedAt: input.now,
    },
  });
  return {
    candidateId: candidate.candidateId,
    electionId: candidate.electionId,
    status: "linked",
    candidateFilerId: resolution.candidateFilerId,
    filerName: resolution.filerName,
    ...skipped,
  };
}

export async function autoLinkMissingSouthCarolinaCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
  candidateElections?: readonly SouthCarolinaFinanceAutoLinkCandidateElection[];
  loadFilerReportSets?: SouthCarolinaCandidateFilerReportSetLoader;
  clientOptions?: SouthCarolinaEthicsClientOptions;
}): Promise<SouthCarolinaFinanceAutoLinkResult[]> {
  const candidates =
    input.candidateElections ??
    (await listSouthCarolinaCandidateElectionsMissingFinanceLinks(input.db, {
      now: input.now,
      maxCandidates: input.maxCandidates,
      electionLookbackDays: input.electionLookbackDays,
      electionLookaheadDays: input.electionLookaheadDays,
    }));
  const results: SouthCarolinaFinanceAutoLinkResult[] = [];
  for (const candidate of candidates) {
    try {
      results.push(
        await autoLinkSouthCarolinaCandidateFinanceForCandidateElection({
          db: input.db,
          candidateElection: candidate,
          now: input.now,
          loadFilerReportSets: input.loadFilerReportSets,
          clientOptions: input.clientOptions,
        })
      );
    } catch (error) {
      results.push({
        candidateId: candidate.candidateId,
        electionId: candidate.electionId,
        status: "error",
        reason: "auto_link_failed",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return results;
}
