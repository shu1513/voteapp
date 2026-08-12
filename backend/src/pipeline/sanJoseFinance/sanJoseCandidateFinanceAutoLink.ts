// Phase 5 auto-link: establishes candidate → controlled-committee links from
// the parsed cycle export via the Phase 2 resolver.
//
// listSanJoseCandidateElectionsMissingFinanceLinks selects work the SF way
// (roster candidates without an active link, inside the eligibility window).
// Resolution runs PER ELECTION — the resolver's committee-shared-by-two-
// candidates check must fire within one contest, while the same committee
// legitimately links across a primary and its runoff (one committee funds
// the whole cycle).
//
// Unlike SF there is no per-election transaction: San José link rows are
// independent (no outside-relations table to keep consistent), and the
// writer itself protects manual links — a matching automatic upsert reuses
// the operator's row, a conflicting one errors into this module's per-
// candidate result instead of blocking the rest of the election.

import type { Pool, PoolClient } from "pg";
import {
  isSanJoseCityFinanceEligibleElection,
  parseSanJoseCityCouncilSeatNumber,
} from "./sanJoseFinanceEligibleOffices.js";
import {
  collectSanJoseExportCommittees,
  normalizeSanJoseTextKey,
  resolveSanJoseCandidateCommittees,
  type SanJoseAppCandidate,
  type SanJoseExportCommittee,
} from "./sanJoseCandidateCommitteeResolver.js";
import type { EfileCalWorkbook } from "../efileCalFinance/efileCalWorkbookParser.js";
import { upsertSanJoseFinanceLink } from "./sanJoseFinanceWriter.js";
import { SAN_JOSE_FINANCE_SOURCE_URL } from "./sanJoseCandidateFinanceSync.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type SanJoseFinanceAutoLinkCandidate = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: SanJoseAppCandidate["officeName"];
  /** Council district seat (1–10); null for Mayor. */
  seatNumber: number | null;
  stateFilingIds: readonly string[];
};

const SAN_JOSE_ELECTION_PREDICATE = `district.state='CA' AND district.district_type='place' AND district.geoid_compact='0668000' AND office.scope='place'`;

export async function listSanJoseCandidateElectionsMissingFinanceLinks(
  db: Queryable,
  input: {
    now: Date;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  },
): Promise<SanJoseFinanceAutoLinkCandidate[]> {
  const result = await db.query<{
    candidate_id: string;
    election_id: string;
    candidate_name: string;
    election_date: string;
    state: string;
    district_type: string;
    geoid_compact: string;
    office_scope: string;
    office_name: string;
    official_ballot_title: string | null;
    state_filing_ids: unknown;
  }>(
    `SELECT candidate.id::text candidate_id,election.id::text election_id,COALESCE(NULLIF(trim(candidate.display_name),''),NULLIF(trim(candidate.first_name||' '||candidate.last_name),'')) candidate_name,election.election_date::text election_date,district.state,district.district_type,district.geoid_compact,office.scope office_scope,office.canonical_name office_name,election.official_ballot_title,candidate.state_filing_ids FROM public.candidate_elections candidate_election JOIN public.candidates candidate ON candidate.id=candidate_election.candidate_id JOIN public.elections election ON election.id=candidate_election.election_id JOIN public.districts district ON district.id=election.district_id JOIN public.offices office ON office.id=election.office_id WHERE candidate.deleted_at IS NULL AND ${SAN_JOSE_ELECTION_PREDICATE} AND election.race_type='office' AND election.election_date>=(($1::timestamptz AT TIME ZONE 'UTC')::date-make_interval(days=>$3::int)) AND election.election_date<=(($1::timestamptz AT TIME ZONE 'UTC')::date+make_interval(days=>$4::int)) AND candidate_election.status NOT IN ('withdrawn','lost') AND NOT EXISTS (SELECT 1 FROM public.sjc_candidate_finance_links link WHERE link.candidate_id=candidate.id AND link.election_id=election.id AND link.link_status='active') ORDER BY election.election_date,candidate.display_name NULLS LAST,candidate.id LIMIT $2::int`,
    [
      input.now.toISOString(),
      input.maxCandidates,
      input.electionLookbackDays,
      input.electionLookaheadDays,
    ],
  );
  const rows: SanJoseFinanceAutoLinkCandidate[] = [];
  for (const row of result.rows) {
    // Exact eligibility in TS: the SQL predicate is district-level, this is
    // the Phase 2 office-level gate (incl. the parseable-district rule).
    if (
      !isSanJoseCityFinanceEligibleElection({
        state: row.state,
        districtType: row.district_type,
        geoidCompact: row.geoid_compact,
        officeScope: row.office_scope,
        officeCanonicalName: row.office_name,
        officialBallotTitle: row.official_ballot_title,
      })
    )
      continue;
    const officeName = row.office_name.trim() as SanJoseAppCandidate["officeName"];
    rows.push({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: Number(row.election_date.slice(0, 4)),
      officeName,
      seatNumber:
        officeName === "Mayor"
          ? null
          : parseSanJoseCityCouncilSeatNumber(row.official_ballot_title),
      stateFilingIds: Array.isArray(row.state_filing_ids)
        ? row.state_filing_ids.filter(
            (value): value is string => typeof value === "string",
          )
        : [],
    });
  }
  return rows;
}

export type SanJoseFinanceAutoLinkResult = {
  candidateId: string;
  electionId: string;
  status: "linked" | "needs_review" | "no_committee" | "error";
  reason?: string;
};

/**
 * Resolves and links every input candidate against the export's committees.
 * Matched candidates get an active efile_export link; ambiguity surfaces as
 * needs_review and no-match as no_committee — neither writes anything, so
 * the committee stays in the manual-review queue.
 */
export async function autoLinkMissingSanJoseCandidateFinanceLinks(input: {
  db: Queryable;
  now: Date;
  candidates: readonly SanJoseFinanceAutoLinkCandidate[];
  /** Concatenated cycle workbooks (loadSanJoseCycleWorkbookData). */
  workbook: EfileCalWorkbook;
  committees?: readonly SanJoseExportCommittee[];
}): Promise<SanJoseFinanceAutoLinkResult[]> {
  // Committee identity can surface on ANY sheet (the Phase 2 dry-run grouped
  // all 21,913 export rows) — a committee whose only activity is a 497 or an
  // S496 still exists and still gates its person-name siblings.
  const committees =
    input.committees ??
    collectSanJoseExportCommittees([
      ...input.workbook.summary,
      ...input.workbook.scheduleA,
      ...input.workbook.scheduleC,
      ...input.workbook.scheduleB1,
      ...input.workbook.scheduleD,
      ...input.workbook.s496,
      ...input.workbook.s497,
    ]);
  const byElection = new Map<string, SanJoseFinanceAutoLinkCandidate[]>();
  for (const candidate of input.candidates) {
    const group = byElection.get(candidate.electionId) ?? [];
    group.push(candidate);
    byElection.set(candidate.electionId, group);
  }
  const results: SanJoseFinanceAutoLinkResult[] = [];
  for (const [electionId, group] of byElection) {
    const resolutions = resolveSanJoseCandidateCommittees({
      candidates: group.map((candidate) => ({
        candidateId: candidate.candidateId,
        displayName: candidate.candidateName,
        officeName: candidate.officeName,
        seatNumber: candidate.seatNumber,
        electionYear: candidate.electionYear,
        stateFilingIds: candidate.stateFilingIds,
      })),
      committees,
    });
    for (const resolution of resolutions) {
      const candidateId = resolution.candidate.candidateId;
      if (resolution.status === "ambiguous") {
        results.push({
          candidateId,
          electionId,
          status: "needs_review",
          reason: resolution.reason,
        });
        continue;
      }
      if (resolution.status === "unmatched") {
        results.push({
          candidateId,
          electionId,
          status: "no_committee",
          reason: resolution.reason,
        });
        continue;
      }
      try {
        await upsertSanJoseFinanceLink({
          db: input.db,
          link: {
            candidateId,
            electionId,
            electionYear: resolution.candidate.electionYear,
            candidateNameNormalized: normalizeSanJoseTextKey(
              resolution.candidate.displayName,
            ),
            fppcId: resolution.filerId,
            committeeName: resolution.committeeName,
            linkStatus: "active",
            linkSource: "efile_export",
            sourceUrl: SAN_JOSE_FINANCE_SOURCE_URL,
            lastVerifiedAt: input.now,
          },
        });
        results.push({ candidateId, electionId, status: "linked" });
      } catch (error) {
        results.push({
          candidateId,
          electionId,
          status: "error",
          reason: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }
  return results;
}
