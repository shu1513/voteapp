import type { Pool, PoolClient } from "pg";

import type { StandardStateFinanceDueListInput, StandardStateFinanceDueListResult } from "../finance/standardStateFinanceDueListQuery.js";
import { MISSOURI_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./missouriFinanceEligibleOffices.js";
import type { MissouriFinanceLinkSource } from "./missouriFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type MissouriCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  electionDate: string;
  officeScope: string;
  officeName: string;
  district: string | null;
  committeeId: string;
  committeeName: string;
  linkSource: MissouriFinanceLinkSource;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

type DbRow = {
  candidate_id: string; election_id: string; candidate_name: string; election_year: number; election_date: string;
  office_scope: string; office_name: string; district: string | null; committee_id: string; committee_name: string;
  link_source: MissouriFinanceLinkSource; source_url: string | null; last_synced_at: string | null; total_due_rows: string | number;
};

export async function listDueMissouriCandidateFinanceSyncRows(
  db: Queryable,
  input: StandardStateFinanceDueListInput
): Promise<StandardStateFinanceDueListResult<MissouriCandidateFinanceDueRow>> {
  const result = await db.query<DbRow>(
    `WITH due AS (
      SELECT link.candidate_id::text candidate_id, link.election_id::text election_id,
        COALESCE(NULLIF(trim(candidate.display_name), ''), NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), ''), link.candidate_name_normalized) candidate_name,
        link.election_year, election.election_date::text election_date, office.scope office_scope, link.office_name, link.district,
        link.committee_id, link.committee_name, link.link_source, link.source_url, summary.last_synced_at::text last_synced_at,
        COUNT(*) OVER () total_due_rows
      FROM public.mo_candidate_finance_links link
      JOIN public.candidates candidate ON candidate.id=link.candidate_id
      JOIN public.candidate_elections candidate_election ON candidate_election.candidate_id=link.candidate_id AND candidate_election.election_id=link.election_id
      JOIN public.elections election ON election.id=link.election_id
      JOIN public.districts district_row ON district_row.id=election.district_id
      LEFT JOIN public.offices office ON office.id=election.office_id
      LEFT JOIN public.mo_candidate_finance_summaries summary ON summary.link_id=link.id AND summary.election_year=link.election_year
      WHERE link.link_status='active' AND candidate.deleted_at IS NULL AND district_row.state='MO' AND election.race_type='office'
        AND election.election_date >= (($1::timestamptz AT TIME ZONE 'UTC')::date - make_interval(days=>$4::int))
        AND election.election_date <= (($1::timestamptz AT TIME ZONE 'UTC')::date + make_interval(days=>$5::int))
        AND candidate_election.status NOT IN ('withdrawn','lost')
        AND (office.scope || '::' || office.canonical_name)=ANY($6::text[])
        AND (summary.last_synced_at IS NULL OR summary.last_synced_at < ($1::timestamptz - make_interval(days=>$2::int)))
      ORDER BY summary.last_synced_at NULLS FIRST, election.election_date, link.candidate_name_normalized, link.id
      LIMIT $3::int)
    SELECT * FROM due`,
    [input.now.toISOString(), input.staleAfterDays, input.maxCandidates, input.electionLookbackDays, input.electionLookaheadDays, [...MISSOURI_FINANCE_ELIGIBLE_OFFICE_KEYS]]
  );
  const rawTotalDueRows = result.rows[0]?.total_due_rows;
  const parsedTotalDueRows = typeof rawTotalDueRows === "number" ? rawTotalDueRows : Number(rawTotalDueRows);
  return {
    rows: result.rows.map((row) => ({
      candidateId: row.candidate_id, electionId: row.election_id, candidateName: row.candidate_name,
      electionYear: row.election_year, electionDate: row.election_date.slice(0, 10), officeScope: row.office_scope,
      officeName: row.office_name, district: row.district, committeeId: row.committee_id, committeeName: row.committee_name,
      linkSource: row.link_source, sourceUrl: row.source_url, lastSyncedAt: row.last_synced_at,
    })),
    totalDueRows: Number.isSafeInteger(parsedTotalDueRows) && parsedTotalDueRows >= 0 ? parsedTotalDueRows : 0,
  };
}
