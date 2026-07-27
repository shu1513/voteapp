import type { Pool, PoolClient } from "pg";

import {
  syncNewJerseyCandidateFinanceFromElec,
  type NewJerseyCandidateFinanceElecSyncResult,
} from "./newJerseyCandidateFinanceSync.js";
import { NEW_JERSEY_FINANCE_ELIGIBLE_OFFICE_KEYS } from "./newJerseyFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

export type NewJerseyCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district: string | null;
  sourceUrl: string | null;
  lastSyncedAt: string | null;
};

export type NewJerseyCandidateFinanceBatchSyncInput = {
  db: ConnectableQueryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  syncNewJerseyCandidateFinanceFromElecFn?: typeof syncNewJerseyCandidateFinanceFromElec;
};

export type NewJerseyCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  ok: boolean;
  status?: NewJerseyCandidateFinanceElecSyncResult["status"];
  candidateEntityS?: number;
  result?: NewJerseyCandidateFinanceElecSyncResult;
  error?: string;
};

export type NewJerseyCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  skippedCandidateCount: number;
  failedCandidateCount: number;
  results: NewJerseyCandidateFinanceBatchSyncItemResult[];
};

type NewJerseyCandidateFinanceDueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_name: string;
  district: string | null;
  source_url: string | null;
  last_synced_at: string | null;
  total_due_rows: string | number;
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS = 1;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 730;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid New Jersey finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Jersey finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function parseTotalDueRows(value: string | number | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function mapDueRow(row: NewJerseyCandidateFinanceDueQueryRow): NewJerseyCandidateFinanceDueRow {
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
    candidateName: row.candidate_name,
    electionYear: row.election_year,
    officeScope: row.office_scope,
    officeName: row.office_name,
    district: row.district,
    sourceUrl: row.source_url,
    lastSyncedAt: row.last_synced_at,
  };
}

export async function listDueNewJerseyCandidateFinanceSyncRows(
  db: Queryable,
  input: {
    now: Date;
    staleAfterDays: number;
    maxCandidates: number;
    electionLookbackDays: number;
    electionLookaheadDays: number;
  }
): Promise<{ rows: NewJerseyCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await db.query<NewJerseyCandidateFinanceDueQueryRow>(
    `
      WITH due AS (
        SELECT
          candidate.id::text AS candidate_id,
          election.id::text AS election_id,
          COALESCE(
            NULLIF(trim(candidate.display_name), ''),
            NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')
          ) AS candidate_name,
          EXTRACT(YEAR FROM election.election_date)::int AS election_year,
          office.scope AS office_scope,
          office.canonical_name AS office_name,
          district.name AS district,
          link.source_url,
          summary.last_synced_at::text AS last_synced_at,
          COUNT(*) OVER () AS total_due_rows
        FROM public.candidate_elections AS candidate_election
        JOIN public.candidates AS candidate
          ON candidate.id = candidate_election.candidate_id
        JOIN public.elections AS election
          ON election.id = candidate_election.election_id
        JOIN public.districts AS district
          ON district.id = election.district_id
        LEFT JOIN public.offices AS office
          ON office.id = election.office_id
        LEFT JOIN LATERAL (
          SELECT existing_link.*
          FROM public.nj_candidate_finance_links AS existing_link
          WHERE existing_link.candidate_id = candidate.id
            AND existing_link.election_id = election.id
            AND existing_link.link_status = 'active'
          ORDER BY existing_link.last_verified_at DESC NULLS LAST,
                   existing_link.created_at DESC,
                   existing_link.id DESC
          LIMIT 1
        ) AS link ON true
        LEFT JOIN public.nj_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        WHERE candidate.deleted_at IS NULL
          AND district.state = 'NJ'
          AND election.race_type = 'office'
          AND election.election_date >= (($1::timestamptz AT TIME ZONE 'UTC')::date - make_interval(days => $4::int))
          AND election.election_date <= (($1::timestamptz AT TIME ZONE 'UTC')::date + make_interval(days => $5::int))
          AND candidate_election.status NOT IN ('withdrawn', 'lost')
          AND (
            lower(regexp_replace(trim(office.scope), '\\s+', ' ', 'g')) ||
            '::' ||
            lower(regexp_replace(trim(office.canonical_name), '\\s+', ' ', 'g'))
          ) = ANY($6::text[])
          AND COALESCE(
            NULLIF(trim(candidate.display_name), ''),
            NULLIF(trim(candidate.first_name || ' ' || candidate.last_name), '')
          ) IS NOT NULL
          AND (
            summary.last_synced_at IS NULL
            OR summary.last_synced_at < ($1::timestamptz - make_interval(days => $2::int))
          )
        ORDER BY summary.last_synced_at ASC NULLS FIRST,
                 election.election_date ASC,
                 candidate_name ASC,
                 candidate.id ASC
        LIMIT $3::int
      )
      SELECT
        candidate_id,
        election_id,
        candidate_name,
        election_year,
        office_scope,
        office_name,
        district,
        source_url,
        last_synced_at,
        total_due_rows
      FROM due
    `,
    [
      input.now.toISOString(),
      input.staleAfterDays,
      input.maxCandidates,
      input.electionLookbackDays,
      input.electionLookaheadDays,
      [...NEW_JERSEY_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]
  );

  return {
    rows: result.rows.map(mapDueRow),
    totalDueRows: result.rows.length > 0 ? parseTotalDueRows(result.rows[0]?.total_due_rows) : 0,
  };
}

export async function syncDueNewJerseyCandidateFinance(
  input: NewJerseyCandidateFinanceBatchSyncInput
): Promise<NewJerseyCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");

  const maxCandidates = normalizePositiveInteger(input.maxCandidates, DEFAULT_MAX_CANDIDATES, "maxCandidates");
  const staleAfterDays = normalizePositiveInteger(input.staleAfterDays, DEFAULT_STALE_AFTER_DAYS, "staleAfterDays");
  const electionLookbackDays = normalizePositiveInteger(
    input.electionLookbackDays,
    DEFAULT_POST_ELECTION_FINANCE_SYNC_GRACE_DAYS,
    "electionLookbackDays"
  );
  const electionLookaheadDays = normalizePositiveInteger(
    input.electionLookaheadDays,
    DEFAULT_ELECTION_LOOKAHEAD_DAYS,
    "electionLookaheadDays"
  );
  const dryRun = input.dryRun === true;
  const syncFn = input.syncNewJerseyCandidateFinanceFromElecFn ?? syncNewJerseyCandidateFinanceFromElec;

  const due = await listDueNewJerseyCandidateFinanceSyncRows(input.db, {
    now,
    staleAfterDays,
    maxCandidates,
    electionLookbackDays,
    electionLookaheadDays,
  });

  const results: NewJerseyCandidateFinanceBatchSyncItemResult[] = [];
  for (const row of due.rows) {
    try {
      const result = await syncFn({
        db: input.db,
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        electionYear: row.electionYear,
        officeScope: row.officeScope,
        officeName: row.officeName,
        district: row.district,
        sourceUrl: row.sourceUrl,
        dryRun,
        now,
      });
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        ok: result.status === "matched",
        status: result.status,
        candidateEntityS: result.status === "matched" ? result.resolution.entityS : undefined,
        result,
      });
    } catch (error) {
      results.push({
        candidateId: row.candidateId,
        electionId: row.electionId,
        electionYear: row.electionYear,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const syncedCandidateCount = results.filter((result) => result.ok).length;
  const skippedCandidateCount = results.filter((result) => !result.ok && result.status !== undefined).length;
  return {
    dryRun,
    now: now.toISOString(),
    staleAfterDays,
    maxCandidates,
    dueCandidateCount: due.totalDueRows,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount,
    skippedCandidateCount,
    failedCandidateCount: results.length - syncedCandidateCount - skippedCandidateCount,
    results,
  };
}
