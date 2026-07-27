import type { Pool, PoolClient } from "pg";
import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";

import {
  refreshNewYorkCityCfbArtifact,
  type NewYorkCityCfbArtifactRefreshResult,
} from "./newYorkCityCfbArtifactCache.js";
import {
  readNewYorkCityCfbContributions,
  readNewYorkCityCfbFinancialAnalysis,
  type NewYorkCityCfbContributionRow,
  type NewYorkCityCfbFinancialAnalysisRow,
} from "./newYorkCityCfbCsv.js";
import { syncNewYorkCityCandidateFinance } from "./newYorkCityCandidateFinanceSync.js";
import {
  fetchNewYorkCityCfbIndependentSpending,
  fetchNewYorkCityCfbIndependentSpenderFunders,
  resolveNewYorkCityCfbCandidateElectionCycles,
  type NewYorkCityCfbIndependentSpenderFunderRow,
  type NewYorkCityCfbIndependentSpendingRow,
} from "./newYorkCityCfbIndependentSpendingClient.js";
import {
  resolveNewYorkCityCandidate,
  type NewYorkCityCandidateFinanceResolution,
} from "./newYorkCityCandidateResolver.js";
import {
  NEW_YORK_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS,
  toNewYorkCityCfbOfficeSearchInput,
} from "./newYorkCityFinanceEligibleOffices.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type Connectable = Queryable & Pick<Pool, "connect">;

export type NewYorkCityCandidateFinanceDueRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeCanonicalName: string;
  districtGeoid: string;
  cfbCandidateId: string | null;
};

export type NewYorkCityCandidateFinanceBatchItem = {
  candidateId: string;
  electionId: string;
  status: "synced" | "unmatched" | "ambiguous" | "not_yet_published" | "failed";
  reason?: string;
  nextCheckAt?: string;
};

type NewYorkCityCandidateFinanceAttemptStatus =
  | "unmatched"
  | "ambiguous"
  | "not_yet_published"
  | "failed";

export type NewYorkCityCandidateFinanceBatchResult = {
  dryRun: boolean;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  deferredCandidateCount: number;
  failedCandidateCount: number;
  results: NewYorkCityCandidateFinanceBatchItem[];
};

type DueQueryRow = {
  candidate_id: string;
  election_id: string;
  candidate_name: string;
  election_year: number;
  office_scope: string;
  office_canonical_name: string;
  district_geoid: string;
  cfb_candidate_id: string | null;
  total_due_rows: string | number;
};

const DEFAULT_MAX_CANDIDATES = 25;
const DEFAULT_STALE_AFTER_DAYS = 7;
const DEFAULT_ELECTION_LOOKAHEAD_DAYS = 1_460;
const DEFAULT_ELECTION_LOOKBACK_DAYS = 1_460;

function positiveInteger(value: number | undefined, fallback: number, field: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) throw new Error(`Invalid NYC finance ${field}: ${value}`);
  return normalized;
}

export async function listDueNewYorkCityCandidateFinanceRows(input: {
  db: Queryable;
  now: Date;
  maxCandidates: number;
  staleAfterDays: number;
  electionLookaheadDays: number;
  electionLookbackDays: number;
}): Promise<{ rows: NewYorkCityCandidateFinanceDueRow[]; totalDueRows: number }> {
  const result = await input.db.query<DueQueryRow>(
    `
      WITH due AS (
        SELECT
          candidate.id::text AS candidate_id,
          election.id::text AS election_id,
          COALESCE(NULLIF(btrim(candidate.display_name), ''), btrim(candidate.first_name || ' ' || candidate.last_name)) AS candidate_name,
          EXTRACT(YEAR FROM election.election_date)::integer AS election_year,
          office.scope AS office_scope,
          office.canonical_name AS office_canonical_name,
          district.geoid_compact AS district_geoid,
          link.cfb_candidate_id,
          COUNT(*) OVER () AS total_due_rows
        FROM public.candidate_elections AS candidate_election
        JOIN public.candidates AS candidate ON candidate.id = candidate_election.candidate_id
        JOIN public.elections AS election ON election.id = candidate_election.election_id
        JOIN public.offices AS office ON office.id = election.office_id
        JOIN public.districts AS district ON district.id = election.district_id
        LEFT JOIN public.nyc_candidate_finance_links AS link
          ON link.candidate_id = candidate.id
         AND link.election_id = election.id
         AND link.link_status = 'active'
        LEFT JOIN public.nyc_candidate_finance_summaries AS summary
          ON summary.link_id = link.id
         AND summary.election_year = link.election_year
        LEFT JOIN public.nyc_candidate_finance_sync_attempts AS attempt
          ON attempt.candidate_id = candidate.id
         AND attempt.election_id = election.id
        WHERE candidate.deleted_at IS NULL
          AND district.state = 'NY'
          AND election.race_type = 'office'
          AND election.election_date >= (($1::timestamptz AT TIME ZONE 'UTC')::date - make_interval(days => $5::int))
          AND election.election_date <= (($1::timestamptz AT TIME ZONE 'UTC')::date + make_interval(days => $4::int))
          AND candidate_election.status <> 'withdrawn'
          AND (office.scope || '::' || office.canonical_name) = ANY($6::text[])
          AND (
            summary.last_synced_at IS NULL
            OR summary.last_synced_at < ($1::timestamptz - make_interval(days => $3::int))
          )
          AND (attempt.next_attempt_at IS NULL OR attempt.next_attempt_at <= $1::timestamptz)
        ORDER BY COALESCE(summary.last_synced_at, attempt.last_attempted_at) ASC NULLS FIRST,
                 election.election_date ASC, candidate_name ASC
        LIMIT $2::int
      )
      SELECT * FROM due
    `,
    [
      input.now.toISOString(),
      input.maxCandidates,
      input.staleAfterDays,
      input.electionLookaheadDays,
      input.electionLookbackDays,
      Array.from(NEW_YORK_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS),
    ]
  );
  return {
    rows: result.rows.map((row) => ({
      candidateId: row.candidate_id,
      electionId: row.election_id,
      candidateName: row.candidate_name,
      electionYear: row.election_year,
      officeScope: row.office_scope,
      officeCanonicalName: row.office_canonical_name,
      districtGeoid: row.district_geoid,
      cfbCandidateId: row.cfb_candidate_id,
    })),
    totalDueRows: Number(result.rows[0]?.total_due_rows ?? 0),
  };
}

export async function recordNewYorkCityCandidateFinanceAttempt(input: {
  db: Queryable;
  candidateId: string;
  electionId: string;
  status: NewYorkCityCandidateFinanceAttemptStatus;
  reason?: string;
  attemptedAt: Date;
  nextAttemptAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.nyc_candidate_finance_sync_attempts (
        candidate_id, election_id, status, reason, last_attempted_at, next_attempt_at
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (candidate_id, election_id)
      DO UPDATE SET status = EXCLUDED.status, reason = EXCLUDED.reason,
        last_attempted_at = EXCLUDED.last_attempted_at, next_attempt_at = EXCLUDED.next_attempt_at
    `,
    [input.candidateId, input.electionId, input.status, input.reason ?? null,
      input.attemptedAt.toISOString(), input.nextAttemptAt.toISOString()]
  );
}

function resolutionForDueRow(
  due: NewYorkCityCandidateFinanceDueRow,
  analysisRows: readonly NewYorkCityCfbFinancialAnalysisRow[]
): NewYorkCityCandidateFinanceResolution {
  if (!due.cfbCandidateId) {
    return resolveNewYorkCityCandidate({
      candidateName: due.candidateName,
      electionYear: due.electionYear,
      officeScope: due.officeScope,
      officeCanonicalName: due.officeCanonicalName,
      districtGeoid: due.districtGeoid,
      analysisRows,
    });
  }
  const expectedOffice = toNewYorkCityCfbOfficeSearchInput({
    officeScope: due.officeScope,
    officeCanonicalName: due.officeCanonicalName,
    districtGeoid: due.districtGeoid,
  });
  if (!expectedOffice) return { status: "unmatched", reason: "unsupported_office" };
  const rows = analysisRows
    .filter((row) =>
      row.electionYear === due.electionYear &&
      row.candidateId === due.cfbCandidateId &&
      row.officeCode === expectedOffice.officeCode &&
      row.boroughCode === expectedOffice.boroughCode
    )
    .sort((left, right) => right.toStatement - left.toStatement || right.fromStatement - left.fromStatement);
  const summary = rows[0];
  return summary
    ? {
        status: "matched",
        cfbCandidateId: summary.candidateId,
        cfbCandidateName: summary.candidateName,
        officeCode: summary.officeCode,
        boroughCode: summary.boroughCode,
        summary,
      }
    : { status: "unmatched", reason: "no_exact_match" };
}

type BatchDataSource = {
  listDueRows: typeof listDueNewYorkCityCandidateFinanceRows;
  refreshArtifact: typeof refreshNewYorkCityCfbArtifact;
  readAnalysis: typeof readNewYorkCityCfbFinancialAnalysis;
  readContributions: typeof readNewYorkCityCfbContributions;
  fetchOutsideSpending: typeof fetchNewYorkCityCfbIndependentSpending;
  fetchOutsideFunders: typeof fetchNewYorkCityCfbIndependentSpenderFunders;
  resolveOutsideCycles: typeof resolveNewYorkCityCfbCandidateElectionCycles;
  syncCandidate: typeof syncNewYorkCityCandidateFinance;
  recordAttempt: typeof recordNewYorkCityCandidateFinanceAttempt;
};

const DEFAULT_DATA_SOURCE: BatchDataSource = {
  listDueRows: listDueNewYorkCityCandidateFinanceRows,
  refreshArtifact: refreshNewYorkCityCfbArtifact,
  readAnalysis: readNewYorkCityCfbFinancialAnalysis,
  readContributions: readNewYorkCityCfbContributions,
  fetchOutsideSpending: fetchNewYorkCityCfbIndependentSpending,
  fetchOutsideFunders: fetchNewYorkCityCfbIndependentSpenderFunders,
  resolveOutsideCycles: resolveNewYorkCityCfbCandidateElectionCycles,
  syncCandidate: syncNewYorkCityCandidateFinance,
  recordAttempt: recordNewYorkCityCandidateFinanceAttempt,
};

async function recordAttempt(input: {
  dataSource: BatchDataSource;
  db: Queryable;
  row: NewYorkCityCandidateFinanceDueRow;
  status: NewYorkCityCandidateFinanceAttemptStatus;
  reason?: string;
  now: Date;
  nextAttemptAt: Date;
  dryRun: boolean;
}): Promise<NewYorkCityCandidateFinanceBatchItem> {
  if (!input.dryRun) {
    try {
      await input.dataSource.recordAttempt({
        db: input.db, candidateId: input.row.candidateId, electionId: input.row.electionId,
        status: input.status, ...(input.reason ? { reason: input.reason } : {}),
        attemptedAt: input.now, nextAttemptAt: input.nextAttemptAt,
      });
    } catch (error) {
      return { candidateId: input.row.candidateId, electionId: input.row.electionId, status: "failed",
        reason: error instanceof Error ? error.message : String(error) };
    }
  }
  return { candidateId: input.row.candidateId, electionId: input.row.electionId, status: input.status,
    ...(input.reason ? { reason: input.reason } : {}), nextCheckAt: input.nextAttemptAt.toISOString() };
}

async function recordFailure(input: {
  dataSource: BatchDataSource;
  db: Queryable;
  row: NewYorkCityCandidateFinanceDueRow;
  error: unknown;
  now: Date;
  staleAfterDays: number;
  dryRun: boolean;
}): Promise<NewYorkCityCandidateFinanceBatchItem> {
  return recordAttempt({
    dataSource: input.dataSource,
    db: input.db,
    row: input.row,
    status: "failed",
    reason: input.error instanceof Error ? input.error.message : String(input.error),
    now: input.now,
    nextAttemptAt: new Date(input.now.getTime() + input.staleAfterDays * 24 * 60 * 60 * 1000),
    dryRun: input.dryRun,
  });
}

function artifactPath(result: NewYorkCityCfbArtifactRefreshResult): string | null {
  return result.status === "not_yet_published" ? null : result.current.filePath;
}

export async function syncDueNewYorkCityCandidateFinance(input: {
  db: Connectable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookaheadDays?: number;
  electionLookbackDays?: number;
  cacheDir?: string;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  dataSource?: Partial<BatchDataSource>;
}): Promise<NewYorkCityCandidateFinanceBatchResult> {
  const now = input.now ?? new Date();
  if (Number.isNaN(now.getTime())) throw new Error("Invalid NYC finance batch timestamp");
  const dataSource = { ...DEFAULT_DATA_SOURCE, ...input.dataSource };
  const staleAfterDays = positiveInteger(input.staleAfterDays, DEFAULT_STALE_AFTER_DAYS, "stale days");
  const due = await dataSource.listDueRows({
    db: input.db,
    now,
    maxCandidates: positiveInteger(input.maxCandidates, DEFAULT_MAX_CANDIDATES, "max candidates"),
    staleAfterDays,
    electionLookaheadDays: positiveInteger(input.electionLookaheadDays, DEFAULT_ELECTION_LOOKAHEAD_DAYS, "lookahead days"),
    electionLookbackDays: positiveInteger(input.electionLookbackDays, DEFAULT_ELECTION_LOOKBACK_DAYS, "lookback days"),
  });
  const results: NewYorkCityCandidateFinanceBatchItem[] = [];
  const byYear = new Map<number, NewYorkCityCandidateFinanceDueRow[]>();
  for (const row of due.rows) {
    const rows = byYear.get(row.electionYear) ?? [];
    rows.push(row);
    byYear.set(row.electionYear, rows);
  }

  for (const [electionYear, yearRows] of byYear) {
    let contributionArtifact: NewYorkCityCfbArtifactRefreshResult;
    let analysisArtifact: NewYorkCityCfbArtifactRefreshResult;
    try {
      [contributionArtifact, analysisArtifact] = await Promise.all([
        dataSource.refreshArtifact({ cacheDir: input.cacheDir, electionYear, kind: "contributions" }),
        dataSource.refreshArtifact({ cacheDir: input.cacheDir, electionYear, kind: "financial_analysis" }),
      ]);
    } catch (error) {
      for (const row of yearRows) {
        results.push(await recordFailure({
          dataSource,
          db: input.db,
          row,
          error,
          now,
          staleAfterDays,
          dryRun: Boolean(input.dryRun),
        }));
      }
      continue;
    }
    const contributionPath = artifactPath(contributionArtifact);
    const analysisPath = artifactPath(analysisArtifact);
    if (!contributionPath || !analysisPath) {
      const nextCheckAt = [contributionArtifact, analysisArtifact]
        .filter((artifact): artifact is Extract<NewYorkCityCfbArtifactRefreshResult, { status: "not_yet_published" }> => artifact.status === "not_yet_published")
        .map((artifact) => artifact.nextCheckAt)
        .sort()
        .at(-1);
      for (const row of yearRows) {
        const proposedNextAttemptAt = nextCheckAt ? new Date(nextCheckAt) : null;
        const nextAttemptAt = proposedNextAttemptAt && proposedNextAttemptAt > now
          ? proposedNextAttemptAt : new Date(now.getTime() + 24 * 60 * 60 * 1000);
        results.push(await recordAttempt({ dataSource, db: input.db, row, status: "not_yet_published",
          now, nextAttemptAt, dryRun: Boolean(input.dryRun) }));
      }
      continue;
    }
    try {
      const analysisRows = (await dataSource.readAnalysis({ filePath: analysisPath })).rows;
      const resolutions = new Map<string, NewYorkCityCandidateFinanceResolution>();
      const cfbCandidateIds = new Set<string>();
      for (const row of yearRows) {
        const resolution = resolutionForDueRow(row, analysisRows);
        resolutions.set(`${row.candidateId}\u0000${row.electionId}`, resolution);
        if (resolution.status === "matched") cfbCandidateIds.add(resolution.cfbCandidateId);
      }
      let contributionRows: NewYorkCityCfbContributionRow[] = [];
      const outsideByCycle = new Map<string, {
        spendingRows?: NewYorkCityCfbIndependentSpendingRow[];
        funderRows?: NewYorkCityCfbIndependentSpenderFunderRow[];
        error?: unknown;
      }>();
      let outsideCycles: Awaited<ReturnType<typeof resolveNewYorkCityCfbCandidateElectionCycles>> | undefined;
      let outsideCycleError: unknown;
      if (cfbCandidateIds.size) {
        contributionRows = (
          await dataSource.readContributions({ filePath: contributionPath, candidateIds: cfbCandidateIds })
        ).rows;
        try {
          outsideCycles = await dataSource.resolveOutsideCycles({ electionYear, candidateIds: cfbCandidateIds });
          await Promise.all([...new Set(outsideCycles.resolved.values())].map(async (electionCycle) => {
            try {
              const [outsideSpending, outsideFunders] = await Promise.all([
                dataSource.fetchOutsideSpending({ electionYear, electionCycle }),
                dataSource.fetchOutsideFunders({ electionYear, electionCycle }),
              ]);
              outsideByCycle.set(electionCycle, {
                spendingRows: outsideSpending.rows,
                funderRows: outsideFunders.rows,
              });
            } catch (error) {
              outsideByCycle.set(electionCycle, { error });
            }
          }));
        } catch (error) {
          outsideCycleError = error;
        }
      }
      for (const row of yearRows) {
        const resolution = resolutions.get(`${row.candidateId}\u0000${row.electionId}`)!;
        if (resolution.status !== "matched") {
          results.push(await recordAttempt({ dataSource, db: input.db, row, status: resolution.status,
            reason: resolution.reason, now,
            nextAttemptAt: new Date(now.getTime() + staleAfterDays * 24 * 60 * 60 * 1000),
            dryRun: Boolean(input.dryRun) }));
          continue;
        }
        const electionCycle = outsideCycles?.resolved.get(resolution.cfbCandidateId);
        const outsideData = electionCycle ? outsideByCycle.get(electionCycle) : undefined;
        const outsideReason = outsideCycleError
          ? outsideCycleError instanceof Error ? outsideCycleError.message : String(outsideCycleError)
          : outsideCycles?.ambiguousCandidateIds.has(resolution.cfbCandidateId)
            ? `ambiguous election cycle for CFB candidate ${resolution.cfbCandidateId}`
            : outsideCycles?.missingCandidateIds.has(resolution.cfbCandidateId)
              ? `no exact election cycle for CFB candidate ${resolution.cfbCandidateId}`
              : outsideData?.error
                ? outsideData.error instanceof Error ? outsideData.error.message : String(outsideData.error)
                : undefined;
        try {
          await dataSource.syncCandidate({
            db: input.db,
            candidateId: row.candidateId,
            electionId: row.electionId,
            candidateName: row.candidateName,
            electionYear,
            resolution,
            contributionRows,
            ...(electionCycle && outsideData?.spendingRows && outsideData.funderRows
              ? {
                  outsideElectionCycle: electionCycle,
                  outsideSpendingRows: outsideData.spendingRows,
                  outsideFunderRows: outsideData.funderRows,
                }
              : {}),
            now,
            dryRun: input.dryRun,
            financeIndustryClassifier: input.financeIndustryClassifier,
            aiClassificationMinAmount: input.aiClassificationMinAmount,
          });
          results.push({
            candidateId: row.candidateId,
            electionId: row.electionId,
            status: "synced",
            ...(outsideReason
              ? { reason: `outside_spending_unavailable: ${outsideReason}` }
              : {}),
          });
        } catch (error) {
          results.push(await recordFailure({
            dataSource,
            db: input.db,
            row,
            error,
            now,
            staleAfterDays,
            dryRun: Boolean(input.dryRun),
          }));
        }
      }
    } catch (error) {
      for (const row of yearRows) {
        if (!results.some((result) => result.candidateId === row.candidateId && result.electionId === row.electionId)) {
          results.push(await recordFailure({
            dataSource,
            db: input.db,
            row,
            error,
            now,
            staleAfterDays,
            dryRun: Boolean(input.dryRun),
          }));
        }
      }
    }
  }

  return {
    dryRun: Boolean(input.dryRun),
    dueCandidateCount: due.totalDueRows,
    selectedCandidateCount: due.rows.length,
    syncedCandidateCount: results.filter((row) => row.status === "synced").length,
    deferredCandidateCount: results.filter((row) => row.status === "not_yet_published" || row.status === "unmatched" || row.status === "ambiguous").length,
    failedCandidateCount: results.filter((row) => row.status === "failed").length,
    results,
  };
}
