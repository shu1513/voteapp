import { Pool } from "pg";

import { getPipelineEnv } from "../../config/env.js";
import {
  verifyHttpUrlReachability,
  type UrlReachabilityResult,
} from "../../ai/urlReachability.js";
import { readSourceUrlHealthPolicyFromEnv } from "./sourceUrlHealthPolicy.js";

type ProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
};

type UrlHealthRow = {
  url: string;
  last_checked_at: Date | null;
  last_http_status: number | null;
  last_error: string | null;
  consecutive_hard_failures: number;
  first_hard_failed_at: Date | null;
  last_hard_failed_at: Date | null;
};

type CheckedUrlState = {
  url: string;
  lastCheckedAt: Date;
  lastHttpStatus: number | null;
  lastError: string | null;
  consecutiveHardFailures: number;
  firstHardFailedAt: Date | null;
  lastHardFailedAt: Date | null;
};

type UrlCheckOutcome = "healthy" | "hard_fail" | "transient_fail";

export type UrlHealthClassification = {
  outcome: UrlCheckOutcome;
  statusCode: number | null;
  reason: string | null;
};

export type SourceUrlHealthProducerResult = {
  enabled: boolean;
  cleanupEnabled: boolean;
  dryRun: boolean;
  force: boolean;
  asOfTimestamp: string;
  staleAfterDays: number;
  maxUrlsPerRun: number;
  hardFailureThreshold: number;
  hardFailureWindowDays: number;
  urls_scanned: number;
  candidates_due_check: number;
  checked_count: number;
  healthy_count: number;
  hard_fail_count: number;
  transient_fail_count: number;
  cleanup_candidate_count: number;
  cleanup_removed_count: number;
  cleanup_urls_count: number;
  failed_count: number;
};

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function parseStatusCodeFromReason(reason: string): number | null {
  const patterns = [
    /\bstatus\s+(\d{3})\b/i,
    /\bhttp\s+(\d{3})\b/i,
    /\b(\d{3})\s+not\s+found\b/i,
    /\b(\d{3})\s+gone\b/i,
  ];
  for (const pattern of patterns) {
    const match = reason.match(pattern);
    if (!match) {
      continue;
    }
    const parsed = Number.parseInt(match[1] ?? "", 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

export function classifyUrlHealthCheckResult(result: UrlReachabilityResult): UrlHealthClassification {
  if (result.ok) {
    return {
      outcome: "healthy",
      statusCode: result.status,
      reason: null,
    };
  }

  const statusCode = parseStatusCodeFromReason(result.reason);
  if (statusCode === 404 || statusCode === 410) {
    return {
      outcome: "hard_fail",
      statusCode,
      reason: result.reason,
    };
  }

  return {
    outcome: "transient_fail",
    statusCode,
    reason: result.reason,
  };
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

export function isCleanupEligible(input: {
  consecutiveHardFailures: number;
  firstHardFailedAt: Date | null;
  lastHttpStatus: number | null;
  hardFailureThreshold: number;
  hardFailureWindowDays: number;
  asOf: Date;
}): boolean {
  if (input.lastHttpStatus !== 404 && input.lastHttpStatus !== 410) {
    return false;
  }
  if (input.consecutiveHardFailures < input.hardFailureThreshold) {
    return false;
  }
  if (!input.firstHardFailedAt) {
    return false;
  }
  return addDays(input.firstHardFailedAt, input.hardFailureWindowDays) <= input.asOf;
}

async function listDistinctElectionSeedUrlsCount(pool: Pool): Promise<number> {
  const result = await pool.query<{ count: string }>(
    `
      SELECT COUNT(*)::text AS count
      FROM (
        SELECT DISTINCT url
        FROM public.election_seed_urls
      ) AS distinct_urls
    `
  );
  return Number.parseInt(result.rows[0]?.count ?? "0", 10);
}

async function listUrlsDueForHealthCheck(
  pool: Pool,
  input: { asOfTimestamp: string; staleAfterDays: number; limit: number }
): Promise<UrlHealthRow[]> {
  const result = await pool.query<UrlHealthRow>(
    `
      WITH distinct_urls AS (
        SELECT DISTINCT url
        FROM public.election_seed_urls
      )
      SELECT
        d.url,
        h.last_checked_at,
        h.last_http_status,
        h.last_error,
        COALESCE(h.consecutive_hard_failures, 0) AS consecutive_hard_failures,
        h.first_hard_failed_at,
        h.last_hard_failed_at
      FROM distinct_urls AS d
      LEFT JOIN public.source_url_health AS h
        ON h.url = d.url
      WHERE h.last_checked_at IS NULL
         OR h.last_checked_at < ($1::timestamptz - make_interval(days => $2::int))
      ORDER BY h.last_checked_at ASC NULLS FIRST, d.url ASC
      LIMIT $3::int
    `,
    [input.asOfTimestamp, input.staleAfterDays, input.limit]
  );
  return result.rows;
}

export function computeHardFailureStreakAfterCheck(input: {
  priorConsecutiveHardFailures: number;
  priorFirstHardFailedAt: Date | null;
  priorLastHardFailedAt: Date | null;
  checkedAt: Date;
  classification: UrlHealthClassification;
}): {
  consecutiveHardFailures: number;
  firstHardFailedAt: Date | null;
  lastHardFailedAt: Date | null;
} {
  if (input.classification.outcome === "hard_fail") {
    const priorStreakActive =
      input.priorConsecutiveHardFailures > 0 && input.priorFirstHardFailedAt !== null;
    const nextConsecutive = priorStreakActive ? input.priorConsecutiveHardFailures + 1 : 1;
    const firstHardFailedAt = priorStreakActive
      ? input.priorFirstHardFailedAt
      : input.checkedAt;
    return {
      consecutiveHardFailures: nextConsecutive,
      firstHardFailedAt,
      lastHardFailedAt: input.checkedAt,
    };
  }

  if (input.classification.outcome === "healthy") {
    return {
      consecutiveHardFailures: 0,
      firstHardFailedAt: null,
      lastHardFailedAt: null,
    };
  }

  return {
    consecutiveHardFailures: input.priorConsecutiveHardFailures,
    firstHardFailedAt: input.priorFirstHardFailedAt,
    lastHardFailedAt: input.priorLastHardFailedAt,
  };
}

async function upsertCheckedUrlStates(pool: Pool, rows: CheckedUrlState[]): Promise<void> {
  if (rows.length === 0) {
    return;
  }
  await pool.query(
    `
      INSERT INTO public.source_url_health (
        url,
        last_checked_at,
        last_http_status,
        last_error,
        consecutive_hard_failures,
        first_hard_failed_at,
        last_hard_failed_at
      )
      SELECT
        t.url,
        t.last_checked_at,
        t.last_http_status,
        t.last_error,
        t.consecutive_hard_failures,
        t.first_hard_failed_at,
        t.last_hard_failed_at
      FROM unnest(
        $1::text[],
        $2::timestamptz[],
        $3::int[],
        $4::text[],
        $5::int[],
        $6::timestamptz[],
        $7::timestamptz[]
      ) AS t(
        url,
        last_checked_at,
        last_http_status,
        last_error,
        consecutive_hard_failures,
        first_hard_failed_at,
        last_hard_failed_at
      )
      ON CONFLICT (url) DO UPDATE
      SET
        last_checked_at = EXCLUDED.last_checked_at,
        last_http_status = EXCLUDED.last_http_status,
        last_error = EXCLUDED.last_error,
        consecutive_hard_failures = EXCLUDED.consecutive_hard_failures,
        first_hard_failed_at = EXCLUDED.first_hard_failed_at,
        last_hard_failed_at = EXCLUDED.last_hard_failed_at,
        updated_at = now()
    `,
    [
      rows.map((row) => row.url),
      rows.map((row) => row.lastCheckedAt.toISOString()),
      rows.map((row) => row.lastHttpStatus),
      rows.map((row) => row.lastError),
      rows.map((row) => row.consecutiveHardFailures),
      rows.map((row) => row.firstHardFailedAt?.toISOString() ?? null),
      rows.map((row) => row.lastHardFailedAt?.toISOString() ?? null),
    ]
  );
}

async function listCleanupCandidateUrls(
  pool: Pool,
  input: {
    hardFailureThreshold: number;
    hardFailureWindowDays: number;
    asOfTimestamp: string;
    limit: number;
  }
): Promise<string[]> {
  const result = await pool.query<{ url: string }>(
    `
      SELECT h.url
      FROM public.source_url_health AS h
      WHERE h.consecutive_hard_failures >= $1::int
        AND h.first_hard_failed_at IS NOT NULL
        AND h.first_hard_failed_at <= ($2::timestamptz - make_interval(days => $3::int))
        AND h.last_http_status IN (404, 410)
        -- source_url_health is shared with the candidate-website checker, so
        -- an unscoped select would hand candidate URLs to THIS cleanup, which
        -- would delete their health rows (erasing retirement streaks) and burn
        -- this run's cleanup slots on rows it cannot act on.
        AND EXISTS (
          SELECT 1
          FROM public.election_seed_urls AS s
          WHERE s.url = h.url
        )
      ORDER BY h.first_hard_failed_at ASC, h.url ASC
      LIMIT $4::int
    `,
    [input.hardFailureThreshold, input.asOfTimestamp, input.hardFailureWindowDays, input.limit]
  );
  return result.rows.map((row) => row.url);
}

async function deleteElectionSeedUrlsByUrl(pool: Pool, urls: readonly string[]): Promise<{ removedCount: number; removedUrls: string[] }> {
  if (urls.length === 0) {
    return { removedCount: 0, removedUrls: [] };
  }
  const result = await pool.query<{ url: string }>(
    `
      DELETE FROM public.election_seed_urls
      WHERE url = ANY($1::text[])
      RETURNING url
    `,
    [urls]
  );

  const removedUrls = [...new Set(result.rows.map((row) => row.url))];
  return {
    removedCount: result.rowCount ?? 0,
    removedUrls,
  };
}

async function deleteSourceUrlHealthByUrl(pool: Pool, urls: readonly string[]): Promise<number> {
  if (urls.length === 0) {
    return 0;
  }
  const result = await pool.query(
    `
      DELETE FROM public.source_url_health
      WHERE url = ANY($1::text[])
        -- A URL can be both an election seed and a candidate's website. The
        -- seed rows are gone by the time this runs, but the candidate checker
        -- still tracks the URL here — deleting its row would erase the hard-
        -- failure streak its retirement gate is accumulating.
        AND NOT EXISTS (
          SELECT 1
          FROM public.candidates AS c
          WHERE c.official_website_url = source_url_health.url
            AND c.deleted_at IS NULL
            AND c.merged_into_candidate_id IS NULL
        )
    `,
    [urls]
  );
  return result.rowCount ?? 0;
}

function nextCheckedState(input: {
  prior: UrlHealthRow;
  checkedAt: Date;
  classification: UrlHealthClassification;
}): CheckedUrlState {
  const streak = computeHardFailureStreakAfterCheck({
    priorConsecutiveHardFailures: input.prior.consecutive_hard_failures,
    priorFirstHardFailedAt: input.prior.first_hard_failed_at,
    priorLastHardFailedAt: input.prior.last_hard_failed_at,
    checkedAt: input.checkedAt,
    classification: input.classification,
  });

  return {
    url: input.prior.url,
    lastCheckedAt: input.checkedAt,
    lastHttpStatus:
      input.classification.outcome === "transient_fail"
        ? input.prior.last_http_status
        : input.classification.statusCode,
    lastError:
      input.classification.outcome === "transient_fail"
        ? input.prior.last_error
        : input.classification.reason,
    consecutiveHardFailures: streak.consecutiveHardFailures,
    firstHardFailedAt: streak.firstHardFailedAt,
    lastHardFailedAt: streak.lastHardFailedAt,
  };
}

async function checkUrlsWithConcurrency(
  rows: UrlHealthRow[],
  input: { timeoutMs: number; concurrency: number }
): Promise<{ checkedRows: CheckedUrlState[]; healthy: number; hardFail: number; transientFail: number; failedCount: number }> {
  const workerCount = Math.max(1, Math.min(input.concurrency, rows.length));
  const checkedRows: CheckedUrlState[] = [];
  let healthy = 0;
  let hardFail = 0;
  let transientFail = 0;
  let failedCount = 0;
  let nextIndex = 0;

  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= rows.length) {
        return;
      }

      const row = rows[currentIndex];
      if (!row) {
        continue;
      }

      try {
        const reachability = await verifyHttpUrlReachability(row.url, {
          timeoutMs: input.timeoutMs,
          // Keep 403 as non-fatal for now: many valid election authority pages bot-block
          // automated probes while remaining user-reachable in browsers.
          allowStatusCodes: [403],
        });
        const classification = classifyUrlHealthCheckResult(reachability);
        if (classification.outcome === "healthy") {
          healthy += 1;
        } else if (classification.outcome === "hard_fail") {
          hardFail += 1;
        } else {
          transientFail += 1;
        }
        checkedRows.push(
          nextCheckedState({
            prior: row,
            checkedAt: new Date(),
            classification,
          })
        );
      } catch {
        failedCount += 1;
      }
    }
  });

  await Promise.all(workers);
  return {
    checkedRows,
    healthy,
    hardFail,
    transientFail,
    failedCount,
  };
}

export async function runSourceUrlHealthProducer(
  options: ProducerOptions = {}
): Promise<SourceUrlHealthProducerResult> {
  const { dryRun = false, force = false } = options;
  const policy = readSourceUrlHealthPolicyFromEnv();
  const enabled = force || policy.enabled;

  if (!enabled) {
    console.log(
      `source_url_health producer skipped: disabled by flag (as_of=${policy.asOfTimestamp})`
    );
    return {
      enabled: false,
      cleanupEnabled: policy.cleanupEnabled,
      dryRun,
      force,
      asOfTimestamp: policy.asOfTimestamp,
      staleAfterDays: policy.staleAfterDays,
      maxUrlsPerRun: policy.maxUrlsPerRun,
      hardFailureThreshold: policy.hardFailureThreshold,
      hardFailureWindowDays: policy.hardFailureWindowDays,
      urls_scanned: 0,
      candidates_due_check: 0,
      checked_count: 0,
      healthy_count: 0,
      hard_fail_count: 0,
      transient_fail_count: 0,
      cleanup_candidate_count: 0,
      cleanup_removed_count: 0,
      cleanup_urls_count: 0,
      failed_count: 0,
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const [urls_scanned, dueRows] = await Promise.all([
      listDistinctElectionSeedUrlsCount(pool),
      listUrlsDueForHealthCheck(pool, {
        asOfTimestamp: policy.asOfTimestamp,
        staleAfterDays: policy.staleAfterDays,
        limit: policy.maxUrlsPerRun,
      }),
    ]);

    const candidates_due_check = dueRows.length;

    if (dueRows.length === 0 && (!policy.cleanupEnabled || dryRun)) {
      return {
        enabled: true,
        cleanupEnabled: policy.cleanupEnabled,
        dryRun,
        force,
        asOfTimestamp: policy.asOfTimestamp,
        staleAfterDays: policy.staleAfterDays,
        maxUrlsPerRun: policy.maxUrlsPerRun,
        hardFailureThreshold: policy.hardFailureThreshold,
        hardFailureWindowDays: policy.hardFailureWindowDays,
        urls_scanned,
        candidates_due_check,
        checked_count: 0,
        healthy_count: 0,
        hard_fail_count: 0,
        transient_fail_count: 0,
        cleanup_candidate_count: 0,
        cleanup_removed_count: 0,
        cleanup_urls_count: 0,
        failed_count: 0,
      };
    }

    const {
      checkedRows,
      healthy,
      hardFail,
      transientFail,
      failedCount,
    } = await checkUrlsWithConcurrency(dueRows, {
      timeoutMs: policy.timeoutMs,
      concurrency: policy.concurrency,
    });

    if (!dryRun) {
      await upsertCheckedUrlStates(pool, checkedRows);
    }

    const cleanupCandidates = await listCleanupCandidateUrls(pool, {
      hardFailureThreshold: policy.hardFailureThreshold,
      hardFailureWindowDays: policy.hardFailureWindowDays,
      asOfTimestamp: policy.asOfTimestamp,
      limit: policy.maxCleanupUrlsPerRun,
    });

    let cleanupRemovedCount = 0;
    let cleanupUrlsCount = 0;
    if (policy.cleanupEnabled && !dryRun && cleanupCandidates.length > 0) {
      const deleted = await deleteElectionSeedUrlsByUrl(pool, cleanupCandidates);
      const healthRowsRemovedCount = await deleteSourceUrlHealthByUrl(pool, cleanupCandidates);
      cleanupRemovedCount = deleted.removedCount;
      cleanupUrlsCount = deleted.removedUrls.length;
      if (deleted.removedUrls.length > 0 || healthRowsRemovedCount > 0) {
        console.log(
          `source_url_health cleanup removed election_seed_urls rows=${deleted.removedCount} urls=${deleted.removedUrls.length} source_url_health_rows=${healthRowsRemovedCount}`
        );
      }
    }

    return {
      enabled: true,
      cleanupEnabled: policy.cleanupEnabled,
      dryRun,
      force,
      asOfTimestamp: policy.asOfTimestamp,
      staleAfterDays: policy.staleAfterDays,
      maxUrlsPerRun: policy.maxUrlsPerRun,
      hardFailureThreshold: policy.hardFailureThreshold,
      hardFailureWindowDays: policy.hardFailureWindowDays,
      urls_scanned,
      candidates_due_check,
      checked_count: checkedRows.length,
      healthy_count: healthy,
      hard_fail_count: hardFail,
      transient_fail_count: transientFail,
      cleanup_candidate_count: cleanupCandidates.length,
      cleanup_removed_count: cleanupRemovedCount,
      cleanup_urls_count: cleanupUrlsCount,
      failed_count: failedCount,
    };
  } finally {
    await pool.end();
  }
}
