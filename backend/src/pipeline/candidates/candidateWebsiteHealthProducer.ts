import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../../config/env.js";
import {
  verifyHttpUrlReachability,
  type UrlReachabilityResult,
} from "../../ai/urlReachability.js";
import { normalizeOptionalUrl } from "../../utils/candidateIdentity.js";
import {
  classifyUrlHealthCheckResult,
  computeHardFailureStreakAfterCheck,
  type UrlHealthClassification,
} from "../elections/sourceUrlHealthProducer.js";
import { readCandidateWebsiteHealthPolicyFromEnv } from "./candidateWebsiteHealthPolicy.js";

type ProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
  maxUrlsOverride?: number;
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

export type OffDomainRedirect = {
  url: string;
  finalUrl: string;
};

export type DeepLinkRootAlive = {
  url: string;
  rootUrl: string;
};

export type RetiredWebsite = {
  candidateId: string;
  displayName: string | null;
  url: string;
};

export type CandidateWebsiteHealthProducerResult = {
  enabled: boolean;
  retireEnabled: boolean;
  dryRun: boolean;
  force: boolean;
  asOfTimestamp: string;
  staleAfterDays: number;
  maxUrlsPerRun: number;
  hardFailureThreshold: number;
  hardFailureWindowDays: number;
  urls_total: number;
  urls_due_check: number;
  checked_count: number;
  healthy_count: number;
  hard_fail_count: number;
  transient_fail_count: number;
  failed_count: number;
  off_domain_redirects: OffDomainRedirect[];
  hard_fail_urls: string[];
  deep_link_root_alive: DeepLinkRootAlive[];
  retire_candidate_count: number;
  retired: RetiredWebsite[];
};

// Same reason-string contract as the shared classifier's unresolved-hostname
// rule; retirement eligibility below matches stored last_error against it.
const UNRESOLVED_HOSTNAME_REASON = "hostname could not be resolved";

// Delegates entirely to the shared classifier: source_url_health rows are
// shared with the seed-URL producer, so the two MUST classify identically or
// their writes contradict each other's streaks on overlapping URLs. The
// unresolved-hostname-is-hard rule (dead campaign domain, squatter bait)
// lives in the shared classifier for exactly that reason.
export function classifyCandidateWebsiteCheckResult(
  result: UrlReachabilityResult
): UrlHealthClassification {
  return classifyUrlHealthCheckResult(result);
}

function comparableHost(url: string): string | null {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return host.startsWith("www.") ? host.slice(4) : host;
  } catch {
    return null;
  }
}

/**
 * Report-only signal: a healthy check whose redirect chain landed on a
 * different host. Campaign domains legitimately front hosted builders
 * (wixsite.com, actblue.com, …), so this never affects classification or
 * retirement — it exists so a domain that now bounces somewhere unexpected
 * shows up in the run report.
 */
export function isOffDomainRedirect(inputUrl: string, finalUrl: string): boolean {
  const inputHost = comparableHost(inputUrl);
  const finalHost = comparableHost(finalUrl);
  if (!inputHost || !finalHost) {
    return false;
  }
  return inputHost !== finalHost;
}

/**
 * A stored URL that points at a subpage ("/about", "/meet-jane") rots long
 * before the site does — the page gets renamed and the campaign never updates
 * the link we hold. Returns the origin root worth probing, or null when the
 * URL is already a root (nothing to suggest) or unparseable.
 */
export function rootUrlOfDeepLink(url: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  const hasPath = parsed.pathname !== "" && parsed.pathname !== "/";
  if (!hasPath && parsed.search === "") {
    return null;
  }
  return `${parsed.protocol}//${parsed.host}/`;
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

/**
 * Retirement mirrors the seed-URL cleanup gate (streak + age window) but its
 * hard statuses differ: alongside 404/410, a NULL status whose stored error is
 * the unresolved-hostname reason qualifies — a dead domain answers nothing, so
 * it can never present a status code.
 */
export function isRetireEligible(input: {
  consecutiveHardFailures: number;
  firstHardFailedAt: Date | null;
  lastHttpStatus: number | null;
  lastError: string | null;
  hardFailureThreshold: number;
  hardFailureWindowDays: number;
  asOf: Date;
}): boolean {
  const hardStatus =
    input.lastHttpStatus === 404 ||
    input.lastHttpStatus === 410 ||
    (input.lastHttpStatus === null &&
      input.lastError !== null &&
      input.lastError.toLowerCase().includes(UNRESOLVED_HOSTNAME_REASON));
  if (!hardStatus) {
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

async function countDistinctCandidateWebsiteUrls(pool: Pool): Promise<number> {
  const result = await pool.query<{ count: string }>(
    `
      SELECT COUNT(*)::text AS count
      FROM (
        SELECT DISTINCT official_website_url
        FROM public.candidates
        WHERE official_website_url IS NOT NULL
          AND length(trim(official_website_url)) > 0
          AND deleted_at IS NULL
          AND merged_into_candidate_id IS NULL
      ) AS distinct_urls
    `
  );
  return Number.parseInt(result.rows[0]?.count ?? "0", 10);
}

// Freshness is judged on the SHARED last_checked_at, so a URL that is also an
// election seed (315 overlap live) counts as fresh after a seed-producer check
// too — which skips this sweep's report-only extras (off-domain redirect, root
// probe) for up to staleAfterDays. Accepted: the seed scheduler is disabled
// everywhere today, and a per-consumer freshness marker would need a schema
// change. Lower CANDIDATE_WEBSITE_HEALTH_STALE_AFTER_DAYS to force earlier
// re-checks if that ever changes.
async function listUrlsDueForHealthCheck(
  pool: Pool,
  input: { asOfTimestamp: string; staleAfterDays: number; limit: number }
): Promise<UrlHealthRow[]> {
  const result = await pool.query<UrlHealthRow>(
    `
      WITH distinct_urls AS (
        SELECT DISTINCT official_website_url AS url
        FROM public.candidates
        WHERE official_website_url IS NOT NULL
          AND length(trim(official_website_url)) > 0
          AND deleted_at IS NULL
          AND merged_into_candidate_id IS NULL
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
      -- Monotonic guard: checks run for minutes between read and write, so a
      -- slower concurrent run (either producer — the table is shared) could
      -- land its older observation after a newer one. Newest check wins.
      WHERE source_url_health.last_checked_at IS NULL
         OR source_url_health.last_checked_at <= EXCLUDED.last_checked_at
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

async function checkUrlsWithConcurrency(
  rows: UrlHealthRow[],
  input: { timeoutMs: number; concurrency: number }
): Promise<{
  checkedRows: CheckedUrlState[];
  healthy: number;
  hardFail: number;
  transientFail: number;
  failedCount: number;
  offDomainRedirects: OffDomainRedirect[];
  hardFailUrls: string[];
  deepLinkRootAlive: DeepLinkRootAlive[];
}> {
  const workerCount = Math.max(1, Math.min(input.concurrency, rows.length));
  const checkedRows: CheckedUrlState[] = [];
  const offDomainRedirects: OffDomainRedirect[] = [];
  const hardFailUrls: string[] = [];
  const deepLinkRootAlive: DeepLinkRootAlive[] = [];
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
          // 403 stays non-fatal: campaign sites sit behind the same WAFs the
          // seed-URL checker tolerates, and a bot-blocked site is
          // user-reachable in a real browser.
          allowStatusCodes: [403],
        });
        const classification = classifyCandidateWebsiteCheckResult(reachability);
        if (classification.outcome === "healthy") {
          healthy += 1;
          if (reachability.ok && isOffDomainRedirect(row.url, reachability.finalUrl)) {
            offDomainRedirects.push({ url: row.url, finalUrl: reachability.finalUrl });
          }
        } else if (classification.outcome === "hard_fail") {
          hardFail += 1;
          hardFailUrls.push(row.url);
          // Report-only repair hint: a dead subpage on a live site is fixed by
          // trimming to the root, which is a one-line correction instead of a
          // research task. Never applied automatically — a live root proves the
          // domain answers, not that it is still this candidate's site.
          const rootUrl = rootUrlOfDeepLink(row.url);
          if (rootUrl) {
            const rootCheck = await verifyHttpUrlReachability(rootUrl, {
              timeoutMs: input.timeoutMs,
              allowStatusCodes: [403],
            });
            if (rootCheck.ok) {
              deepLinkRootAlive.push({ url: row.url, rootUrl });
            }
          }
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
    offDomainRedirects,
    hardFailUrls,
    deepLinkRootAlive,
  };
}

type RetireCandidateRow = {
  id: string;
  display_name: string | null;
  official_website_url: string;
};

async function listRetireEligibleCandidates(
  pool: Pool,
  input: {
    hardFailureThreshold: number;
    hardFailureWindowDays: number;
    asOfTimestamp: string;
    limit: number;
  }
): Promise<RetireCandidateRow[]> {
  const result = await pool.query<RetireCandidateRow>(
    `
      SELECT c.id, c.display_name, c.official_website_url
      FROM public.candidates AS c
      JOIN public.source_url_health AS h
        ON h.url = c.official_website_url
      WHERE c.official_website_url IS NOT NULL
        AND c.deleted_at IS NULL
        AND c.merged_into_candidate_id IS NULL
        AND h.consecutive_hard_failures >= $1::int
        AND h.first_hard_failed_at IS NOT NULL
        AND h.first_hard_failed_at <= ($2::timestamptz - make_interval(days => $3::int))
        AND (
          h.last_http_status IN (404, 410)
          OR (
            h.last_http_status IS NULL
            AND h.last_error IS NOT NULL
            AND lower(h.last_error) LIKE '%could not be resolved%'
          )
        )
      ORDER BY h.first_hard_failed_at ASC, c.id ASC
      LIMIT $4::int
    `,
    [input.hardFailureThreshold, input.asOfTimestamp, input.hardFailureWindowDays, input.limit]
  );
  return result.rows;
}

function parseJsonStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry): entry is string => typeof entry === "string");
}

/**
 * A dead URL was still genuinely the candidate's site, so it is ARCHIVED into
 * former_website_urls (where it keeps matching as a hard identifier), not
 * dropped — dropping is reserved for URLs that were wrong all along
 * (--replace/--clear on the profile writers). Dedupe key matches the rotation
 * code: the normalized URL.
 */
export function buildFormerWebsitesAfterRetire(input: {
  retiredUrl: string;
  storedFormerWebsites: readonly string[];
}): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const url of [...input.storedFormerWebsites, input.retiredUrl]) {
    const trimmed = url.trim();
    if (!trimmed) {
      continue;
    }
    const key = normalizeOptionalUrl(trimmed) ?? trimmed.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(trimmed);
  }
  return result;
}

async function retireCandidateWebsite(
  client: PoolClient,
  candidateId: string,
  expectedUrl: string,
  policy: { hardFailureThreshold: number; hardFailureWindowDays: number; asOf: Date }
): Promise<RetiredWebsite | null> {
  const locked = await client.query<{
    id: string;
    display_name: string | null;
    official_website_url: string | null;
    former_website_urls: unknown;
  }>(
    `
      SELECT id, display_name, official_website_url, former_website_urls
      FROM public.candidates
      WHERE id = $1::uuid
      FOR UPDATE
    `,
    [candidateId]
  );
  const row = locked.rows[0];
  // Eligibility was computed against expectedUrl OUTSIDE this transaction. If
  // a profile writer replaced the URL in the gap (the repair this sweep exists
  // to provoke), the stored value is now a live site whose health was never
  // judged — retiring it would archive the fix. Skip unless the row still
  // holds the exact URL whose failure streak earned retirement.
  if (!row || row.official_website_url !== expectedUrl) {
    return null;
  }
  // Eligibility was read OUTSIDE this transaction, and the URL matching above
  // only proves nobody swapped the link — not that it is still failing. The
  // seed-URL producer writes to the same source_url_health rows (315 URLs
  // overlap live), so a check between selection and this lock can have found
  // the site healthy again. Re-read the health row under lock and re-run the
  // full eligibility predicate before touching the candidate.
  const health = await client.query<{
    consecutive_hard_failures: number;
    first_hard_failed_at: Date | null;
    last_http_status: number | null;
    last_error: string | null;
  }>(
    `
      SELECT consecutive_hard_failures, first_hard_failed_at, last_http_status, last_error
      FROM public.source_url_health
      WHERE url = $1::text
      FOR UPDATE
    `,
    [expectedUrl]
  );
  const healthRow = health.rows[0];
  if (
    !healthRow ||
    !isRetireEligible({
      consecutiveHardFailures: healthRow.consecutive_hard_failures,
      firstHardFailedAt: healthRow.first_hard_failed_at,
      lastHttpStatus: healthRow.last_http_status,
      lastError: healthRow.last_error,
      hardFailureThreshold: policy.hardFailureThreshold,
      hardFailureWindowDays: policy.hardFailureWindowDays,
      asOf: policy.asOf,
    })
  ) {
    return null;
  }
  const formerWebsites = buildFormerWebsitesAfterRetire({
    retiredUrl: row.official_website_url,
    storedFormerWebsites: parseJsonStringArray(row.former_website_urls),
  });
  await client.query(
    `
      UPDATE public.candidates
      SET official_website_url = NULL,
          former_website_urls = $2::jsonb
      WHERE id = $1::uuid
    `,
    [candidateId, JSON.stringify(formerWebsites)]
  );
  return {
    candidateId: row.id,
    displayName: row.display_name,
    url: row.official_website_url,
  };
}

export async function runCandidateWebsiteHealthProducer(
  options: ProducerOptions = {}
): Promise<CandidateWebsiteHealthProducerResult> {
  const { dryRun = false, force = false } = options;
  const policy = readCandidateWebsiteHealthPolicyFromEnv();
  const enabled = force || policy.enabled;
  const maxUrlsPerRun = options.maxUrlsOverride ?? policy.maxUrlsPerRun;

  const baseResult: CandidateWebsiteHealthProducerResult = {
    enabled,
    retireEnabled: policy.retireEnabled,
    dryRun,
    force,
    asOfTimestamp: policy.asOfTimestamp,
    staleAfterDays: policy.staleAfterDays,
    maxUrlsPerRun,
    hardFailureThreshold: policy.hardFailureThreshold,
    hardFailureWindowDays: policy.hardFailureWindowDays,
    urls_total: 0,
    urls_due_check: 0,
    checked_count: 0,
    healthy_count: 0,
    hard_fail_count: 0,
    transient_fail_count: 0,
    failed_count: 0,
    off_domain_redirects: [],
    hard_fail_urls: [],
    deep_link_root_alive: [],
    retire_candidate_count: 0,
    retired: [],
  };

  if (!enabled) {
    console.log(
      `candidate_website_health producer skipped: disabled by flag (as_of=${policy.asOfTimestamp})`
    );
    return baseResult;
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  try {
    const [urlsTotal, dueRows] = await Promise.all([
      countDistinctCandidateWebsiteUrls(pool),
      listUrlsDueForHealthCheck(pool, {
        asOfTimestamp: policy.asOfTimestamp,
        staleAfterDays: policy.staleAfterDays,
        limit: maxUrlsPerRun,
      }),
    ]);

    baseResult.urls_total = urlsTotal;
    baseResult.urls_due_check = dueRows.length;

    const {
      checkedRows,
      healthy,
      hardFail,
      transientFail,
      failedCount,
      offDomainRedirects,
      hardFailUrls,
      deepLinkRootAlive,
    } = await checkUrlsWithConcurrency(dueRows, {
      timeoutMs: policy.timeoutMs,
      concurrency: policy.concurrency,
    });

    if (!dryRun) {
      await upsertCheckedUrlStates(pool, checkedRows);
    }

    baseResult.checked_count = checkedRows.length;
    baseResult.healthy_count = healthy;
    baseResult.hard_fail_count = hardFail;
    baseResult.transient_fail_count = transientFail;
    baseResult.failed_count = failedCount;
    baseResult.off_domain_redirects = offDomainRedirects;
    baseResult.hard_fail_urls = hardFailUrls;
    baseResult.deep_link_root_alive = deepLinkRootAlive;

    const retireCandidates = await listRetireEligibleCandidates(pool, {
      hardFailureThreshold: policy.hardFailureThreshold,
      hardFailureWindowDays: policy.hardFailureWindowDays,
      asOfTimestamp: policy.asOfTimestamp,
      limit: policy.maxRetireUrlsPerRun,
    });
    baseResult.retire_candidate_count = retireCandidates.length;

    if (policy.retireEnabled && !dryRun && retireCandidates.length > 0) {
      const client = await pool.connect();
      try {
        for (const candidate of retireCandidates) {
          await client.query("BEGIN");
          try {
            const retired = await retireCandidateWebsite(
              client,
              candidate.id,
              candidate.official_website_url,
              {
                hardFailureThreshold: policy.hardFailureThreshold,
                hardFailureWindowDays: policy.hardFailureWindowDays,
                asOf: new Date(policy.asOfTimestamp),
              }
            );
            await client.query("COMMIT");
            if (retired) {
              baseResult.retired.push(retired);
              console.log(
                `candidate_website_health retired dead website candidate=${retired.candidateId} url=${retired.url}`
              );
            }
          } catch (error) {
            await client.query("ROLLBACK");
            throw error;
          }
        }
      } finally {
        client.release();
      }
    }

    return baseResult;
  } finally {
    await pool.end();
  }
}
