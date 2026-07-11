import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";

// Phase 0 connectivity probe for New York campaign finance (plan-new-york-finance.md).
//
// The NYSBOE Public Reporting hosts block backend clients behind a Cloudflare
// challenge, so New York must be built on the official NY Open Data (Socrata)
// mirrors instead. This probe makes no database writes; it only proves that the
// deployed backend can read the two datasets the future module needs, with the
// query shapes the module will use (stable order, bounded paging, retry), and
// that one known Independent Expenditure Committee forms a usable chain:
// registry row -> its own Schedule R allocation -> the same filer's parent
// Schedule F expenditure -> its cycle-scoped funder receipts.
export const NEW_YORK_SODA_BASE_URL = "https://data.ny.gov/resource";
export const NEW_YORK_SODA_DISCLOSURES_DATASET = "e9ss-239a";
export const NEW_YORK_SODA_FILERS_DATASET = "7x2g-h32p";

const DEFAULT_ELECTION_YEAR = 2026;
const DEFAULT_PAGE_LIMIT = 100;
const DEFAULT_MAX_PAGES = 3;
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_RETRY_BASE_DELAY_MS = 500;
// Citizens for Affordable Rates PAC: a registered Independent Expenditure
// Committee with 2026 Schedule R activity, verified 2026-07-11.
const DEFAULT_KNOWN_IE_FILER_ID = "590891";
const INDEPENDENT_EXPENDITURE_COMMITTEE_TYPE = "Independent Expenditure Committee";
const RECEIPT_SCHEDULES_SOQL = "('A','B','C','D')";

const KNOWN_FLAGS = new Set([
  "--year",
  "--page-limit",
  "--max-pages",
  "--timeout-ms",
  "--max-attempts",
  "--known-ie-filer-id",
  "--app-token",
]);

export type NewYorkSodaConnectivityProbeArgs = {
  electionYear: number;
  pageLimit: number;
  maxPages: number;
  timeoutMs: number;
  maxAttempts: number;
  knownIeFilerId: string;
  appToken: string | null;
};

export type NewYorkSodaConnectivityProbeCheck = {
  name: "filer_lookup" | "schedule_r_paging" | "parent_expenditure_mapping" | "ie_group_funders";
  ok: boolean;
  url: string;
  status: number | null;
  attempts: number;
  latency_ms: number;
  row_count: number | null;
  etag: string | null;
  last_modified: string | null;
  detail: string;
};

export type NewYorkSodaConnectivityProbeOutput = {
  type: "new_york_soda_connectivity_probe";
  ts: string;
  args: Omit<NewYorkSodaConnectivityProbeArgs, "appToken"> & { appTokenProvided: boolean };
  ok: boolean;
  checks: NewYorkSodaConnectivityProbeCheck[];
};

type FetchJsonResult = {
  payload: unknown;
  status: number;
  attempts: number;
  latencyMs: number;
  etag: string | null;
  lastModified: string | null;
};

type ProbeRuntime = {
  fetchImpl: typeof fetch;
  sleep: (ms: number) => Promise<void>;
  now: () => number;
};

// Carries the structured request telemetry into failed checks so the probe
// output stays diagnostic (status/attempts/latency) even when a request fails.
class NewYorkSodaRequestError extends Error {
  constructor(
    message: string,
    public readonly status: number | null,
    public readonly attempts: number,
    public readonly latencyMs: number
  ) {
    super(message);
    this.name = "NewYorkSodaRequestError";
  }
}

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(next.trim());
      index += 1;
    }
  }

  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0] ?? null;
}

// This probe gates the New York build decision, so a typo like --yeer must
// fail loudly instead of silently probing the defaults.
function assertKnownFlags(args: readonly string[]): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected positional argument: ${arg}`);
    }
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!KNOWN_FLAGS.has(name)) {
      throw new Error(`Unknown flag: ${name}`);
    }
    if (!arg.includes("=")) {
      index += 1;
    }
  }
}

function parseBoundedIntegerFlag(
  args: readonly string[],
  name: string,
  fallback: number,
  min: number,
  max: number
): number {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`Out-of-range ${name} value: ${raw} (expected ${min}-${max})`);
  }
  return value;
}

export function parseProbeNewYorkSodaConnectivityArgs(args: readonly string[]): NewYorkSodaConnectivityProbeArgs {
  assertKnownFlags(args);
  const knownIeFilerId = parseFlagValue(args, "--known-ie-filer-id") ?? DEFAULT_KNOWN_IE_FILER_ID;
  if (!/^\d{1,12}$/.test(knownIeFilerId)) {
    throw new Error(`Invalid --known-ie-filer-id value: ${knownIeFilerId}`);
  }
  return {
    electionYear: parseBoundedIntegerFlag(args, "--year", DEFAULT_ELECTION_YEAR, 2000, 2100),
    pageLimit: parseBoundedIntegerFlag(args, "--page-limit", DEFAULT_PAGE_LIMIT, 1, 50_000),
    maxPages: parseBoundedIntegerFlag(args, "--max-pages", DEFAULT_MAX_PAGES, 1, 100),
    timeoutMs: parseBoundedIntegerFlag(args, "--timeout-ms", DEFAULT_TIMEOUT_MS, 1, 600_000),
    maxAttempts: parseBoundedIntegerFlag(args, "--max-attempts", DEFAULT_MAX_ATTEMPTS, 1, 10),
    knownIeFilerId,
    appToken: parseFlagValue(args, "--app-token") ?? (process.env.NEW_YORK_SODA_APP_TOKEN?.trim() || null),
  };
}

export function buildNewYorkSodaUrl(datasetId: string, params: Record<string, string>): string {
  if (!/^[a-z0-9]{4}-[a-z0-9]{4}$/i.test(datasetId)) {
    throw new Error(`Invalid New York SODA dataset ID: ${datasetId}`);
  }
  const url = new URL(`${NEW_YORK_SODA_BASE_URL}/${datasetId}.json`);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }
  return url.toString();
}

// SoQL string literals escape single quotes by doubling them. Values pulled
// from API responses must go through this before entering a $where clause.
export function soqlStringLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function isRetryableStatus(status: number): boolean {
  return status === 429 || status >= 500;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

async function fetchNewYorkSodaJson(
  url: string,
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime
): Promise<FetchJsonResult> {
  const startedAt = runtime.now();
  let lastFailure = "";
  let lastStatus: number | null = null;

  for (let attempt = 1; attempt <= args.maxAttempts; attempt += 1) {
    if (attempt > 1) {
      await runtime.sleep(DEFAULT_RETRY_BASE_DELAY_MS * 2 ** (attempt - 2));
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), args.timeoutMs);
    const headers = new Headers({ accept: "application/json" });
    if (args.appToken) {
      headers.set("X-App-Token", args.appToken);
    }

    let response: Response;
    try {
      response = await runtime.fetchImpl(url, { headers, signal: controller.signal });
    } catch (error) {
      lastStatus = null;
      lastFailure = isAbortError(error)
        ? `timed out after ${args.timeoutMs}ms`
        : error instanceof Error
          ? error.message
          : String(error);
      continue;
    } finally {
      clearTimeout(timeout);
    }

    lastStatus = response.status;
    if (isRetryableStatus(response.status)) {
      lastFailure = `HTTP ${response.status}`;
      continue;
    }
    if (!response.ok) {
      throw new NewYorkSodaRequestError(
        `New York SODA request failed: HTTP ${response.status} for ${url}`,
        response.status,
        attempt,
        runtime.now() - startedAt
      );
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch (error) {
      throw new NewYorkSodaRequestError(
        `New York SODA response was not valid JSON for ${url}: ${error instanceof Error ? error.message : String(error)}`,
        response.status,
        attempt,
        runtime.now() - startedAt
      );
    }
    return {
      payload,
      status: response.status,
      attempts: attempt,
      latencyMs: runtime.now() - startedAt,
      etag: response.headers.get("etag"),
      lastModified: response.headers.get("last-modified"),
    };
  }

  throw new NewYorkSodaRequestError(
    `New York SODA request failed after ${args.maxAttempts} attempts (${lastFailure}) for ${url}`,
    lastStatus,
    args.maxAttempts,
    runtime.now() - startedAt
  );
}

function asRowArray(payload: unknown, url: string): Record<string, unknown>[] {
  if (!Array.isArray(payload)) {
    throw new NewYorkSodaRequestError(`New York SODA response is missing the result array for ${url}`, null, 1, 0);
  }
  return payload as Record<string, unknown>[];
}

function rowString(row: Record<string, unknown>, key: string): string {
  const value = row[key];
  return typeof value === "string" ? value.trim() : "";
}

function failedCheck(
  name: NewYorkSodaConnectivityProbeCheck["name"],
  url: string,
  error: unknown
): NewYorkSodaConnectivityProbeCheck {
  const requestError = error instanceof NewYorkSodaRequestError ? error : null;
  return {
    name,
    ok: false,
    url,
    status: requestError?.status ?? null,
    attempts: requestError?.attempts ?? 0,
    latency_ms: requestError?.latencyMs ?? 0,
    row_count: null,
    etag: null,
    last_modified: null,
    detail: error instanceof Error ? error.message : String(error),
  };
}

async function runFilerLookupCheck(
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime
): Promise<NewYorkSodaConnectivityProbeCheck> {
  const url = buildNewYorkSodaUrl(NEW_YORK_SODA_FILERS_DATASET, {
    $where: `filer_id=${soqlStringLiteral(args.knownIeFilerId)}`,
    $limit: "2",
  });
  try {
    const result = await fetchNewYorkSodaJson(url, args, runtime);
    const rows = asRowArray(result.payload, url);
    const committeeType = rows.length === 1 ? rowString(rows[0], "committee_type_desc") : "";
    const ok = rows.length === 1 && committeeType === INDEPENDENT_EXPENDITURE_COMMITTEE_TYPE;
    return {
      name: "filer_lookup",
      ok,
      url,
      status: result.status,
      attempts: result.attempts,
      latency_ms: result.latencyMs,
      row_count: rows.length,
      etag: result.etag,
      last_modified: result.lastModified,
      detail: ok
        ? `filer ${args.knownIeFilerId} is a registered ${INDEPENDENT_EXPENDITURE_COMMITTEE_TYPE}`
        : `expected exactly one ${INDEPENDENT_EXPENDITURE_COMMITTEE_TYPE} row for filer ${args.knownIeFilerId}; got ${rows.length} row(s) with committee type ${committeeType || "(none)"}`,
    };
  } catch (error) {
    return failedCheck("filer_lookup", url, error);
  }
}

async function runScheduleRPagingCheck(
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime
): Promise<NewYorkSodaConnectivityProbeCheck> {
  const where = `filing_sched_abbrev='R' AND election_year_r='${args.electionYear}' AND r_support_oppose IS NOT NULL`;
  const firstPageUrl = buildNewYorkSodaUrl(NEW_YORK_SODA_DISCLOSURES_DATASET, {
    $where: where,
    $order: "filing_trans_id",
    $limit: String(args.pageLimit),
    $offset: "0",
  });

  try {
    const seenFilingTransIds = new Set<string>();
    let duplicateFilingTransId: string | null = null;
    let invalidSupportOppose: string | null = null;
    let rowCount = 0;
    let status: number | null = null;
    let attempts = 0;
    let latencyMs = 0;
    let etag: string | null = null;
    let lastModified: string | null = null;

    for (let pageIndex = 0; pageIndex < args.maxPages; pageIndex += 1) {
      const url = buildNewYorkSodaUrl(NEW_YORK_SODA_DISCLOSURES_DATASET, {
        $where: where,
        $order: "filing_trans_id",
        $limit: String(args.pageLimit),
        $offset: String(pageIndex * args.pageLimit),
      });
      const result = await fetchNewYorkSodaJson(url, args, runtime);
      const rows = asRowArray(result.payload, url);
      status = result.status;
      attempts += result.attempts;
      latencyMs += result.latencyMs;
      etag ??= result.etag;
      lastModified ??= result.lastModified;
      rowCount += rows.length;

      for (const row of rows) {
        const filingTransId = rowString(row, "filing_trans_id");
        if (filingTransId) {
          if (seenFilingTransIds.has(filingTransId)) {
            duplicateFilingTransId ??= filingTransId;
          }
          seenFilingTransIds.add(filingTransId);
        }
        const supportOppose = rowString(row, "r_support_oppose");
        if (supportOppose !== "S" && supportOppose !== "O") {
          invalidSupportOppose ??= supportOppose || "(empty)";
        }
      }

      if (rows.length < args.pageLimit) {
        break;
      }
    }

    const problems: string[] = [];
    if (rowCount === 0) {
      problems.push(`no Schedule R rows with explicit support/oppose for ${args.electionYear}`);
    }
    if (duplicateFilingTransId) {
      problems.push(`duplicate filing_trans_id ${duplicateFilingTransId} across pages`);
    }
    if (invalidSupportOppose) {
      problems.push(`unexpected r_support_oppose value ${invalidSupportOppose}`);
    }

    return {
      name: "schedule_r_paging",
      ok: problems.length === 0,
      url: firstPageUrl,
      status,
      attempts,
      latency_ms: latencyMs,
      row_count: rowCount,
      etag,
      last_modified: lastModified,
      detail:
        problems.length === 0
          ? `paged ${rowCount} explicit support/oppose Schedule R row(s) for ${args.electionYear} with stable ordering`
          : problems.join("; "),
    };
  } catch (error) {
    return failedCheck("schedule_r_paging", firstPageUrl, error);
  }
}

const PARENT_MAPPING_SAMPLE_SIZE = 5;

async function runParentExpenditureMappingCheck(
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime
): Promise<NewYorkSodaConnectivityProbeCheck> {
  // Prove the chain for the known IE committee itself, not for whichever
  // filer happens to appear first in the global Schedule R pages. Some real
  // trans_mapping values point at expenditures that are absent from the
  // dataset (amended/superseded filings; 5 of 47 for the default committee on
  // 2026-07-11), so sample several mappings and require at least one clean
  // resolution to exactly one same-filer Schedule F row. Phase 1 skips
  // unresolvable rows by rule; the probe reports the observed ratio.
  const scheduleRUrl = buildNewYorkSodaUrl(NEW_YORK_SODA_DISCLOSURES_DATASET, {
    $where: `filer_id=${soqlStringLiteral(args.knownIeFilerId)} AND filing_sched_abbrev='R' AND trans_mapping IS NOT NULL`,
    $order: "filing_trans_id",
    $limit: String(PARENT_MAPPING_SAMPLE_SIZE),
  });
  try {
    const scheduleRResult = await fetchNewYorkSodaJson(scheduleRUrl, args, runtime);
    const scheduleRRows = asRowArray(scheduleRResult.payload, scheduleRUrl);
    const transMappings = [
      ...new Set(scheduleRRows.map((row) => rowString(row, "trans_mapping")).filter((value) => value.length > 0)),
    ];
    if (transMappings.length === 0) {
      return {
        name: "parent_expenditure_mapping",
        ok: false,
        url: scheduleRUrl,
        status: scheduleRResult.status,
        attempts: scheduleRResult.attempts,
        latency_ms: scheduleRResult.latencyMs,
        row_count: scheduleRRows.length,
        etag: scheduleRResult.etag,
        last_modified: scheduleRResult.lastModified,
        detail: `filer ${args.knownIeFilerId} has no Schedule R row with trans_mapping`,
      };
    }

    let attempts = scheduleRResult.attempts;
    let latencyMs = scheduleRResult.latencyMs;
    let status = scheduleRResult.status;
    let etag = scheduleRResult.etag;
    let lastModified = scheduleRResult.lastModified;
    let lastUrl = scheduleRUrl;
    let resolvedCount = 0;
    const unresolvedDetails: string[] = [];

    for (const transMapping of transMappings) {
      const parentUrl = buildNewYorkSodaUrl(NEW_YORK_SODA_DISCLOSURES_DATASET, {
        $where: `filer_id=${soqlStringLiteral(args.knownIeFilerId)} AND trans_number=${soqlStringLiteral(transMapping)}`,
        $limit: "5",
      });
      const parentResult = await fetchNewYorkSodaJson(parentUrl, args, runtime);
      const parentRows = asRowArray(parentResult.payload, parentUrl);
      const schedules = parentRows.map((row) => rowString(row, "filing_sched_abbrev"));
      attempts += parentResult.attempts;
      latencyMs += parentResult.latencyMs;
      status = parentResult.status;
      etag ??= parentResult.etag;
      lastModified ??= parentResult.lastModified;
      lastUrl = parentUrl;
      if (parentRows.length === 1 && schedules[0] === "F") {
        resolvedCount += 1;
      } else {
        unresolvedDetails.push(
          `${transMapping} -> ${parentRows.length} row(s) on schedule(s) ${schedules.join(", ") || "(none)"}`
        );
      }
    }

    const ok = resolvedCount > 0;
    const summary = `${resolvedCount} of ${transMappings.length} sampled trans_mapping value(s) resolved to exactly one same-filer Schedule F expenditure for filer ${args.knownIeFilerId}`;
    return {
      name: "parent_expenditure_mapping",
      ok,
      url: lastUrl,
      status,
      attempts,
      latency_ms: latencyMs,
      row_count: resolvedCount,
      etag,
      last_modified: lastModified,
      detail: unresolvedDetails.length === 0 ? summary : `${summary}; unresolved: ${unresolvedDetails.join("; ")}`,
    };
  } catch (error) {
    return failedCheck("parent_expenditure_mapping", scheduleRUrl, error);
  }
}

async function runIeGroupFundersCheck(
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime
): Promise<NewYorkSodaConnectivityProbeCheck> {
  // Fetch the fields the future funder->industry backtrace will read, scoped
  // to the probed election year so stale cycles cannot produce a green check.
  const url = buildNewYorkSodaUrl(NEW_YORK_SODA_DISCLOSURES_DATASET, {
    $select: "flng_ent_name,cntrbr_type_desc,org_amt,election_year",
    $where: `filer_id=${soqlStringLiteral(args.knownIeFilerId)} AND filing_sched_abbrev IN ${RECEIPT_SCHEDULES_SOQL} AND election_year='${args.electionYear}'`,
    $order: "filing_trans_id",
    $limit: "5",
  });
  try {
    const result = await fetchNewYorkSodaJson(url, args, runtime);
    const rows = asRowArray(result.payload, url);
    const namedRows = rows.filter((row) => rowString(row, "flng_ent_name").length > 0 && rowString(row, "org_amt").length > 0);
    const ok = rows.length > 0 && namedRows.length === rows.length;
    return {
      name: "ie_group_funders",
      ok,
      url,
      status: result.status,
      attempts: result.attempts,
      latency_ms: result.latencyMs,
      row_count: rows.length,
      etag: result.etag,
      last_modified: result.lastModified,
      detail: ok
        ? `filer ${args.knownIeFilerId} has itemized ${args.electionYear} receipts with funder name and amount fields (sample: ${rowString(namedRows[0], "flng_ent_name")})`
        : rows.length === 0
          ? `filer ${args.knownIeFilerId} has no itemized schedule A-D receipts for election year ${args.electionYear}`
          : `expected funder name and amount on every sampled receipt row; ${rows.length - namedRows.length} of ${rows.length} row(s) were missing them`,
    };
  } catch (error) {
    return failedCheck("ie_group_funders", url, error);
  }
}

export async function runProbeNewYorkSodaConnectivity(input: {
  args: NewYorkSodaConnectivityProbeArgs;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  now?: Date;
}): Promise<NewYorkSodaConnectivityProbeOutput> {
  const runtime: ProbeRuntime = {
    fetchImpl: input.fetchImpl ?? fetch,
    sleep: input.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))),
    now: () => Date.now(),
  };

  const checks = [
    await runFilerLookupCheck(input.args, runtime),
    await runScheduleRPagingCheck(input.args, runtime),
    await runParentExpenditureMappingCheck(input.args, runtime),
    await runIeGroupFundersCheck(input.args, runtime),
  ];
  const { appToken, ...safeArgs } = input.args;
  return {
    type: "new_york_soda_connectivity_probe",
    ts: (input.now ?? new Date()).toISOString(),
    args: { ...safeArgs, appTokenProvided: appToken !== null },
    ok: checks.every((check) => check.ok),
    checks,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const output = await runProbeNewYorkSodaConnectivity({
    args: parseProbeNewYorkSodaConnectivityArgs(process.argv.slice(2)),
  });
  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("New York SODA connectivity probe failed:", message);
    process.exitCode = 1;
  });
}
