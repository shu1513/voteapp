import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";

// Phase 0 connectivity probe for New York campaign finance (plan-new-york-finance.md).
//
// The NYSBOE Public Reporting hosts block backend clients behind a Cloudflare
// challenge, so New York must be built on the official NY Open Data (Socrata)
// mirrors instead. This probe makes no database writes; it only proves that the
// deployed backend can read the two datasets the future module needs, with the
// query shapes the module will use (stable order, bounded paging, retry).
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

function parsePositiveIntegerFlag(args: readonly string[], name: string, fallback: number): number {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

export function parseProbeNewYorkSodaConnectivityArgs(args: readonly string[]): NewYorkSodaConnectivityProbeArgs {
  const knownIeFilerId = parseFlagValue(args, "--known-ie-filer-id") ?? DEFAULT_KNOWN_IE_FILER_ID;
  if (!/^\d+$/.test(knownIeFilerId)) {
    throw new Error(`Invalid --known-ie-filer-id value: ${knownIeFilerId}`);
  }
  return {
    electionYear: parsePositiveIntegerFlag(args, "--year", DEFAULT_ELECTION_YEAR),
    pageLimit: parsePositiveIntegerFlag(args, "--page-limit", DEFAULT_PAGE_LIMIT),
    maxPages: parsePositiveIntegerFlag(args, "--max-pages", DEFAULT_MAX_PAGES),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms", DEFAULT_TIMEOUT_MS),
    maxAttempts: parsePositiveIntegerFlag(args, "--max-attempts", DEFAULT_MAX_ATTEMPTS),
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
      lastFailure = isAbortError(error)
        ? `timed out after ${args.timeoutMs}ms`
        : error instanceof Error
          ? error.message
          : String(error);
      continue;
    } finally {
      clearTimeout(timeout);
    }

    if (isRetryableStatus(response.status)) {
      lastFailure = `HTTP ${response.status}`;
      continue;
    }
    if (!response.ok) {
      throw new Error(`New York SODA request failed: HTTP ${response.status} for ${url}`);
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch (error) {
      throw new Error(
        `New York SODA response was not valid JSON for ${url}: ${error instanceof Error ? error.message : String(error)}`
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

  throw new Error(`New York SODA request failed after ${args.maxAttempts} attempts (${lastFailure}) for ${url}`);
}

function asRowArray(payload: unknown, url: string): Record<string, unknown>[] {
  if (!Array.isArray(payload)) {
    throw new Error(`New York SODA response is missing the result array for ${url}`);
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
  return {
    name,
    ok: false,
    url,
    status: null,
    attempts: 0,
    latency_ms: 0,
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
    $where: `filer_id='${args.knownIeFilerId}'`,
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

type ScheduleRPagingResult = {
  check: NewYorkSodaConnectivityProbeCheck;
  mappingRow: { filerId: string; transMapping: string } | null;
};

async function runScheduleRPagingCheck(
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime
): Promise<ScheduleRPagingResult> {
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
    let mappingRow: ScheduleRPagingResult["mappingRow"] = null;
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
        const transMapping = rowString(row, "trans_mapping");
        const filerId = rowString(row, "filer_id");
        if (!mappingRow && transMapping && filerId) {
          mappingRow = { filerId, transMapping };
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
      check: {
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
      },
      mappingRow,
    };
  } catch (error) {
    return { check: failedCheck("schedule_r_paging", firstPageUrl, error), mappingRow: null };
  }
}

async function runParentExpenditureMappingCheck(
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime,
  mappingRow: ScheduleRPagingResult["mappingRow"]
): Promise<NewYorkSodaConnectivityProbeCheck> {
  if (!mappingRow) {
    return {
      name: "parent_expenditure_mapping",
      ok: false,
      url: "",
      status: null,
      attempts: 0,
      latency_ms: 0,
      row_count: null,
      etag: null,
      last_modified: null,
      detail: "no Schedule R row with trans_mapping was available from the paging check",
    };
  }

  const url = buildNewYorkSodaUrl(NEW_YORK_SODA_DISCLOSURES_DATASET, {
    $where: `filer_id='${mappingRow.filerId}' AND trans_number='${mappingRow.transMapping}'`,
    $limit: "5",
  });
  try {
    const result = await fetchNewYorkSodaJson(url, args, runtime);
    const rows = asRowArray(result.payload, url);
    const schedules = rows.map((row) => rowString(row, "filing_sched_abbrev"));
    const ok = rows.length === 1 && schedules[0] === "F";
    return {
      name: "parent_expenditure_mapping",
      ok,
      url,
      status: result.status,
      attempts: result.attempts,
      latency_ms: result.latencyMs,
      row_count: rows.length,
      etag: result.etag,
      last_modified: result.lastModified,
      detail: ok
        ? `trans_mapping resolved to exactly one same-filer Schedule F expenditure for filer ${mappingRow.filerId}`
        : `expected exactly one Schedule F row for filer ${mappingRow.filerId} trans_number ${mappingRow.transMapping}; got ${rows.length} row(s) on schedule(s) ${schedules.join(", ") || "(none)"}`,
    };
  } catch (error) {
    return failedCheck("parent_expenditure_mapping", url, error);
  }
}

async function runIeGroupFundersCheck(
  args: NewYorkSodaConnectivityProbeArgs,
  runtime: ProbeRuntime
): Promise<NewYorkSodaConnectivityProbeCheck> {
  const url = buildNewYorkSodaUrl(NEW_YORK_SODA_DISCLOSURES_DATASET, {
    $select: "count(*) AS receipt_count",
    $where: `filer_id='${args.knownIeFilerId}' AND filing_sched_abbrev IN ('A','B','C','D')`,
  });
  try {
    const result = await fetchNewYorkSodaJson(url, args, runtime);
    const rows = asRowArray(result.payload, url);
    const receiptCount = rows.length === 1 ? Number.parseInt(rowString(rows[0], "receipt_count"), 10) : Number.NaN;
    const ok = Number.isInteger(receiptCount) && receiptCount > 0;
    return {
      name: "ie_group_funders",
      ok,
      url,
      status: result.status,
      attempts: result.attempts,
      latency_ms: result.latencyMs,
      row_count: Number.isInteger(receiptCount) ? receiptCount : null,
      etag: result.etag,
      last_modified: result.lastModified,
      detail: ok
        ? `filer ${args.knownIeFilerId} has ${receiptCount} itemized receipt row(s) on schedules A-D`
        : `expected a positive receipt count for filer ${args.knownIeFilerId}; got ${rows.length === 1 ? rowString(rows[0], "receipt_count") || "(empty)" : `${rows.length} row(s)`}`,
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

  const filerLookup = await runFilerLookupCheck(input.args, runtime);
  const scheduleRPaging = await runScheduleRPagingCheck(input.args, runtime);
  const parentMapping = await runParentExpenditureMappingCheck(input.args, runtime, scheduleRPaging.mappingRow);
  const ieGroupFunders = await runIeGroupFundersCheck(input.args, runtime);

  const checks = [filerLookup, scheduleRPaging.check, parentMapping, ieGroupFunders];
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
