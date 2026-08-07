import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isNorthCarolinaNcsbeRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  acquireNcsbeCycleArtifacts,
  type NcsbeAcquisitionCommittee,
} from "../pipeline/northCarolinaFinance/northCarolinaNcsbeArtifactAcquisition.js";
import { DEFAULT_NCSBE_CACHE_DIR } from "../pipeline/northCarolinaFinance/northCarolinaNcsbeArtifactCache.js";
import {
  createNcsbeTransport,
  requireNcsbeYear,
  DEFAULT_NCSBE_REQUEST_SPACING_MS,
  type NcsbeTransport,
} from "../pipeline/northCarolinaFinance/northCarolinaNcsbeClient.js";
import { NORTH_CAROLINA_SBOEID_PATTERN } from "../pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

// Downloads one cycle of NCSBE campaign-finance artifacts into the local
// cache (north_carolina_plan.md PR 4): per-committee document inventories and
// every structured disclosure report touching the Y−1..Y window, plus the
// statewide IE doc-type inventories and their structured reports. Plain paced
// HTTP — no browser, no session (the opposite of Ohio). Nothing here writes
// to the database; the finance sync reads the cache only.
//
//   npm run north-carolina-candidates:finance:raw:refresh -- \
//     --cycle-year=2026 --committee STA-JV516O-C-001:57190
//
// Committees are passed as <SBoEID>:<OrgGroupID> (both appear in the
// committee-search results). With no --committee, an IE-only run pulls just
// the IE inventories and reports.

export { DEFAULT_NCSBE_CACHE_DIR };

export type RefreshNorthCarolinaNcsbeRawDataScriptOptions = {
  cycleYear: number;
  cacheDir: string;
  committees: NcsbeAcquisitionCommittee[];
  includeIe: boolean;
  spacingMs: number;
  force: boolean;
  dryRun: boolean;
};

const VALUE_FLAG_NAMES = ["--cycle-year", "--year", "--cache-dir", "--committee", "--spacing-ms"] as const;
const BOOLEAN_FLAG_NAMES = ["--skip-ie", "--force", "--dry-run"] as const;

// Every token must be a known flag or a known flag's value. A silently
// ignored token here is dangerous: a misspelled --dry-run would start a real
// paced pull against the portal.
function assertKnownRefreshScriptArgs(args: readonly string[]): void {
  const valueFlagNames: readonly string[] = VALUE_FLAG_NAMES;
  const booleanFlagNames: readonly string[] = BOOLEAN_FLAG_NAMES;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (booleanFlagNames.includes(arg)) {
      continue;
    }
    if (booleanFlagNames.includes(name)) {
      throw new Error(`${name} does not take a value`);
    }
    if (valueFlagNames.includes(name)) {
      if (arg === name) {
        // The next token is this flag's value; readValueFlags validates it.
        index += 1;
      }
      continue;
    }
    throw new Error(`Unknown option: ${arg}`);
  }
}

function readValueFlags(args: readonly string[], name: string): string[] {
  const values: string[] = [];
  const inlinePrefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing value for ${name}`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing value for ${name}`);
      }
      values.push(next.trim());
      index += 1;
    }
  }
  return values;
}

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values = readValueFlags(args, name);
  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0];
}

function parsePositiveInteger(value: string | undefined, fallback: number, flagName: string): number {
  if (!value) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${flagName} value: ${value}`);
  }
  return Number(value);
}

export function parseNcsbeCommitteeArg(value: string): NcsbeAcquisitionCommittee {
  const separator = value.lastIndexOf(":");
  if (separator < 0) {
    throw new Error(`--committee must be <SBoEID>:<OrgGroupID>, got ${JSON.stringify(value)}`);
  }
  const sboeId = value.slice(0, separator).trim().toUpperCase();
  const orgGroupIdRaw = value.slice(separator + 1).trim();
  if (!NORTH_CAROLINA_SBOEID_PATTERN.test(sboeId)) {
    throw new Error(`--committee SBoEID does not match the pinned pattern: ${JSON.stringify(sboeId)}`);
  }
  if (!/^[1-9]\d*$/.test(orgGroupIdRaw)) {
    throw new Error(`--committee OrgGroupID must be a positive integer: ${JSON.stringify(orgGroupIdRaw)}`);
  }
  return { sboeId, orgGroupId: Number(orgGroupIdRaw) };
}

export function parseRefreshNorthCarolinaNcsbeRawDataScriptArgs(
  args: readonly string[]
): RefreshNorthCarolinaNcsbeRawDataScriptOptions {
  assertKnownRefreshScriptArgs(args);
  const cycleYearValues = readValueFlags(args, "--cycle-year");
  const yearValues = readValueFlags(args, "--year");
  if (cycleYearValues.length > 0 && yearValues.length > 0) {
    throw new Error("Provide --cycle-year or --year, not both");
  }
  const cycleYearRaw = readValueFlag(args, "--cycle-year") ?? readValueFlag(args, "--year");
  const cycleYear = requireNcsbeYear(
    parsePositiveInteger(cycleYearRaw, new Date().getUTCFullYear(), "--cycle-year")
  );
  const cacheDir = (
    readValueFlag(args, "--cache-dir")?.trim() ||
    process.env.NORTH_CAROLINA_NCSBE_RAW_DATA_CACHE_DIR?.trim() ||
    DEFAULT_NCSBE_CACHE_DIR
  ).trim();

  const committees = readValueFlags(args, "--committee").map(parseNcsbeCommitteeArg);
  const seen = new Set<string>();
  for (const committee of committees) {
    if (seen.has(committee.sboeId)) {
      throw new Error(`Duplicate --committee SBoEID: ${committee.sboeId}`);
    }
    seen.add(committee.sboeId);
  }

  const includeIe = !args.includes("--skip-ie");
  if (committees.length === 0 && !includeIe) {
    throw new Error("Nothing to fetch: pass --committee entries or drop --skip-ie");
  }

  return {
    cycleYear,
    cacheDir: resolve(cacheDir),
    committees,
    includeIe,
    spacingMs: parsePositiveInteger(
      readValueFlag(args, "--spacing-ms"),
      DEFAULT_NCSBE_REQUEST_SPACING_MS,
      "--spacing-ms"
    ),
    force: args.includes("--force"),
    dryRun: args.includes("--dry-run"),
  };
}

export async function runRefreshNorthCarolinaNcsbeRawDataScript(input: {
  options: RefreshNorthCarolinaNcsbeRawDataScriptOptions;
  // Test seam; a real run builds the paced transport itself.
  transport?: NcsbeTransport;
  log?: (message: string) => void;
  now?: Date;
}) {
  const { options } = input;
  const log = input.log ?? ((message: string) => console.log(message));
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid NCSBE raw data refresh timestamp");
  }

  if (options.dryRun) {
    // Dry runs never touch the portal: reports are discovered from the live
    // inventories, so there is no request-free way to preview them — the
    // preview is the run's inputs, not its downloads.
    return {
      type: "north_carolina_ncsbe_raw_data_refresh",
      ts: new Date().toISOString(),
      started_at: startedAt.toISOString(),
      cycle_year: options.cycleYear,
      cache_dir: options.cacheDir,
      dry_run: true,
      committees: options.committees.map((committee) => ({
        sboe_id: committee.sboeId,
        org_group_id: committee.orgGroupId,
      })),
      include_ie: options.includeIe,
      spacing_ms: options.spacingMs,
      force: options.force,
    };
  }

  const transport = input.transport ?? createNcsbeTransport({ spacingMs: options.spacingMs, log });
  const acquisition = await acquireNcsbeCycleArtifacts({
    transport,
    cacheDir: options.cacheDir,
    cycleYear: options.cycleYear,
    committees: options.committees,
    includeIe: options.includeIe,
    force: options.force,
    retrievedAt: startedAt,
    log,
  });

  return {
    type: "north_carolina_ncsbe_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    cycle_year: options.cycleYear,
    cache_dir: options.cacheDir,
    dry_run: false,
    committees: acquisition.committees.map((committee) => ({
      sboe_id: committee.sboeId,
      inventory_row_count: committee.inventoryRowCount,
      selected_report_count: committee.selectedReportCount,
      unusable_period_row_count: committee.unusablePeriodRowCount,
      fetched_report_count: committee.fetched.length,
      skipped_report_ids: committee.skippedReportIds,
      failures: committee.failures.map((failure) => ({
        report_id: failure.reportId,
        message: failure.message,
      })),
    })),
    committee_failures: acquisition.committeeFailures.map((failure) => ({
      sboe_id: failure.sboeId,
      message: failure.message,
    })),
    ie:
      acquisition.ie === null
        ? null
        : {
            years: acquisition.ie.years,
            inventory_row_count: acquisition.ie.inventoryRowCount,
            structured_row_count: acquisition.ie.structuredRowCount,
            image_only_row_count: acquisition.ie.imageOnlyRowCount,
            fetched_report_count: acquisition.ie.fetched.length,
            skipped_report_ids: acquisition.ie.skippedReportIds,
            failures: acquisition.ie.failures.map((failure) => ({
              report_id: failure.reportId,
              message: failure.message,
            })),
          },
    ie_failure: acquisition.ieFailure,
    // True when NO requested scope succeeded — every committee failed and
    // the IE pass (if requested) failed too. Partial results keep exit code
    // 0 (failures ride in the payload and the next run's skip logic makes
    // retries cheap), but a run that acquired nothing must fail loudly so
    // automation can alert and retry instead of trusting an empty success.
    total_failure: acquisition.committees.length === 0 && acquisition.ie === null,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshNorthCarolinaNcsbeRawDataScriptArgs(process.argv.slice(2));
  if (!isNorthCarolinaNcsbeRawDataRefreshEnabled(options.force)) {
    console.log("North Carolina NCSBE raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshNorthCarolinaNcsbeRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
  if ("total_failure" in output && output.total_failure) {
    console.error("North Carolina NCSBE raw data refresh acquired nothing; see failures in the output above");
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("North Carolina NCSBE raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
