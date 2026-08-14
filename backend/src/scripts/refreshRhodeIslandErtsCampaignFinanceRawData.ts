import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isRhodeIslandErtsRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  acquireRhodeIslandErtsArtifacts,
  ertsCycleWindowForYear,
  type ErtsAcquisitionOrganization,
} from "../pipeline/rhodeIslandFinance/rhodeIslandErtsArtifactAcquisition.js";
import { DEFAULT_RHODE_ISLAND_ERTS_CACHE_DIR } from "../pipeline/rhodeIslandFinance/rhodeIslandErtsArtifactCache.js";
import {
  createErtsTransport,
  DEFAULT_ERTS_REQUEST_SPACING_MS,
  requireErtsOrgId,
  type ErtsTransport,
} from "../pipeline/rhodeIslandFinance/rhodeIslandErtsClient.js";

// Downloads one cycle of Rhode Island ERTS campaign-finance artifacts into
// the local cache (rhode_island_plan.md PR 4): per-organization filing lists,
// per-CF-2-period contribution/expenditure report pages and detail exports
// (each export reconciled against its summary groupings — the only
// silent-truncation control the portal offers), the in-force CF-2 version
// PDFs, and the paginated CF-8 "Other Filings" index. Paced WebForms HTTP —
// one request in flight, 2 s spacing. Nothing here writes to the database;
// the finance sync reads the cache only.
//
//   npm run rhode-island-candidates:finance:raw:refresh -- \
//     --cycle-year=2026 --organization "2235:McKee:DANIEL J MCKEE"
//   npm run rhode-island-candidates:finance:raw:refresh -- --cycle-year=2026
//
// Organizations are passed as <OrgID>:<searchLastName>:<organizationName> —
// the numeric Board key (verified against the portal's own search redirect
// before anything is fetched), the search term, and the exact organization
// name to select. With no --organization, a CF-8-only run pulls just the
// index. Roster-driven discovery arrives with the resolver (PR 5).

export type RefreshRhodeIslandErtsRawDataScriptOptions = {
  cycleYear: number;
  cacheDir: string;
  organizations: ErtsAcquisitionOrganization[];
  includeCf8: boolean;
  spacingMs: number;
  force: boolean;
  dryRun: boolean;
};

const VALUE_FLAG_NAMES = ["--cycle-year", "--cache-dir", "--organization", "--spacing-ms"] as const;
const BOOLEAN_FLAG_NAMES = ["--skip-cf8", "--force", "--dry-run"] as const;

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

// <OrgID>:<searchLastName>:<organizationName>; the name keeps any further
// colons verbatim.
export function parseErtsOrganizationArg(value: string): ErtsAcquisitionOrganization {
  const first = value.indexOf(":");
  const second = first < 0 ? -1 : value.indexOf(":", first + 1);
  if (first < 0 || second < 0) {
    throw new Error(`--organization must be <OrgID>:<searchLastName>:<organizationName>, got ${JSON.stringify(value)}`);
  }
  const orgId = requireErtsOrgId(value.slice(0, first));
  const searchLastName = value.slice(first + 1, second).trim();
  const organizationName = value.slice(second + 1).trim();
  if (searchLastName.length === 0 || organizationName.length === 0) {
    throw new Error(`--organization needs a non-empty search term and organization name: ${JSON.stringify(value)}`);
  }
  return { orgId, searchLastName, organizationName };
}

export function parseRefreshRhodeIslandErtsRawDataScriptArgs(
  args: readonly string[]
): RefreshRhodeIslandErtsRawDataScriptOptions {
  assertKnownRefreshScriptArgs(args);
  const currentYear = new Date().getUTCFullYear();
  const cycleYear = parsePositiveInteger(
    readValueFlag(args, "--cycle-year"),
    // RI cycles end on even years; default to the cycle containing today.
    currentYear % 2 === 0 ? currentYear : currentYear + 1,
    "--cycle-year"
  );
  const cacheDir = (
    readValueFlag(args, "--cache-dir")?.trim() ||
    process.env.RHODE_ISLAND_ERTS_RAW_DATA_CACHE_DIR?.trim() ||
    DEFAULT_RHODE_ISLAND_ERTS_CACHE_DIR
  ).trim();

  const organizations = readValueFlags(args, "--organization").map(parseErtsOrganizationArg);
  const seen = new Set<string>();
  for (const organization of organizations) {
    if (seen.has(organization.orgId)) {
      throw new Error(`Duplicate --organization OrgID: ${organization.orgId}`);
    }
    seen.add(organization.orgId);
  }

  const includeCf8 = !args.includes("--skip-cf8");
  if (organizations.length === 0 && !includeCf8) {
    throw new Error("Nothing to fetch: pass --organization entries or drop --skip-cf8");
  }

  return {
    cycleYear,
    cacheDir: resolve(cacheDir),
    organizations,
    includeCf8,
    spacingMs: parsePositiveInteger(readValueFlag(args, "--spacing-ms"), DEFAULT_ERTS_REQUEST_SPACING_MS, "--spacing-ms"),
    force: args.includes("--force"),
    dryRun: args.includes("--dry-run"),
  };
}

export async function runRefreshRhodeIslandErtsRawDataScript(input: {
  options: RefreshRhodeIslandErtsRawDataScriptOptions;
  // Test seam; a real run builds the paced transport itself.
  transport?: ErtsTransport;
  log?: (message: string) => void;
  now?: Date;
}) {
  const { options } = input;
  const log = input.log ?? ((message: string) => console.log(message));
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid ERTS raw data refresh timestamp");
  }
  const cycle = ertsCycleWindowForYear(options.cycleYear);

  if (options.dryRun) {
    // Dry runs never touch the portal: the fetch set is discovered from the
    // live filing lists, so the preview is the run's inputs, not its
    // downloads.
    return {
      type: "rhode_island_erts_raw_data_refresh",
      ts: new Date().toISOString(),
      started_at: startedAt.toISOString(),
      cycle_year: options.cycleYear,
      cycle_begin: cycle.beginUs,
      cycle_end: cycle.endUs,
      cache_dir: options.cacheDir,
      dry_run: true,
      organizations: options.organizations.map((organization) => ({
        org_id: organization.orgId,
        search_last_name: organization.searchLastName,
        organization_name: organization.organizationName,
      })),
      include_cf8: options.includeCf8,
      spacing_ms: options.spacingMs,
      force: options.force,
    };
  }

  const transport = input.transport ?? createErtsTransport({ spacingMs: options.spacingMs, log });
  const acquisition = await acquireRhodeIslandErtsArtifacts({
    transport,
    cacheDir: options.cacheDir,
    cycle,
    organizations: options.organizations,
    includeCf8: options.includeCf8,
    force: options.force,
    retrievedAt: startedAt,
    log,
  });

  return {
    type: "rhode_island_erts_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    cycle_year: options.cycleYear,
    cycle_begin: cycle.beginUs,
    cycle_end: cycle.endUs,
    cache_dir: options.cacheDir,
    dry_run: false,
    organizations: acquisition.organizations.map((organization) => ({
      org_id: organization.orgId,
      organization_name: organization.organizationName,
      filing_row_count: organization.filingRowCount,
      selected_filing_count: organization.selectedFilingCount,
      unfiled_row_count: organization.unfiledRowCount,
      non_cf2_filed_row_count: organization.nonCf2FiledRowCount,
      out_of_cycle_row_count: organization.outOfCycleRowCount,
      unusable_period_row_count: organization.unusablePeriodRowCount,
      periods: organization.periods.map((period) => ({
        begin: period.beginIso,
        end: period.endIso,
        contributions: period.contributionClassification,
        expenditures: period.expenditureClassification,
        export_row_count: period.exportRowCount,
        confirmed_summary_only_labels: period.confirmedSummaryOnlyLabels,
      })),
      fetched_pdf_count: organization.fetchedPdfCount,
      skipped_pdf_count: organization.skippedPdfCount,
    })),
    organization_failures: acquisition.organizationFailures.map((failure) => ({
      org_id: failure.orgId,
      message: failure.message,
    })),
    cf8:
      acquisition.cf8 === null
        ? null
        : {
            page_count: acquisition.cf8.pageCount,
            row_count: acquisition.cf8.rowCount,
            cycle_row_count: acquisition.cf8.cycleRowCount,
            independent_expenditure_row_count: acquisition.cf8.independentExpenditureRowCount,
            missing_scan_link_count: acquisition.cf8.missingScanLinkCount,
          },
    cf8_failure: acquisition.cf8Failure,
    // True when NO requested scope succeeded. Partial results keep exit code
    // 0 (failures ride in the payload and re-runs are cheap), but a run that
    // acquired nothing must fail loudly so automation can alert and retry
    // instead of trusting an empty success.
    total_failure: acquisition.organizations.length === 0 && acquisition.cf8 === null,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshRhodeIslandErtsRawDataScriptArgs(process.argv.slice(2));
  if (!isRhodeIslandErtsRawDataRefreshEnabled(options.force)) {
    console.log("Rhode Island ERTS raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshRhodeIslandErtsRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
  if ("total_failure" in output && output.total_failure) {
    console.error("Rhode Island ERTS raw data refresh acquired nothing; see failures in the output above");
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Rhode Island ERTS raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
