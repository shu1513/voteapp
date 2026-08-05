import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isOhioSosRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  collectOhioSos31uAnnualTotals,
  downloadOhioSosCycleArtifacts,
  fetchOhioSos31uDetails,
  listOhioSosPortalFiles,
  planOhioSosCycleDownloads,
  withOhioSosChromeTab,
  DEFAULT_OHIO_SOS_REQUEST_SPACING_MS,
} from "../pipeline/ohioFinance/ohioSosArtifactAcquisition.js";
import {
  getOhioSosCycleArtifactStatus,
  normalizeOhioSosTransactionYear,
  DEFAULT_OHIO_SOS_CACHE_DIR,
} from "../pipeline/ohioFinance/ohioSosArtifactCache.js";
import {
  connectOhioSosChrome,
  DEFAULT_OHIO_SOS_CHROME_DEBUG_URL,
} from "../pipeline/ohioFinance/ohioSosChromeClient.js";

// Downloads one cycle of Ohio SoS bulk campaign-finance artifacts into the
// local cache, then fetches the Form 31-U detail for every independent
// expenditure report found in them.
//
// The portal is behind Cloudflare and refuses scripted HTTP, headless
// Chrome, and fresh-profile automated Chrome alike (ohio_plan.md decision 9),
// so this script drives a Chrome the user has already started with their own
// profile. It solves no challenges and spoofs nothing: if Cloudflare shows an
// interstitial, the script stops and says so.
//
//   1. Quit Chrome, then start it with remote debugging:
//        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
//          --remote-debugging-port=9222
//   2. npm run ohio-candidates:finance:raw:refresh -- --cycle-year=2026
//
// Downloads are strictly sequential with a delay between them; the portal
// answers rapid requests with HTTP 429.

export { DEFAULT_OHIO_SOS_CACHE_DIR };

export type RefreshOhioSosCampaignFinanceRawDataScriptOptions = {
  cycleYear: number;
  cacheDir: string;
  chromeDebugUrl: string;
  spacingMs: number;
  downloadTimeoutMs: number;
  force: boolean;
  skipDetails: boolean;
  dryRun: boolean;
};

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

export function parseRefreshOhioSosCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshOhioSosCampaignFinanceRawDataScriptOptions {
  const cycleYearRaw = readValueFlag(args, "--cycle-year") ?? readValueFlag(args, "--year");
  const cycleYear = normalizeOhioSosTransactionYear(
    parsePositiveInteger(cycleYearRaw, new Date().getUTCFullYear(), "--cycle-year")
  );
  const cacheDir = (
    readValueFlag(args, "--cache-dir")?.trim() ||
    process.env.OHIO_SOS_RAW_DATA_CACHE_DIR?.trim() ||
    DEFAULT_OHIO_SOS_CACHE_DIR
  ).trim();

  return {
    cycleYear,
    cacheDir: resolve(cacheDir),
    chromeDebugUrl: readValueFlag(args, "--chrome-debug-url")?.trim() || DEFAULT_OHIO_SOS_CHROME_DEBUG_URL,
    spacingMs: parsePositiveInteger(
      readValueFlag(args, "--spacing-ms"),
      DEFAULT_OHIO_SOS_REQUEST_SPACING_MS,
      "--spacing-ms"
    ),
    downloadTimeoutMs: parsePositiveInteger(
      readValueFlag(args, "--download-timeout-ms"),
      300_000,
      "--download-timeout-ms"
    ),
    force: args.includes("--force"),
    skipDetails: args.includes("--skip-31u-details"),
    dryRun: args.includes("--dry-run"),
  };
}

export async function runRefreshOhioSosCampaignFinanceRawDataScript(input: {
  options: RefreshOhioSosCampaignFinanceRawDataScriptOptions;
  log?: (message: string) => void;
  now?: Date;
}) {
  const { options } = input;
  const log = input.log ?? ((message: string) => console.log(message));
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Ohio SoS raw data refresh timestamp");
  }

  const cachedBefore = await getOhioSosCycleArtifactStatus({
    cacheDir: options.cacheDir,
    cycleYear: options.cycleYear,
  });
  // Without --force, artifacts already cached and intact are left alone —
  // re-pulling a clean 305 MB cycle costs the portal for nothing.
  const skip = options.force
    ? new Set<string>()
    : new Set(cachedBefore.filter((status) => status.status === "ready").map((status) => status.fileName));

  const session = await connectOhioSosChrome({ debugUrl: options.chromeDebugUrl });
  try {
    return await withOhioSosChromeTab(session, async (tab) => {
      const listedFiles = await listOhioSosPortalFiles({
        session,
        tab,
        spacingMs: options.spacingMs,
        log,
      });
      const plan = planOhioSosCycleDownloads({ cycleYear: options.cycleYear, listedFiles });
      if (plan.missingFileNames.length > 0) {
        log(`Not listed on the portal: ${plan.missingFileNames.join(", ")}`);
      }

      if (options.dryRun) {
        return {
          type: "ohio_sos_raw_data_refresh",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          cycle_year: options.cycleYear,
          cache_dir: options.cacheDir,
          dry_run: true,
          listed_file_count: listedFiles.length,
          plan: plan.entries.map((entry) => ({
            file_name: entry.fileName,
            download_id: entry.downloadId,
            date_modified: entry.dateModified,
            cached: skip.has(entry.fileName),
          })),
          missing_file_names: plan.missingFileNames,
        };
      }

      const acquisition = await downloadOhioSosCycleArtifacts({
        session,
        tab,
        cycleYear: options.cycleYear,
        cacheDir: options.cacheDir,
        plan,
        spacingMs: options.spacingMs,
        downloadTimeoutMs: options.downloadTimeoutMs,
        skip,
        log,
        now: startedAt,
      });

      let details = null;
      if (!options.skipDetails) {
        const annualTotals = await collectOhioSos31uAnnualTotals({
          cacheDir: options.cacheDir,
          cycleYear: options.cycleYear,
          now: startedAt,
        });
        log(`Found ${annualTotals.size} Form 31-U report keys in the cached expenditure files`);
        const fetched = await fetchOhioSos31uDetails({
          session,
          tab,
          cacheDir: options.cacheDir,
          cycleYear: options.cycleYear,
          annualTotals,
          spacingMs: options.spacingMs,
          log,
          retrievedAt: startedAt,
        });
        details = {
          detail_path: fetched.detailPath,
          report_count: fetched.reports.length,
          row_count: fetched.reports.reduce((sum, report) => sum + report.rows.length, 0),
          unreconciled_report_keys: fetched.reports
            .filter((report) => !report.reconciliation.matches)
            .map((report) => report.reportKey),
          excluded_direction_row_count: fetched.reports.reduce(
            (sum, report) => sum + report.reconciliation.excludedDirectionRowCount,
            0
          ),
          excluded_direction_cents: fetched.reports.reduce(
            (sum, report) => sum + report.reconciliation.excludedDirectionCents,
            0
          ),
          failures: fetched.failures,
        };
      }

      return {
        type: "ohio_sos_raw_data_refresh",
        ts: new Date().toISOString(),
        started_at: startedAt.toISOString(),
        cycle_year: options.cycleYear,
        cache_dir: options.cacheDir,
        dry_run: false,
        downloaded: acquisition.downloaded.map((result) => ({
          file_name: result.entry.fileName,
          download_id: result.entry.downloadId,
          row_count: result.manifest.rowCount,
          byte_size: result.manifest.byteSize,
          sha256: result.manifest.sha256,
          min_transaction_date: result.manifest.minTransactionDateIso,
          max_transaction_date: result.manifest.maxTransactionDateIso,
          implausible_date_row_count: result.manifest.implausibleDateRowCount,
          missing_amount_row_count: result.manifest.missingAmountRowCount,
        })),
        skipped_file_names: acquisition.skipped.map((entry) => entry.fileName),
        missing_file_names: acquisition.missingFileNames,
        failures: acquisition.failures,
        form_31u: details,
      };
    });
  } finally {
    session.close();
  }
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshOhioSosCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isOhioSosRawDataRefreshEnabled(options.force)) {
    console.log("Ohio SoS raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshOhioSosCampaignFinanceRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Ohio SoS raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
