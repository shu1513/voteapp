// The only live-portal caller on the North Dakota direct path besides
// auto-link. For each election year it refreshes the two schedule files that
// carry the election's periods, resolves the cycle window from them (the
// same rule the sync applies), then refreshes the contributions CSV, the
// CON API harvest and the IE API harvest for every year the window spans,
// and the committee registry once per election year — so the cache always
// holds exactly what the sync will ask for.

import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isNorthDakotaCfrsRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  normalizeNorthDakotaArtifactYear,
  readNorthDakotaBulkArtifact,
  refreshNorthDakotaApiContributionsArtifact,
  refreshNorthDakotaApiIndependentExpendituresArtifact,
  refreshNorthDakotaBulkArtifact,
  refreshNorthDakotaRegistryArtifact,
  type NorthDakotaArtifactRefreshResult,
} from "../pipeline/northDakotaFinance/northDakotaCfrsArtifactCache.js";
import { resolveNorthDakotaCfrsCacheDir } from "../pipeline/northDakotaFinance/northDakotaCandidateFinanceSync.js";
import {
  getNorthDakotaDataDownloadCatalog,
  type NorthDakotaCfrsClientOptions,
} from "../pipeline/northDakotaFinance/northDakotaCfrsClient.js";
import { parseNorthDakotaReportingScheduleCsv } from "../pipeline/northDakotaFinance/northDakotaCfrsCsv.js";
import {
  northDakotaCycleWindowYears,
  northDakotaScheduleYearsForElection,
  resolveNorthDakotaCandidateCycleWindow,
} from "../pipeline/northDakotaFinance/northDakotaReportingCycleWindows.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type RefreshNorthDakotaCampaignFinanceRawDataScriptOptions = {
  /** Candidate election years to refresh, deduped, in argument order. */
  electionYears: number[];
  cacheDir: string;
  /** Bypasses only the refresh feature sub-gate, never cache safety. */
  force: boolean;
  /** Forwarded as the cache's force: lets a 0-row artifact displace a populated one. */
  acceptEmpty: boolean;
  timeoutMs: number | undefined;
};

const BOOLEAN_FLAGS = new Set(["--force", "--accept-empty"]);
const VALUE_FLAGS = new Set(["--election-year", "--cache-dir", "--timeout-ms"]);

function readValueFlags(args: readonly string[], name: string): string[] {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg.startsWith(prefix)) {
      values.push(arg.slice(prefix.length).trim());
    } else if (arg === name) {
      values.push(args[index + 1]!.trim());
      index += 1;
    }
  }
  return values;
}

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values = readValueFlags(args, name);
  if (values.length > 1) throw new Error(`Provide ${name} at most once`);
  return values[0];
}

function parseElectionYear(value: string): number {
  if (!/^\d{4}$/.test(value)) {
    throw new Error(`Invalid --election-year value: ${value}`);
  }
  return normalizeNorthDakotaArtifactYear(Number(value));
}

export function parseRefreshNorthDakotaCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshNorthDakotaCampaignFinanceRawDataScriptOptions {
  assertKnownCliFlags(args, "North Dakota CFRS raw data refresh", BOOLEAN_FLAGS, VALUE_FLAGS);
  const electionYears = [...new Set(readValueFlags(args, "--election-year").map(parseElectionYear))];
  if (electionYears.length === 0) {
    electionYears.push(normalizeNorthDakotaArtifactYear(new Date().getUTCFullYear()));
  }
  const timeoutValue = readValueFlag(args, "--timeout-ms");
  if (timeoutValue !== undefined && !/^[1-9]\d*$/.test(timeoutValue)) {
    throw new Error(`Invalid --timeout-ms value: ${timeoutValue}`);
  }
  return {
    electionYears,
    cacheDir: resolveNorthDakotaCfrsCacheDir(readValueFlag(args, "--cache-dir")),
    force: args.includes("--force"),
    acceptEmpty: args.includes("--accept-empty"),
    timeoutMs: timeoutValue === undefined ? undefined : Number(timeoutValue),
  };
}

function summarize(refresh: NorthDakotaArtifactRefreshResult) {
  return {
    kind: refresh.current.artifact.kind,
    year: refresh.current.artifact.year,
    status: refresh.status,
    file_path: refresh.filePath,
    record_count: refresh.current.recordCount,
    recovered_row_count: refresh.current.recoveredRowCount,
    sha256: refresh.current.sha256,
  };
}

// The portal wedged once under a burst of rapid fetches (plan: "burst
// sensitivity observed"); the probe paced its calls at ~2 s and never
// tripped it, so every artifact refresh here waits the same beat.
export const NORTH_DAKOTA_REFRESH_PAUSE_MS = 2_000;

export async function runRefreshNorthDakotaCampaignFinanceRawDataScript(input: {
  options: RefreshNorthDakotaCampaignFinanceRawDataScriptOptions;
  clientOptions?: NorthDakotaCfrsClientOptions;
  refreshBulk?: typeof refreshNorthDakotaBulkArtifact;
  refreshApi?: typeof refreshNorthDakotaApiContributionsArtifact;
  refreshIe?: typeof refreshNorthDakotaApiIndependentExpendituresArtifact;
  refreshRegistry?: typeof refreshNorthDakotaRegistryArtifact;
  fetchCatalog?: typeof getNorthDakotaDataDownloadCatalog;
  readBulk?: typeof readNorthDakotaBulkArtifact;
  /** Wait between portal-touching steps; tests pass 0. */
  pauseMs?: number;
}) {
  const clientOptions: NorthDakotaCfrsClientOptions = {
    ...input.clientOptions,
    ...(input.options.timeoutMs === undefined ? {} : { timeoutMs: input.options.timeoutMs }),
  };
  const refreshBulk = input.refreshBulk ?? refreshNorthDakotaBulkArtifact;
  const refreshApi = input.refreshApi ?? refreshNorthDakotaApiContributionsArtifact;
  const refreshIe = input.refreshIe ?? refreshNorthDakotaApiIndependentExpendituresArtifact;
  const refreshRegistry = input.refreshRegistry ?? refreshNorthDakotaRegistryArtifact;
  const readBulk = input.readBulk ?? readNorthDakotaBulkArtifact;
  const pauseMs = input.pauseMs ?? NORTH_DAKOTA_REFRESH_PAUSE_MS;
  const pause = () => (pauseMs > 0 ? new Promise<void>((done) => setTimeout(done, pauseMs)) : Promise.resolve());
  const { cacheDir, acceptEmpty } = input.options;
  // One catalog read for the whole run.
  const catalog = await (input.fetchCatalog ?? getNorthDakotaDataDownloadCatalog)(clientOptions);

  const artifacts: ReturnType<typeof summarize>[] = [];
  const windows = [];
  const refreshedBulk = new Set<string>();
  const refreshedApi = new Set<number>();
  for (const electionYear of input.options.electionYears) {
    const scheduleRows = [];
    for (const year of northDakotaScheduleYearsForElection(electionYear)) {
      const key = `reporting_schedules:${year}`;
      if (!refreshedBulk.has(key)) {
        refreshedBulk.add(key);
        await pause();
        artifacts.push(
          summarize(await refreshBulk({ kind: "reporting_schedules", year, cacheDir, catalog, force: acceptEmpty, clientOptions }))
        );
      }
      const parsed = parseNorthDakotaReportingScheduleCsv((await readBulk({ kind: "reporting_schedules", year, cacheDir })).csvText);
      scheduleRows.push(...parsed.rows);
    }
    const window = resolveNorthDakotaCandidateCycleWindow({ scheduleRows, electionYear });
    const windowYears = northDakotaCycleWindowYears(window);
    windows.push({
      election_year: electionYear,
      election: window.election,
      window_start: window.windowStart,
      window_end: window.windowEnd,
      window_years: windowYears,
    });
    for (const year of windowYears) {
      const key = `contributions:${year}`;
      if (!refreshedBulk.has(key)) {
        refreshedBulk.add(key);
        await pause();
        artifacts.push(summarize(await refreshBulk({ kind: "contributions", year, cacheDir, catalog, force: acceptEmpty, clientOptions })));
      }
      if (!refreshedApi.has(year)) {
        refreshedApi.add(year);
        await pause();
        artifacts.push(summarize(await refreshApi({ year, cacheDir, force: acceptEmpty, clientOptions })));
        await pause();
        artifacts.push(summarize(await refreshIe({ year, cacheDir, force: acceptEmpty, clientOptions })));
      }
    }
    // Election years are deduped by the arg parser, so once each.
    await pause();
    artifacts.push(summarize(await refreshRegistry({ electionYear, cacheDir, force: acceptEmpty, clientOptions })));
  }
  return {
    type: "north_dakota_cfrs_raw_data_refresh",
    ts: new Date().toISOString(),
    cache_dir: cacheDir,
    windows,
    artifacts,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshNorthDakotaCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isNorthDakotaCfrsRawDataRefreshEnabled(options.force)) {
    console.log("North Dakota CFRS raw data refresh disabled; no artifact refreshed");
    return;
  }
  console.log(JSON.stringify(await runRefreshNorthDakotaCampaignFinanceRawDataScript({ options }), null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("North Dakota CFRS raw data refresh failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
