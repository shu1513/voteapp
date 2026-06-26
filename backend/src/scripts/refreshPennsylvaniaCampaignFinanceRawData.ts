import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isPennsylvaniaCampaignFinanceRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR,
  PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS,
  buildPennsylvaniaCampaignFinanceExportUrl,
  type PennsylvaniaCampaignFinanceExportExtractor,
  normalizePennsylvaniaCampaignFinanceExportYear,
  parsePennsylvaniaCampaignFinanceHttpsUrl,
  refreshPennsylvaniaCampaignFinanceExportCache,
} from "../pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceArtifactCache.js";

export { DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR };

export type RefreshPennsylvaniaCampaignFinanceRawDataScriptOptions = {
  year: number;
  url: string;
  cacheDir: string;
  force: boolean;
  timeoutMs: number;
};

function readValueFlags(args: readonly string[], name: string): string[] {
  const values: string[] = [];
  const inlinePrefix = `${name}=`;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
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

function parseLocalPath(value: string | undefined, fallback: string, flagName: string): string {
  const normalized = value?.trim() || fallback;
  if (normalized.length === 0) {
    throw new Error(`${flagName} is required`);
  }
  return resolve(normalized);
}

export function defaultPennsylvaniaCampaignFinanceRawDataRefreshYear(now = new Date()): number {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Pennsylvania campaign finance raw data refresh date");
  }
  return now.getUTCFullYear();
}

export function parseRefreshPennsylvaniaCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshPennsylvaniaCampaignFinanceRawDataScriptOptions {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(
    parsePositiveInteger(
      readValueFlag(args, "--year"),
      defaultPennsylvaniaCampaignFinanceRawDataRefreshYear(),
      "--year"
    )
  );
  return {
    year,
    url: parsePennsylvaniaCampaignFinanceHttpsUrl(
      readValueFlag(args, "--url")?.trim() || buildPennsylvaniaCampaignFinanceExportUrl({ year }),
      "--url"
    ),
    cacheDir: parseLocalPath(
      readValueFlag(args, "--cache-dir"),
      DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR,
      "--cache-dir"
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshPennsylvaniaCampaignFinanceRawDataScript(input: {
  options: RefreshPennsylvaniaCampaignFinanceRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  extractArchive?: PennsylvaniaCampaignFinanceExportExtractor;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Pennsylvania campaign finance raw data refresh timestamp");
  }

  const refresh = await refreshPennsylvaniaCampaignFinanceExportCache({
    year: input.options.year,
    cacheDir: input.options.cacheDir,
    url: input.options.url,
    force: input.options.force,
    fetchImpl: input.fetchImpl,
    extractArchive: input.extractArchive,
    timeoutMs: input.options.timeoutMs,
    now: startedAt,
  });

  return {
    type: "pennsylvania_campaign_finance_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    year: input.options.year,
    cache_dir: refresh.cacheDir,
    archive_path: refresh.archivePath,
    extracted_dir: refresh.extractedDir,
    metadata_path: refresh.metadataPath,
    status: refresh.status,
    remote: refresh.remote,
    previous: refresh.previous,
    current: refresh.current,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshPennsylvaniaCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isPennsylvaniaCampaignFinanceRawDataRefreshEnabled(options.force)) {
    console.log("Pennsylvania campaign finance raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshPennsylvaniaCampaignFinanceRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Pennsylvania campaign finance raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
