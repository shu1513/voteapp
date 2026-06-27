import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isIndianaCampaignFinanceRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR,
  INDIANA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS,
  buildIndianaCampaignFinanceArtifactUrl,
  normalizeIndianaCampaignFinanceArtifactKind,
  parseIndianaCampaignFinanceHttpsUrl,
  refreshIndianaCampaignFinanceArtifactCache,
  type IndianaCampaignFinanceArtifactKind,
} from "../pipeline/indianaFinance/indianaCampaignFinanceArtifactCache.js";

export { DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR };

export type RefreshIndianaCampaignFinanceRawDataScriptOptions = {
  year: number;
  artifactKind: IndianaCampaignFinanceArtifactKind;
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
    if (arg?.startsWith(inlinePrefix)) {
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

function parseYear(value: string | undefined): number {
  return parsePositiveInteger(value, new Date().getUTCFullYear(), "--year");
}

function parseLocalPath(value: string | undefined, fallback: string, flagName: string): string {
  const normalized = value?.trim() || fallback;
  if (normalized.length === 0) {
    throw new Error(`${flagName} is required`);
  }
  return resolve(normalized);
}

function parseArtifactKind(value: string | undefined): IndianaCampaignFinanceArtifactKind {
  return normalizeIndianaCampaignFinanceArtifactKind(value?.trim() || "contribution");
}

export function parseRefreshIndianaCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshIndianaCampaignFinanceRawDataScriptOptions {
  const year = parseYear(readValueFlag(args, "--year"));
  const artifactKind = parseArtifactKind(readValueFlag(args, "--artifact-kind"));
  return {
    year,
    artifactKind,
    url: parseIndianaCampaignFinanceHttpsUrl(
      readValueFlag(args, "--url")?.trim() || buildIndianaCampaignFinanceArtifactUrl({ year, artifactKind }),
      "--url"
    ),
    cacheDir: parseLocalPath(
      readValueFlag(args, "--cache-dir"),
      DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR,
      "--cache-dir"
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      INDIANA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshIndianaCampaignFinanceRawDataScript(input: {
  options: RefreshIndianaCampaignFinanceRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Indiana campaign finance raw data refresh timestamp");
  }

  const refresh = await refreshIndianaCampaignFinanceArtifactCache({
    year: input.options.year,
    artifactKind: input.options.artifactKind,
    cacheDir: input.options.cacheDir,
    url: input.options.url,
    force: input.options.force,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.options.timeoutMs,
    now: startedAt,
  });

  return {
    type: "indiana_campaign_finance_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    year: input.options.year,
    artifact_kind: input.options.artifactKind,
    cache_dir: refresh.cacheDir,
    zip_path: refresh.zipPath,
    metadata_path: refresh.metadataPath,
    status: refresh.status,
    remote: refresh.remote,
    previous: refresh.previous,
    current: refresh.current,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshIndianaCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isIndianaCampaignFinanceRawDataRefreshEnabled(options.force)) {
    console.log("Indiana campaign finance raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshIndianaCampaignFinanceRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Indiana campaign finance raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
