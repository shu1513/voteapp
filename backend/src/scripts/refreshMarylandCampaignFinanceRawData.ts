import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMarylandCfsRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_MARYLAND_CFS_CACHE_DIR,
  refreshMarylandCfsArtifactCache,
} from "../pipeline/marylandFinance/marylandCfsArtifactCache.js";
import {
  MARYLAND_CFS_FETCH_TIMEOUT_MS,
  MARYLAND_CFS_PUBLIC_EXPORT_API_URL,
  normalizeMarylandCfsArtifactKind,
  parseMarylandCfsHttpsUrl,
  type MarylandCfsArtifactKind,
} from "../pipeline/marylandFinance/marylandCfsClient.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { readStrictFlagValues } from "../utils/cliFlags.js";

export { DEFAULT_MARYLAND_CFS_CACHE_DIR };

export type RefreshMarylandCampaignFinanceRawDataScriptOptions = {
  filingYear: number;
  artifactKind: MarylandCfsArtifactKind;
  url: string;
  cacheDir: string;
  force: boolean;
  timeoutMs: number;
};

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values = readStrictFlagValues(args, name);
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

function parseFilingYear(value: string | undefined): number {
  return parsePositiveInteger(value, new Date().getUTCFullYear(), "--filing-year");
}

function readFilingYearFlag(args: readonly string[]): string | undefined {
  const values = [...readStrictFlagValues(args, "--filing-year"), ...readStrictFlagValues(args, "--year")];
  if (values.length > 1) {
    throw new Error("Provide --filing-year at most once");
  }
  return values[0];
}

function parseLocalPath(value: string | undefined, fallback: string, flagName: string): string {
  const normalized = value?.trim() || fallback;
  if (normalized.length === 0) {
    throw new Error(`${flagName} is required`);
  }
  return resolve(normalized);
}

function parseArtifactKind(value: string | undefined): MarylandCfsArtifactKind {
  return normalizeMarylandCfsArtifactKind(value?.trim() || "contributions");
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--artifact-kind", "--cache-dir", "--filing-year", "--timeout-ms", "--url", "--year"]);

export function parseRefreshMarylandCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshMarylandCampaignFinanceRawDataScriptOptions {
  assertKnownCliFlags(args, "Maryland campaign finance raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  const filingYear = parseFilingYear(readFilingYearFlag(args));
  return {
    filingYear,
    artifactKind: parseArtifactKind(readValueFlag(args, "--artifact-kind")),
    url: parseMarylandCfsHttpsUrl(readValueFlag(args, "--url")?.trim() || MARYLAND_CFS_PUBLIC_EXPORT_API_URL, "--url"),
    cacheDir: parseLocalPath(
      readValueFlag(args, "--cache-dir"),
      process.env.MARYLAND_CFS_RAW_DATA_CACHE_DIR?.trim() || DEFAULT_MARYLAND_CFS_CACHE_DIR,
      "--cache-dir"
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(readValueFlag(args, "--timeout-ms"), MARYLAND_CFS_FETCH_TIMEOUT_MS, "--timeout-ms"),
  };
}

export async function runRefreshMarylandCampaignFinanceRawDataScript(input: {
  options: RefreshMarylandCampaignFinanceRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Maryland CFS raw data refresh timestamp");
  }

  const refresh = await refreshMarylandCfsArtifactCache({
    filingYear: input.options.filingYear,
    artifactKind: input.options.artifactKind,
    cacheDir: input.options.cacheDir,
    url: input.options.url,
    force: input.options.force,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.options.timeoutMs,
    now: startedAt,
  });

  return {
    type: "maryland_cfs_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    filing_year: input.options.filingYear,
    artifact_kind: input.options.artifactKind,
    cache_dir: refresh.cacheDir,
    file_path: refresh.filePath,
    metadata_path: refresh.metadataPath,
    status: refresh.status,
    remote: refresh.remote,
    previous: refresh.previous,
    current: refresh.current,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshMarylandCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isMarylandCfsRawDataRefreshEnabled(options.force)) {
    console.log("Maryland CFS raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshMarylandCampaignFinanceRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Maryland CFS raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
