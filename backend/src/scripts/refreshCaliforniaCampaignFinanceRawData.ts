import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS,
  CAL_ACCESS_RAW_DATA_ZIP_URL,
  parseCalAccessHttpsUrl,
  refreshCalAccessRawDataArtifactCache,
} from "../pipeline/californiaFinance/calAccessRawDataArtifactCache.js";
import {
  listCalAccessRawDataManifestFileNames,
  validateCalAccessRawDataManifest,
} from "../pipeline/californiaFinance/calAccessRawDataManifest.js";
import { probeCalAccessRawDataZip } from "../pipeline/californiaFinance/calAccessRawDataProbe.js";

export const DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR = "scratch/california-campaign-finance";

export type RefreshCaliforniaCampaignFinanceRawDataScriptOptions = {
  url: string;
  cacheDir: string;
  force: boolean;
  validateManifest: boolean;
  timeoutMs: number;
};

function readValueFlags(args: readonly string[], name: string): string[] {
  const values: string[] = [];
  const inlinePrefix = `${name}=`;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg?.startsWith(inlinePrefix)) {
      values.push(arg.slice(inlinePrefix.length));
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (next && !next.startsWith("--")) {
        values.push(next);
        index += 1;
      }
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

export function parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshCaliforniaCampaignFinanceRawDataScriptOptions {
  return {
    url: parseCalAccessHttpsUrl(readValueFlag(args, "--url")?.trim() || CAL_ACCESS_RAW_DATA_ZIP_URL, "--url"),
    cacheDir: parseLocalPath(readValueFlag(args, "--cache-dir"), DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR, "--cache-dir"),
    force: args.includes("--force"),
    validateManifest: args.includes("--manifest"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshCaliforniaCampaignFinanceRawDataScript(input: {
  options: RefreshCaliforniaCampaignFinanceRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid CAL-ACCESS raw data refresh timestamp");
  }

  const refresh = await refreshCalAccessRawDataArtifactCache({
    cacheDir: input.options.cacheDir,
    url: input.options.url,
    force: input.options.force,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.options.timeoutMs,
    now: startedAt,
  });

  const manifestValidation = input.options.validateManifest
    ? validateCalAccessRawDataManifest(
        await probeCalAccessRawDataZip({
          zipPath: refresh.zipPath,
          selectedFileNames: listCalAccessRawDataManifestFileNames(),
          maxRowsPerFile: 1,
          maxFiles: 50,
        })
      )
    : null;

  return {
    type: "cal_access_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    cache_dir: refresh.cacheDir,
    zip_path: refresh.zipPath,
    metadata_path: refresh.metadataPath,
    status: refresh.status,
    remote: refresh.remote,
    previous: refresh.previous,
    current: refresh.current,
    manifest_validation: manifestValidation,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshCaliforniaCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  const output = await runRefreshCaliforniaCampaignFinanceRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("CAL-ACCESS raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
