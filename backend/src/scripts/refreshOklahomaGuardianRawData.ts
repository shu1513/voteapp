import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isOklahomaGuardianRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_OKLAHOMA_GUARDIAN_CONTRIBUTION_CACHE_DIR,
  OKLAHOMA_GUARDIAN_CONTRIBUTION_FETCH_TIMEOUT_MS,
  buildOklahomaGuardianContributionZipUrl,
  parseOklahomaGuardianHttpsUrl,
  refreshOklahomaGuardianContributionArtifactCache,
} from "../pipeline/oklahomaFinance/oklahomaGuardianContributionArtifactCache.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { readStrictFlagValues } from "../utils/cliFlags.js";

export { DEFAULT_OKLAHOMA_GUARDIAN_CONTRIBUTION_CACHE_DIR };

export type RefreshOklahomaGuardianRawDataScriptOptions = {
  year: number;
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

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parseRefreshOklahomaGuardianRawDataScriptArgs(
  args: readonly string[]
): RefreshOklahomaGuardianRawDataScriptOptions {
  assertKnownCliFlags(args, "Oklahoma Guardian raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  const year = parseYear(readValueFlag(args, "--year"));
  return {
    year,
    url: parseOklahomaGuardianHttpsUrl(
      readValueFlag(args, "--url")?.trim() || buildOklahomaGuardianContributionZipUrl({ year }),
      "--url"
    ),
    cacheDir: parseLocalPath(
      readValueFlag(args, "--cache-dir"),
      DEFAULT_OKLAHOMA_GUARDIAN_CONTRIBUTION_CACHE_DIR,
      "--cache-dir"
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      OKLAHOMA_GUARDIAN_CONTRIBUTION_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshOklahomaGuardianRawDataScript(input: {
  options: RefreshOklahomaGuardianRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Oklahoma Guardian raw data refresh timestamp");
  }

  const refresh = await refreshOklahomaGuardianContributionArtifactCache({
    year: input.options.year,
    cacheDir: input.options.cacheDir,
    url: input.options.url,
    force: input.options.force,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.options.timeoutMs,
    now: startedAt,
  });

  return {
    type: "oklahoma_guardian_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    year: input.options.year,
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
  const options = parseRefreshOklahomaGuardianRawDataScriptArgs(process.argv.slice(2));
  if (!isOklahomaGuardianRawDataRefreshEnabled(options.force)) {
    console.log("Oklahoma Guardian raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshOklahomaGuardianRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Oklahoma Guardian raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
