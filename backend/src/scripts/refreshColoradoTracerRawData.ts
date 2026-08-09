import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  COLORADO_TRACER_CONTRIBUTION_FETCH_TIMEOUT_MS,
  DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR,
  buildColoradoTracerContributionZipUrl,
  parseColoradoTracerHttpsUrl,
  refreshColoradoTracerContributionArtifactCache,
} from "../pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export { DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR };

export type RefreshColoradoTracerRawDataScriptOptions = {
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
    if (arg?.startsWith(inlinePrefix)) {
      values.push(arg.slice(inlinePrefix.length));
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--")) {
        throw new Error(`Missing value for ${name}`);
      }
      values.push(next);
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

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parseRefreshColoradoTracerRawDataScriptArgs(
  args: readonly string[]
): RefreshColoradoTracerRawDataScriptOptions {
  assertKnownCliFlags(args, "Colorado TRACER raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  const year = parseYear(readValueFlag(args, "--year"));
  return {
    year,
    url: parseColoradoTracerHttpsUrl(
      readValueFlag(args, "--url")?.trim() || buildColoradoTracerContributionZipUrl({ year }),
      "--url"
    ),
    cacheDir: parseLocalPath(
      readValueFlag(args, "--cache-dir"),
      DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR,
      "--cache-dir"
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      COLORADO_TRACER_CONTRIBUTION_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshColoradoTracerRawDataScript(input: {
  options: RefreshColoradoTracerRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Colorado TRACER raw data refresh timestamp");
  }

  const refresh = await refreshColoradoTracerContributionArtifactCache({
    year: input.options.year,
    cacheDir: input.options.cacheDir,
    url: input.options.url,
    force: input.options.force,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.options.timeoutMs,
    now: startedAt,
  });

  return {
    type: "colorado_tracer_raw_data_refresh",
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
  const options = parseRefreshColoradoTracerRawDataScriptArgs(process.argv.slice(2));
  const output = await runRefreshColoradoTracerRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Colorado TRACER raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
