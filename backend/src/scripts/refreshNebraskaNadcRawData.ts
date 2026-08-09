import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isNebraskaNadcRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_NEBRASKA_NADC_CACHE_DIR,
  NEBRASKA_NADC_FETCH_TIMEOUT_MS,
  buildNebraskaNadcArtifactUrl,
  parseNebraskaNadcHttpsUrl,
  refreshNebraskaNadcArtifactCache,
  type NebraskaNadcArtifactKind,
} from "../pipeline/nebraskaFinance/nebraskaNadcArtifactCache.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export { DEFAULT_NEBRASKA_NADC_CACHE_DIR };

export type RefreshNebraskaNadcRawDataScriptOptions = {
  year: number;
  artifactKind: NebraskaNadcArtifactKind;
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

function parseArtifactKind(value: string | undefined): NebraskaNadcArtifactKind {
  const normalized = value?.trim() || "contribution_loan";
  if (normalized === "contribution_loan" || normalized === "expenditure") {
    return normalized;
  }
  throw new Error(`Invalid --artifact-kind value: ${value ?? ""}`);
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--artifact-kind", "--cache-dir", "--timeout-ms", "--url", "--year"]);

export function parseRefreshNebraskaNadcRawDataScriptArgs(
  args: readonly string[]
): RefreshNebraskaNadcRawDataScriptOptions {
  assertKnownCliFlags(args, "Nebraska NADC raw data refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  const year = parseYear(readValueFlag(args, "--year"));
  const artifactKind = parseArtifactKind(readValueFlag(args, "--artifact-kind"));
  return {
    year,
    artifactKind,
    url: parseNebraskaNadcHttpsUrl(
      readValueFlag(args, "--url")?.trim() || buildNebraskaNadcArtifactUrl({ year, artifactKind }),
      "--url"
    ),
    cacheDir: parseLocalPath(readValueFlag(args, "--cache-dir"), DEFAULT_NEBRASKA_NADC_CACHE_DIR, "--cache-dir"),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(readValueFlag(args, "--timeout-ms"), NEBRASKA_NADC_FETCH_TIMEOUT_MS, "--timeout-ms"),
  };
}

export async function runRefreshNebraskaNadcRawDataScript(input: {
  options: RefreshNebraskaNadcRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Nebraska NADC raw data refresh timestamp");
  }

  const refresh = await refreshNebraskaNadcArtifactCache({
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
    type: "nebraska_nadc_raw_data_refresh",
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
  const options = parseRefreshNebraskaNadcRawDataScriptArgs(process.argv.slice(2));
  if (!isNebraskaNadcRawDataRefreshEnabled(options.force)) {
    console.log("Nebraska NADC raw data refresh disabled; no artifact refreshed");
    return;
  }
  const output = await runRefreshNebraskaNadcRawDataScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Nebraska NADC raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
