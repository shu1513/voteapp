import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isNewHampshireCfsRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_NEW_HAMPSHIRE_CFS_CACHE_DIR,
  normalizeNewHampshireCfsArtifactKind,
  normalizeNewHampshireCfsFilingYear,
  refreshNewHampshireCfsArtifactCache,
  type NewHampshireCfsArtifactKind,
} from "../pipeline/newHampshireFinance/newHampshireCfsArtifactCache.js";
import { NEW_HAMPSHIRE_CFS_FETCH_TIMEOUT_MS } from "../pipeline/newHampshireFinance/newHampshireCfsClient.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type RefreshNewHampshireCampaignFinanceRawDataScriptOptions = {
  filingYear: number;
  artifactKind: NewHampshireCfsArtifactKind;
  cacheDir: string;
  force: boolean;
  timeoutMs: number;
};

const BOOLEAN_FLAGS = new Set(["--force"]);
const VALUE_FLAGS = new Set([
  "--filing-year",
  "--year",
  "--artifact-kind",
  "--cache-dir",
  "--timeout-ms",
]);

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

function parsePositiveInteger(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

export function parseRefreshNewHampshireCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshNewHampshireCampaignFinanceRawDataScriptOptions {
  assertKnownCliFlags(args, "New Hampshire CFS raw data refresh", BOOLEAN_FLAGS, VALUE_FLAGS);
  const yearValues = [
    ...readValueFlags(args, "--filing-year"),
    ...readValueFlags(args, "--year"),
  ];
  if (yearValues.length > 1) throw new Error("Provide --filing-year at most once");

  return {
    filingYear: normalizeNewHampshireCfsFilingYear(
      parsePositiveInteger(yearValues[0], new Date().getUTCFullYear(), "--filing-year")
    ),
    artifactKind: normalizeNewHampshireCfsArtifactKind(
      readValueFlag(args, "--artifact-kind") ?? "contributions"
    ),
    cacheDir: resolve(
      readValueFlag(args, "--cache-dir") ??
        (process.env.NEW_HAMPSHIRE_CFS_RAW_DATA_CACHE_DIR?.trim() ||
          DEFAULT_NEW_HAMPSHIRE_CFS_CACHE_DIR)
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      NEW_HAMPSHIRE_CFS_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshNewHampshireCampaignFinanceRawDataScript(input: {
  options: RefreshNewHampshireCampaignFinanceRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid New Hampshire CFS raw data refresh timestamp");
  }
  const refresh = await refreshNewHampshireCfsArtifactCache({
    ...input.options,
    fetchImpl: input.fetchImpl,
    now: startedAt,
  });
  return {
    type: "new_hampshire_cfs_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    filing_year: input.options.filingYear,
    artifact_kind: input.options.artifactKind,
    cache_dir: refresh.cacheDir,
    file_path: refresh.filePath,
    metadata_path: refresh.metadataPath,
    status: refresh.status,
    previous: refresh.previous,
    current: refresh.current,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshNewHampshireCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isNewHampshireCfsRawDataRefreshEnabled(options.force)) {
    console.log("New Hampshire CFS raw data refresh disabled; no artifact refreshed");
    return;
  }
  console.log(
    JSON.stringify(await runRefreshNewHampshireCampaignFinanceRawDataScript({ options }), null, 2)
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("New Hampshire CFS raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
