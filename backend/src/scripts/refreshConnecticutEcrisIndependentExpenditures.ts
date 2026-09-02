import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isConnecticutEcrisRawDataRefreshEnabled } from "../config/featureFlags.js";
import { DEFAULT_CONNECTICUT_ECRIS_CACHE_DIR } from "../pipeline/connecticutFinance/connecticutEcrisArtifactCache.js";
import { writeConnecticutEcrisIndependentExpenditureCache } from "../pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureCache.js";
import {
  CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_FETCH_TIMEOUT_MS,
  fetchConnecticutEcrisIndependentExpenditures,
} from "../pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureClient.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type RefreshConnecticutEcrisIndependentExpendituresScriptOptions = {
  year: number;
  cacheDir: string;
  force: boolean;
  timeoutMs: number;
};

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values: string[] = [];
  const inlinePrefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg?.startsWith(inlinePrefix)) {
      values.push(arg.slice(inlinePrefix.length).trim());
      continue;
    }
    if (arg === name) {
      values.push((args[index + 1] ?? "").trim());
      index += 1;
    }
  }
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

const KNOWN_BOOLEAN_FLAGS = new Set(["--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms", "--year"]);

export function parseRefreshConnecticutEcrisIndependentExpendituresScriptArgs(
  args: readonly string[]
): RefreshConnecticutEcrisIndependentExpendituresScriptOptions {
  assertKnownCliFlags(args, "Connecticut eCRIS independent expenditure refresh", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    year: parsePositiveInteger(readValueFlag(args, "--year"), new Date().getUTCFullYear(), "--year"),
    cacheDir: resolve(
      readValueFlag(args, "--cache-dir")?.trim() ||
        process.env.CONNECTICUT_ECRIS_CACHE_DIR?.trim() ||
        DEFAULT_CONNECTICUT_ECRIS_CACHE_DIR
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      CONNECTICUT_ECRIS_INDEPENDENT_EXPENDITURE_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshConnecticutEcrisIndependentExpendituresScript(input: {
  options: RefreshConnecticutEcrisIndependentExpendituresScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  const fetchResult = await fetchConnecticutEcrisIndependentExpenditures({
    year: input.options.year,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.options.timeoutMs,
  });
  const written = await writeConnecticutEcrisIndependentExpenditureCache({
    cacheDir: input.options.cacheDir,
    fetchResult,
    now: startedAt,
  });
  const targetedRows = fetchResult.rows.filter(
    (row) => row.supportingCandidates.length > 0 || row.opposingCandidates.length > 0
  );

  return {
    type: "connecticut_ecris_independent_expenditure_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    year: fetchResult.year,
    source_url: fetchResult.sourceUrl,
    file_path: written.filePath,
    row_count: fetchResult.rows.length,
    candidate_targeted_row_count: targetedRows.length,
    search_windows: fetchResult.searchWindows,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshConnecticutEcrisIndependentExpendituresScriptArgs(process.argv.slice(2));

  if (!isConnecticutEcrisRawDataRefreshEnabled(options.force)) {
    console.log("Connecticut eCRIS raw data refresh disabled; no independent expenditures fetched");
    return;
  }

  const output = await runRefreshConnecticutEcrisIndependentExpendituresScript({ options });
  console.log(JSON.stringify(output, null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Connecticut eCRIS independent expenditure refresh failed:", message);
    process.exitCode = 1;
  });
}
