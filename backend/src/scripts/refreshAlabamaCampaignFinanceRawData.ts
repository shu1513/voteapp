import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isAlabamaFcpaRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_ALABAMA_FCPA_CACHE_DIR,
  normalizeAlabamaExtractKind,
  normalizeAlabamaExtractYear,
  refreshAlabamaFcpaArtifactCache,
  type AlabamaExtractKind,
} from "../pipeline/alabamaFinance/alabamaFcpaArtifactCache.js";
import {
  getAlabamaExtractCatalog,
  type AlabamaFcpaClientOptions,
} from "../pipeline/alabamaFinance/alabamaFcpaClient.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";
import { readStrictFlagValues } from "../utils/cliFlags.js";

export type RefreshAlabamaCampaignFinanceRawDataScriptOptions = {
  /** Transaction-date years to refresh, deduped, in argument order. */
  years: number[];
  artifactKind: AlabamaExtractKind;
  cacheDir: string;
  /** Bypasses only the refresh feature sub-gate, never cache safety. */
  force: boolean;
  /**
   * Forwarded as the cache's force: lets a 0-row extract displace a
   * populated artifact. Deliberately separate from --force so routine
   * gate-bypass runs keep the last-good-artifact guarantee.
   */
  acceptEmpty: boolean;
  timeoutMs: number | undefined;
};

const BOOLEAN_FLAGS = new Set(["--force", "--accept-empty"]);
const VALUE_FLAGS = new Set(["--year", "--artifact-kind", "--cache-dir", "--timeout-ms"]);

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values = readStrictFlagValues(args, name);
  if (values.length > 1) throw new Error(`Provide ${name} at most once`);
  return values[0];
}

function parseYear(value: string): number {
  if (!/^\d{4}$/.test(value)) {
    throw new Error(`Invalid --year value: ${value}`);
  }
  return normalizeAlabamaExtractYear(Number(value));
}

export function parseRefreshAlabamaCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshAlabamaCampaignFinanceRawDataScriptOptions {
  assertKnownCliFlags(args, "Alabama FCPA raw data refresh", BOOLEAN_FLAGS, VALUE_FLAGS);
  // The bucket window spans transaction-date years (2024 rows live in the
  // 2024 file even for 2025-registered committees), so --year repeats to
  // refresh a whole window off one catalog read.
  const yearValues = readStrictFlagValues(args, "--year");
  const years = [...new Set(yearValues.map(parseYear))];
  if (years.length === 0) {
    years.push(normalizeAlabamaExtractYear(new Date().getUTCFullYear()));
  }

  const timeoutValue = readValueFlag(args, "--timeout-ms");
  if (timeoutValue !== undefined && !/^[1-9]\d*$/.test(timeoutValue)) {
    throw new Error(`Invalid --timeout-ms value: ${timeoutValue}`);
  }

  return {
    years,
    artifactKind: normalizeAlabamaExtractKind(readValueFlag(args, "--artifact-kind") ?? "cash"),
    cacheDir: resolve(
      readValueFlag(args, "--cache-dir") ??
        (process.env.ALABAMA_FCPA_RAW_DATA_CACHE_DIR?.trim() || DEFAULT_ALABAMA_FCPA_CACHE_DIR)
    ),
    force: args.includes("--force"),
    acceptEmpty: args.includes("--accept-empty"),
    timeoutMs: timeoutValue === undefined ? undefined : Number(timeoutValue),
  };
}

export async function runRefreshAlabamaCampaignFinanceRawDataScript(input: {
  options: RefreshAlabamaCampaignFinanceRawDataScriptOptions;
  clientOptions?: AlabamaFcpaClientOptions;
}) {
  const clientOptions: AlabamaFcpaClientOptions = {
    ...input.clientOptions,
    ...(input.options.timeoutMs === undefined ? {} : { timeoutMs: input.options.timeoutMs }),
  };
  // One catalog read for the whole run: download ids are unstable, but they
  // are stable within a single catalog snapshot.
  const catalog = await getAlabamaExtractCatalog(clientOptions);
  const artifacts = [];
  for (const year of input.options.years) {
    const refresh = await refreshAlabamaFcpaArtifactCache({
      kind: input.options.artifactKind,
      year,
      cacheDir: input.options.cacheDir,
      catalog,
      force: input.options.acceptEmpty,
      clientOptions,
    });
    artifacts.push({
      year,
      status: refresh.status,
      file_path: refresh.filePath,
      metadata_path: refresh.metadataPath,
      record_count: refresh.current.recordCount,
      quarantined_count: refresh.current.quarantinedCount,
      csv_sha256: refresh.current.csvSha256,
      source_last_updated: refresh.current.source.lastUpdatedRaw,
    });
  }
  return {
    type: "alabama_fcpa_raw_data_refresh",
    ts: new Date().toISOString(),
    artifact_kind: input.options.artifactKind,
    cache_dir: input.options.cacheDir,
    artifacts,
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshAlabamaCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isAlabamaFcpaRawDataRefreshEnabled(options.force)) {
    console.log("Alabama FCPA raw data refresh disabled; no artifact refreshed");
    return;
  }
  console.log(
    JSON.stringify(await runRefreshAlabamaCampaignFinanceRawDataScript({ options }), null, 2)
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Alabama FCPA raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
