import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isMinnesotaCampaignFinanceRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR,
  MINNESOTA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS,
  refreshMinnesotaCampaignFinanceArtifactCache,
} from "../pipeline/minnesotaFinance/minnesotaCampaignFinanceArtifactCache.js";

export type RefreshMinnesotaCampaignFinanceRawDataScriptOptions = {
  cacheDir: string;
  force: boolean;
  timeoutMs: number;
};

const BOOLEAN_FLAGS = new Set(["--force"]);
const VALUE_FLAGS = new Set(["--cache-dir", "--timeout-ms"]);

function validateKnownArgs(args: readonly string[]): void {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index] ?? "";
    const name = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (BOOLEAN_FLAGS.has(name)) {
      if (arg !== name) {
        throw new Error(`Minnesota campaign finance flag does not accept a value: ${name}`);
      }
      continue;
    }
    if (VALUE_FLAGS.has(name)) {
      if (arg === name) {
        index += 1;
      }
      continue;
    }
    if (arg.startsWith("--")) {
      throw new Error(`Unknown Minnesota campaign finance raw data refresh flag: ${name}`);
    }
    throw new Error(`Unexpected Minnesota campaign finance raw data refresh argument: ${arg}`);
  }
}

function readValueFlag(args: readonly string[], name: string): string | undefined {
  const values: string[] = [];
  const inlinePrefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index] ?? "";
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (!value) {
        throw new Error(`Missing value for ${name}`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const value = args[index + 1]?.trim() ?? "";
      if (!value || value.startsWith("--")) {
        throw new Error(`Missing value for ${name}`);
      }
      values.push(value);
      index += 1;
    }
  }
  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0];
}

function parsePositiveInteger(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined) {
    return fallback;
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

export function parseRefreshMinnesotaCampaignFinanceRawDataScriptArgs(
  args: readonly string[]
): RefreshMinnesotaCampaignFinanceRawDataScriptOptions {
  validateKnownArgs(args);
  return {
    cacheDir: resolve(
      readValueFlag(args, "--cache-dir") ??
        (process.env.MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR?.trim() ||
          DEFAULT_MINNESOTA_CAMPAIGN_FINANCE_CACHE_DIR)
    ),
    force: args.includes("--force"),
    timeoutMs: parsePositiveInteger(
      readValueFlag(args, "--timeout-ms"),
      MINNESOTA_CAMPAIGN_FINANCE_FETCH_TIMEOUT_MS,
      "--timeout-ms"
    ),
  };
}

export async function runRefreshMinnesotaCampaignFinanceRawDataScript(input: {
  options: RefreshMinnesotaCampaignFinanceRawDataScriptOptions;
  fetchImpl?: typeof fetch;
  now?: Date;
}) {
  const startedAt = input.now ?? new Date();
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("Invalid Minnesota campaign finance raw data refresh timestamp");
  }

  const refresh = await refreshMinnesotaCampaignFinanceArtifactCache({
    cacheDir: input.options.cacheDir,
    force: input.options.force,
    fetchImpl: input.fetchImpl,
    timeoutMs: input.options.timeoutMs,
    now: startedAt,
  });

  return {
    type: "minnesota_campaign_finance_raw_data_refresh",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    cache_dir: refresh.cacheDir,
    metadata_path: refresh.metadataPath,
    status: refresh.status,
    downloads: refresh.current.downloads,
  };
}

// Run this refresh before the due sync so its three bulk artifacts exist in the shared cache directory.
async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseRefreshMinnesotaCampaignFinanceRawDataScriptArgs(process.argv.slice(2));
  if (!isMinnesotaCampaignFinanceRawDataRefreshEnabled(options.force)) {
    console.log("Minnesota campaign finance raw data refresh disabled; no artifact refreshed");
    return;
  }
  console.log(JSON.stringify(await runRefreshMinnesotaCampaignFinanceRawDataScript({ options }), null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Minnesota campaign finance raw data refresh failed:", message);
    process.exitCode = 1;
  });
}
