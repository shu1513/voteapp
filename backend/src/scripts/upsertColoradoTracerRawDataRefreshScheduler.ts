import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isColoradoCampaignFinanceEnabled } from "../config/featureFlags.js";
import {
  upsertRecurringColoradoTracerRawDataRefreshJobs,
  type ColoradoTracerRawDataRefreshJobData,
} from "../scheduler/coloradoTracerRawDataRefreshScheduler.js";

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(inlinePrefix));
  if (inline) {
    const value = inline.slice(inlinePrefix.length).trim();
    if (value.length === 0) {
      throw new Error(`Missing ${name} value`);
    }
    return value;
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const next = args[index + 1];
    if (!next || next.startsWith("--") || next.trim().length === 0) {
      throw new Error(`Missing ${name} value`);
    }
    return next.trim();
  }

  return null;
}

function parsePositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

export function parseUpsertColoradoTracerRawDataRefreshSchedulerArgs(
  args: readonly string[]
): ColoradoTracerRawDataRefreshJobData {
  return {
    force: args.includes("--force"),
    year: parsePositiveIntegerFlag(args, "--year"),
    url: parseFlagValue(args, "--url") || undefined,
    cacheDir: parseFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseUpsertColoradoTracerRawDataRefreshSchedulerArgs(process.argv.slice(2));
  const enabled = isColoradoCampaignFinanceEnabled();
  await upsertRecurringColoradoTracerRawDataRefreshJobs(jobData);
  console.log(
    enabled
      ? "Colorado TRACER raw-data refresh recurring scheduler upserted (daily metadata check)"
      : "Colorado TRACER raw-data refresh recurring scheduler disabled; scheduler cleanup completed"
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Colorado TRACER raw-data refresh scheduler upsert failed:", error);
    process.exit(1);
  });
}
