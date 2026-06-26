import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import type { MaineCfisArtifactKind } from "../pipeline/maineFinance/maineCfisClient.js";
import {
  enqueueManualMaineCfisRawDataRefreshJob,
  type MaineCfisRawDataRefreshJobData,
} from "../scheduler/maineCfisRawDataRefreshScheduler.js";

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

function parseFilingYearFlag(args: readonly string[]): number | undefined {
  const filingYear = parsePositiveIntegerFlag(args, "--filing-year");
  const year = parsePositiveIntegerFlag(args, "--year");
  if (filingYear !== undefined && year !== undefined) {
    throw new Error("Provide --filing-year at most once");
  }
  return filingYear ?? year;
}

function parseArtifactKindFlag(args: readonly string[]): MaineCfisArtifactKind | undefined {
  const raw = parseFlagValue(args, "--artifact-kind");
  if (raw === null) {
    return undefined;
  }
  if (raw !== "contributions" && raw !== "expenditures") {
    throw new Error(`Invalid --artifact-kind value: ${raw}`);
  }
  return raw;
}

export function parseMaineCfisRawDataRefreshTriggerArgs(args: readonly string[]): MaineCfisRawDataRefreshJobData {
  return {
    force: args.includes("--force"),
    filingYear: parseFilingYearFlag(args),
    artifactKind: parseArtifactKindFlag(args),
    url: parseFlagValue(args, "--url") || undefined,
    cacheDir: parseFlagValue(args, "--cache-dir") || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parseMaineCfisRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualMaineCfisRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("Maine CFIS raw-data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(`Maine CFIS raw-data refresh job enqueued (jobId=${jobId} force=${Boolean(jobData.force)})`);
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Maine CFIS raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
