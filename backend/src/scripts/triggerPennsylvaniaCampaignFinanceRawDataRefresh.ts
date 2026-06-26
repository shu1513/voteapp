import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import {
  enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob,
  type PennsylvaniaCampaignFinanceRawDataRefreshJobData,
} from "../scheduler/pennsylvaniaCampaignFinanceRawDataRefreshScheduler.js";

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
  const value = raw.trim();
  if (value.length === 0 || !/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(value);
}

export function parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(
  args: readonly string[]
): PennsylvaniaCampaignFinanceRawDataRefreshJobData {
  return {
    year: parsePositiveIntegerFlag(args, "--year"),
    force: args.includes("--force"),
    url: parseFlagValue(args, "--url")?.trim() || undefined,
    cacheDir: parseFlagValue(args, "--cache-dir")?.trim() || undefined,
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const jobData = parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(process.argv.slice(2));
  const jobId = await enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob(jobData);
  if (jobId === "disabled") {
    console.log("Pennsylvania campaign finance raw-data refresh is disabled; job was not enqueued");
    return;
  }
  console.log(
    `Pennsylvania campaign finance raw-data refresh job enqueued (jobId=${jobId} year=${
      jobData.year ?? "current"
    } force=${Boolean(jobData.force)})`
  );
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Pennsylvania campaign finance raw-data refresh trigger failed:", error);
    process.exit(1);
  });
}
