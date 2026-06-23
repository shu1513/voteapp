import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { loadProjectEnv } from "../config/env.js";
import { isCaliforniaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  loadCalAccessCommitteeResolutionData,
  loadCalAccessReceiptRowsForCommittees,
} from "../pipeline/californiaFinance/calAccessRawDataLoader.js";
import {
  syncCaliforniaCandidateFinance,
  type CaliforniaCandidateFinanceSyncResult,
} from "../pipeline/californiaFinance/californiaCandidateFinanceSync.js";

export type SyncCaliforniaCandidateFinanceScriptOptions = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  controlledCommitteeId: string;
  controlledCommitteeName: string;
  sourceUrl?: string;
  dryRun: boolean;
  includeOutside: boolean;
  force: boolean;
  timeoutMs?: number;
  rawZipPath?: string;
  rawCacheDir?: string;
  aiClassifyIndustries: boolean;
  aiClassificationMinAmount?: number;
};

type DryRunDb = {
  query(): Promise<never>;
};

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(inlinePrefix));
  if (inline) {
    return inline.slice(inlinePrefix.length);
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const next = args[index + 1];
    if (next && !next.startsWith("--")) {
      return next;
    }
  }

  return null;
}

function parseRequiredStringFlag(args: readonly string[], name: string): string {
  const value = parseFlagValue(args, name)?.trim();
  if (!value) {
    throw new Error(`Missing required ${name} flag`);
  }
  return value;
}

function parseOptionalStringFlag(args: readonly string[], name: string): string | undefined {
  const value = parseFlagValue(args, name)?.trim();
  return value && value.length > 0 ? value : undefined;
}

function parsePositiveIntegerFlag(args: readonly string[], name: string): number | undefined {
  const value = parseFlagValue(args, name)?.trim();
  if (!value) {
    return undefined;
  }
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

function parseUuidFlag(args: readonly string[], name: string): string {
  const value = parseRequiredStringFlag(args, name).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value)) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return value;
}

function parseElectionYear(args: readonly string[]): number {
  const value = parseRequiredStringFlag(args, "--year");
  if (!/^\d{4}$/.test(value)) {
    throw new Error(`Invalid --year value: ${value}`);
  }
  const year = Number(value);
  if (year < 2001 || year > 2100) {
    throw new Error(`Invalid --year value: ${value}`);
  }
  return year;
}

export function parseSyncCaliforniaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncCaliforniaCandidateFinanceScriptOptions {
  return {
    candidateId: parseUuidFlag(args, "--candidate-id"),
    electionId: parseUuidFlag(args, "--election-id"),
    candidateName: parseRequiredStringFlag(args, "--candidate-name"),
    electionYear: parseElectionYear(args),
    officeName: parseRequiredStringFlag(args, "--office"),
    controlledCommitteeId: parseRequiredStringFlag(args, "--committee-id"),
    controlledCommitteeName: parseRequiredStringFlag(args, "--committee-name"),
    sourceUrl: parseOptionalStringFlag(args, "--source-url"),
    dryRun: args.includes("--dry-run"),
    includeOutside: !args.includes("--skip-outside"),
    force: args.includes("--force"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    rawZipPath: parseOptionalStringFlag(args, "--raw-zip"),
    rawCacheDir: parseOptionalStringFlag(args, "--raw-cache-dir"),
    aiClassifyIndustries: !args.includes("--no-ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

function createDryRunDb(): DryRunDb {
  return {
    async query(): Promise<never> {
      throw new Error("California candidate finance dry-run should not execute database queries");
    },
  };
}

function getDatabaseUrl(): string {
  return process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp";
}

export function toSyncCaliforniaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncCaliforniaCandidateFinanceScriptOptions;
  result: CaliforniaCandidateFinanceSyncResult;
}) {
  return {
    type: "california_candidate_finance_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    candidate_id: input.options.candidateId,
    election_id: input.options.electionId,
    candidate_name: input.options.candidateName,
    election_year: input.options.electionYear,
    office_name: input.options.officeName,
    controlled_committee_id: input.options.controlledCommitteeId,
    dry_run: input.options.dryRun,
    include_outside: input.options.includeOutside,
    ai_classify_industries: input.options.aiClassifyIndustries,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncCaliforniaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isCaliforniaCampaignFinanceSyncEnabled(options.force)) {
    console.log("California campaign finance sync disabled; no California data fetched");
    return;
  }

  const pool = options.dryRun ? null : new Pool({ connectionString: getDatabaseUrl() });

  try {
    const resolutionData = await loadCalAccessCommitteeResolutionData({
      zipPath: options.rawZipPath,
      cacheDir: options.rawCacheDir,
    });
    const receiptData = resolutionData
      ? await loadCalAccessReceiptRowsForCommittees({
          zipPath: options.rawZipPath ?? resolutionData.zipPath,
          cacheDir: options.rawCacheDir,
          sourceUrl: resolutionData.sourceUrl,
          committeeIds: [options.controlledCommitteeId],
          campaignCoverRows: resolutionData.campaignCoverRows,
        })
      : null;
    const controlledCommitteeKey = options.controlledCommitteeId.trim().toUpperCase();
    const result = await syncCaliforniaCandidateFinance({
      db: pool ?? createDryRunDb(),
      candidateId: options.candidateId,
      electionId: options.electionId,
      candidateName: options.candidateName,
      electionYear: options.electionYear,
      officeName: options.officeName,
      controlledCommitteeId: options.controlledCommitteeId,
      controlledCommitteeName: options.controlledCommitteeName,
      sourceUrl: options.sourceUrl,
      dryRun: options.dryRun,
      includeOutside: options.includeOutside,
      powerSearchOptions: options.timeoutMs ? { timeoutMs: options.timeoutMs } : undefined,
      directReceiptRows: receiptData?.receiptRowsByCommitteeId.get(controlledCommitteeKey),
      controlledCommitteeFilingIds: receiptData?.controlledCommitteeFilingIdsByCommitteeId.get(controlledCommitteeKey),
      directSourceUrl: receiptData?.sourceUrl ?? resolutionData?.sourceUrl,
      loadOutsideReceiptRowsForCommittees: resolutionData
        ? (committeeIds) =>
            loadCalAccessReceiptRowsForCommittees({
              zipPath: options.rawZipPath ?? resolutionData.zipPath,
              cacheDir: options.rawCacheDir,
              sourceUrl: resolutionData.sourceUrl,
              committeeIds,
              campaignCoverRows: resolutionData.campaignCoverRows,
            })
        : undefined,
      financeIndustryClassifier:
        options.aiClassifyIndustries && !options.dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: options.aiClassificationMinAmount,
      now: startedAt,
    });

    console.log(JSON.stringify(toSyncCaliforniaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool?.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("California candidate finance sync failed:", message);
    process.exitCode = 1;
  });
}
