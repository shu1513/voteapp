import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { loadProjectEnv } from "../config/env.js";
import { isCandidateFinanceEnabled } from "../config/featureFlags.js";
import { syncCandidateFinance, type CandidateFinanceSyncResult } from "../pipeline/finance/candidateFinanceSync.js";
import { DEFAULT_OPEN_FEC_TIMEOUT_MS, readOpenFecApiKeysFromEnv } from "../pipeline/presidential/openFecClient.js";

export type SyncCandidateFinanceScriptOptions = {
  fecCandidateId: string;
  electionYear: number;
  dryRun: boolean;
  includeOutside: boolean;
  perPage?: number;
  outsideGroupLimit?: number;
  timeoutMs?: number;
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

function parseElectionYear(args: readonly string[]): number {
  const value = parseRequiredStringFlag(args, "--year");
  if (!/^\d{4}$/.test(value)) {
    throw new Error(`Invalid --year value: ${value}`);
  }
  const year = Number(value);
  if (year < 1970 || year > 2100) {
    throw new Error(`Invalid --year value: ${value}`);
  }
  return year;
}

function parseFecCandidateId(args: readonly string[]): string {
  const value = parseRequiredStringFlag(args, "--fec-id").toUpperCase();
  if (!/^[HPS][0-9A-Z]{8}$/.test(value)) {
    throw new Error(`Invalid --fec-id value: ${value}`);
  }
  return value;
}

export function parseSyncCandidateFinanceScriptArgs(args: readonly string[]): SyncCandidateFinanceScriptOptions {
  return {
    fecCandidateId: parseFecCandidateId(args),
    electionYear: parseElectionYear(args),
    dryRun: args.includes("--dry-run"),
    includeOutside: args.includes("--include-outside"),
    perPage: parsePositiveIntegerFlag(args, "--per-page"),
    outsideGroupLimit: parsePositiveIntegerFlag(args, "--top-groups"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    aiClassifyIndustries: args.includes("--ai-classify-industries"),
    aiClassificationMinAmount: parsePositiveIntegerFlag(args, "--ai-min-amount"),
  };
}

function createDryRunDb(): DryRunDb {
  return {
    async query(): Promise<never> {
      throw new Error("candidate finance dry-run should not execute database queries");
    },
  };
}

function getDatabaseUrl(): string {
  return process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp";
}

export function toSyncCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncCandidateFinanceScriptOptions;
  result: CandidateFinanceSyncResult;
}) {
  return {
    type: "candidate_finance_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    fec_candidate_id: input.options.fecCandidateId,
    election_year: input.options.electionYear,
    dry_run: input.options.dryRun,
    include_outside: input.options.includeOutside,
    ai_classify_industries: input.options.aiClassifyIndustries,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  if (!isCandidateFinanceEnabled()) {
    console.log("candidate_finance sync disabled; no FEC data fetched");
    return;
  }
  const options = parseSyncCandidateFinanceScriptArgs(process.argv.slice(2));

  const apiKeys = readOpenFecApiKeysFromEnv();
  const timeoutMs = options.timeoutMs ?? DEFAULT_OPEN_FEC_TIMEOUT_MS;

  if (apiKeys.length === 0) {
    throw new Error("No OpenFEC API keys configured. Set FEC_API_KEY_1 or FEC_API_KEY.");
  }

  const pool = options.dryRun ? null : new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncCandidateFinance({
      db: pool ?? createDryRunDb(),
      fecCandidateId: options.fecCandidateId,
      electionYear: options.electionYear,
      openFecOptions: { apiKeys, timeoutMs },
      dryRun: options.dryRun,
      includeOutside: options.includeOutside,
      perPage: options.perPage,
      outsideGroupLimit: options.outsideGroupLimit,
      financeIndustryClassifier:
        options.aiClassifyIndustries && !options.dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: options.aiClassificationMinAmount,
      now: startedAt,
    });

    console.log(JSON.stringify(toSyncCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool?.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("candidate finance sync failed:", message);
    process.exitCode = 1;
  });
}
