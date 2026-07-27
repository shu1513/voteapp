import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isIllinoisCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueIllinoisCandidateFinance,
  type IllinoisCandidateFinanceBatchSyncResult,
} from "../pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js";
import {
  createIllinoisSbeArtifactCandidateCommitteeResolver,
  loadIllinoisFinanceDataForDueRowFromArtifacts,
  illinoisMinReceiptYearForLookback,
  loadIllinoisSbeArtifactDataSet,
} from "../pipeline/illinoisFinance/illinoisSbeArtifactDataSource.js";
import { parseFlagValue, parseFlagValues } from "./illinoisCandidateFinanceScriptArgs.js";

export type SyncDueIllinoisCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  timeoutMs?: number;
  contributionCsvPaths: string[];
  expenditureCsvPaths: string[];
  contributionSourceUrl?: string;
  expenditureSourceUrl?: string;
  normalizedArtifactPath?: string;
  receiptsTsvPath?: string;
};

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

function assertNoUnknownFlags(args: readonly string[], allowedFlags: readonly string[]): void {
  const allowed = new Set(allowedFlags);
  for (const arg of args) {
    if (!arg.startsWith("--")) {
      continue;
    }
    const flag = arg.includes("=") ? arg.slice(0, arg.indexOf("=")) : arg;
    if (!allowed.has(flag)) {
      throw new Error(`Unknown option: ${flag}`);
    }
  }
}

export function parseSyncDueIllinoisCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueIllinoisCandidateFinanceScriptOptions {
  assertNoUnknownFlags(args, [
    "--dry-run",
    "--force",
    "--max-candidates",
    "--stale-after-days",
    "--lookback-days",
    "--lookahead-days",
    "--timeout-ms",
    "--contributions-csv",
    "--expenditures-csv",
    "--contributions-url",
    "--expenditures-url",
    "--normalized-artifact",
    "--receipts-tsv",
  ]);
  const contributionCsvPaths = parseFlagValues(args, "--contributions-csv");
  const expenditureCsvPaths = parseFlagValues(args, "--expenditures-csv");
  const contributionSourceUrl = parseFlagValue(args, "--contributions-url") || undefined;
  const expenditureSourceUrl = parseFlagValue(args, "--expenditures-url") || undefined;
  const normalizedArtifactPath = parseFlagValue(args, "--normalized-artifact") || undefined;
  const receiptsTsvPath = parseFlagValue(args, "--receipts-tsv") || undefined;
  // --receipts-tsv is deliberately absent from this guard: the normalized
  // artifact it requires may arrive via ILLINOIS_SBE_NORMALIZED_ARTIFACT_PATH,
  // which argument parsing cannot see. The data-set loader raises the
  // receipts-specific error with full knowledge of the environment.
  if (
    contributionCsvPaths.length === 0 &&
    !normalizedArtifactPath &&
    (expenditureCsvPaths.length > 0 || contributionSourceUrl || expenditureSourceUrl)
  ) {
    throw new Error("Provide --contributions-csv or --normalized-artifact when using Illinois SBE artifact flags");
  }
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    contributionCsvPaths,
    expenditureCsvPaths,
    contributionSourceUrl,
    expenditureSourceUrl,
    normalizedArtifactPath,
    receiptsTsvPath,
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Illinois candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueIllinoisCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueIllinoisCandidateFinanceScriptOptions;
  normalizedArtifactPath?: string;
  receiptsTsvPath?: string;
  result: IllinoisCandidateFinanceBatchSyncResult;
}) {
  const successfulResults = input.result.results.flatMap((item) => (item.ok && item.result ? [item.result] : []));
  const normalizedArtifactPath = input.normalizedArtifactPath ?? input.options.normalizedArtifactPath;
  const receiptsTsvPath = input.receiptsTsvPath ?? input.options.receiptsTsvPath;
  return {
    type: "illinois_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    data_source:
      input.options.contributionCsvPaths.length > 0 || normalizedArtifactPath ? "artifact" : "live",
    artifact_contribution_csv_count: input.options.contributionCsvPaths.length,
    artifact_expenditure_csv_count: input.options.expenditureCsvPaths.length,
    normalized_artifact: Boolean(normalizedArtifactPath),
    receipts_tsv: Boolean(receiptsTsvPath),
    outside_expenditure_data_available_count: successfulResults.filter(
      (result) => result.outsideExpenditureDataAvailable
    ).length,
    outside_group_contribution_data_available_count: successfulResults.filter(
      (result) => result.outsideGroupContributionDataAvailable
    ).length,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueIllinoisCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isIllinoisCampaignFinanceSyncEnabled(options.force)) {
    console.log("Illinois campaign finance due sync disabled; no Illinois data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const normalizedArtifactPath =
      options.normalizedArtifactPath ??
      (process.env.ILLINOIS_SBE_NORMALIZED_ARTIFACT_PATH?.trim() || undefined);
    const receiptsTsvPath =
      options.receiptsTsvPath ?? (process.env.ILLINOIS_SBE_RECEIPTS_TSV_PATH?.trim() || undefined);
    // receiptsTsvPath forces artifact mode too: an env-configured receipts
    // path must either load or fail loudly (the data set requires the
    // normalized artifact alongside it), never fall through to live loading.
    const artifacts =
      options.contributionCsvPaths.length > 0 || normalizedArtifactPath || receiptsTsvPath
        ? await loadIllinoisSbeArtifactDataSet({
            contributionCsvPaths: options.contributionCsvPaths,
            expenditureCsvPaths: options.expenditureCsvPaths,
            contributionSourceUrl: options.contributionSourceUrl,
            expenditureSourceUrl: options.expenditureSourceUrl,
            normalizedArtifactPath,
            receiptsTsvPath,
            minReceiptYear: illinoisMinReceiptYearForLookback({
              now: startedAt,
              electionLookbackDays: options.electionLookbackDays ?? 1,
            }),
          })
        : null;
    const artifactCandidateCommitteeResolver = artifacts
      ? createIllinoisSbeArtifactCandidateCommitteeResolver(artifacts)
      : undefined;
    const result = await syncDueIllinoisCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      sbeClientOptions: options.timeoutMs ? { timeoutMs: options.timeoutMs } : undefined,
      resolveCandidateCommittee: artifactCandidateCommitteeResolver,
      loadIllinoisFinanceDataFn: artifacts
        ? async (row) => loadIllinoisFinanceDataForDueRowFromArtifacts({ row, artifacts })
        : undefined,
    });

    console.log(
      JSON.stringify(
        toSyncDueIllinoisCandidateFinanceScriptOutput({ startedAt, options, normalizedArtifactPath, receiptsTsvPath, result }),
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Illinois candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
