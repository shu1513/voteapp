import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isOregonCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueOregonCandidateFinance,
  type OregonCandidateFinanceBatchSyncResult,
} from "../pipeline/oregonFinance/oregonCandidateFinanceBatchSync.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type SyncDueOregonCandidateFinanceScriptOptions = {
  dryRun: boolean;
  force: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
};

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || next.trim().length === 0) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(next.trim());
      index += 1;
    }
  }

  if (values.length > 1) {
    throw new Error(`Provide ${name} at most once`);
  }
  return values[0] ?? null;
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

const KNOWN_BOOLEAN_FLAGS = new Set(["--dry-run", "--force"]);
const KNOWN_VALUE_FLAGS = new Set(["--direct-max-breakdowns", "--lookahead-days", "--lookback-days", "--max-candidates", "--min-industry-amount", "--outside-max-breakdowns", "--outside-max-groups", "--stale-after-days"]);

export function parseSyncDueOregonCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncDueOregonCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Oregon candidate finance due sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    maxCandidates: parsePositiveIntegerFlag(args, "--max-candidates"),
    staleAfterDays: parsePositiveIntegerFlag(args, "--stale-after-days"),
    electionLookbackDays: parsePositiveIntegerFlag(args, "--lookback-days"),
    electionLookaheadDays: parsePositiveIntegerFlag(args, "--lookahead-days"),
    directMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--direct-max-breakdowns"),
    outsideMaxGroups: parsePositiveIntegerFlag(args, "--outside-max-groups"),
    outsideMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--outside-max-breakdowns"),
    minIndustryAmount: parsePositiveIntegerFlag(args, "--min-industry-amount"),
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Oregon candidate finance due sync");
  }
  return databaseUrl;
}

export function toSyncDueOregonCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncDueOregonCandidateFinanceScriptOptions;
  result: OregonCandidateFinanceBatchSyncResult;
}) {
  return {
    type: "oregon_candidate_finance_due_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncDueOregonCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isOregonCampaignFinanceSyncEnabled(options.force)) {
    console.log("Oregon campaign finance due sync disabled; no Oregon data loaded");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncDueOregonCandidateFinance({
      db: pool,
      now: startedAt,
      dryRun: options.dryRun,
      maxCandidates: options.maxCandidates,
      staleAfterDays: options.staleAfterDays,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
      directMaxBreakdownsPerCategory: options.directMaxBreakdownsPerCategory,
      outsideMaxGroups: options.outsideMaxGroups,
      outsideMaxBreakdownsPerCategory: options.outsideMaxBreakdownsPerCategory,
      minIndustryAmount: options.minIndustryAmount,
    });

    console.log(JSON.stringify(toSyncDueOregonCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Oregon candidate finance due sync failed:", message);
    process.exitCode = 1;
  });
}
