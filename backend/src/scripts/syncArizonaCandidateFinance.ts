import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { isArizonaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  buildArizonaCandidateFinanceSnapshot,
  type ArizonaCandidateFinanceSnapshot,
} from "../pipeline/arizonaFinance/arizonaCandidateFinanceSnapshot.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type SyncArizonaCandidateFinanceScriptOptions = {
  candidateName: string;
  candidateCommitteeId: string;
  electionYear: number;
  candidateFilerId?: string;
  includeOutside: boolean;
  force: boolean;
  timeoutMs?: number;
  directIncomeLimit?: number;
  independentExpenditureLimitPerPosition?: number;
  outsideGroupIncomeLimitPerGroup?: number;
  outsideMaxGroups?: number;
  directMaxBreakdownsPerCategory?: number;
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

function parseRequiredStringFlag(args: readonly string[], name: string): string {
  const value = parseFlagValue(args, name);
  if (!value) {
    throw new Error(`Missing required ${name} flag`);
  }
  return value;
}

function parseOptionalStringFlag(args: readonly string[], name: string): string | undefined {
  return parseFlagValue(args, name) ?? undefined;
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

function parsePositiveIntegerAliasFlag(
  args: readonly string[],
  preferredName: string,
  legacyName: string
): number | undefined {
  const preferred = parsePositiveIntegerFlag(args, preferredName);
  const legacy = parsePositiveIntegerFlag(args, legacyName);
  if (preferred !== undefined && legacy !== undefined) {
    throw new Error(`Provide ${preferredName} or ${legacyName}, not both`);
  }
  return preferred ?? legacy;
}

function parseNonNegativeNumberFlag(args: readonly string[], name: string): number | undefined {
  const raw = parseFlagValue(args, name);
  if (raw === null) {
    return undefined;
  }
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(raw)) {
    throw new Error(`Invalid ${name} value: ${raw}`);
  }
  return Number(raw);
}

function parseElectionYear(args: readonly string[]): number {
  const raw = parseRequiredStringFlag(args, "--year");
  if (!/^\d{4}$/.test(raw)) {
    throw new Error(`Invalid --year value: ${raw}`);
  }
  const year = Number(raw);
  if (year < 2002 || year > 2100) {
    throw new Error(`Invalid --year value: ${raw}`);
  }
  return year;
}

const KNOWN_BOOLEAN_FLAGS = new Set(["--force", "--no-outside", "--skip-outside"]);
const KNOWN_VALUE_FLAGS = new Set(["--candidate-filer-id", "--candidate-name", "--committee-id", "--direct-limit", "--direct-max-breakdowns", "--ie-limit", "--income-limit", "--min-industry-amount", "--outside-income-limit", "--outside-max-breakdowns", "--outside-max-groups", "--timeout-ms", "--year"]);

export function parseSyncArizonaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncArizonaCandidateFinanceScriptOptions {
  assertKnownCliFlags(args, "Arizona candidate finance sync", KNOWN_BOOLEAN_FLAGS, KNOWN_VALUE_FLAGS);
  return {
    candidateName: parseRequiredStringFlag(args, "--candidate-name"),
    candidateCommitteeId: parseRequiredStringFlag(args, "--committee-id"),
    candidateFilerId: parseOptionalStringFlag(args, "--candidate-filer-id"),
    electionYear: parseElectionYear(args),
    includeOutside: !args.includes("--skip-outside") && !args.includes("--no-outside"),
    force: args.includes("--force"),
    timeoutMs: parsePositiveIntegerFlag(args, "--timeout-ms"),
    directIncomeLimit: parsePositiveIntegerAliasFlag(args, "--income-limit", "--direct-limit"),
    independentExpenditureLimitPerPosition: parsePositiveIntegerFlag(args, "--ie-limit"),
    outsideGroupIncomeLimitPerGroup: parsePositiveIntegerFlag(args, "--outside-income-limit"),
    outsideMaxGroups: parsePositiveIntegerFlag(args, "--outside-max-groups"),
    directMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--direct-max-breakdowns"),
    outsideMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--outside-max-breakdowns"),
    minIndustryAmount: parseNonNegativeNumberFlag(args, "--min-industry-amount"),
  };
}

export function toSyncArizonaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncArizonaCandidateFinanceScriptOptions;
  snapshot: ArizonaCandidateFinanceSnapshot;
}) {
  return {
    type: "arizona_candidate_finance_snapshot_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    candidate_name: input.options.candidateName,
    candidate_committee_id: input.options.candidateCommitteeId,
    candidate_filer_id: input.options.candidateFilerId ?? null,
    election_year: input.options.electionYear,
    include_outside: input.options.includeOutside,
    snapshot: input.snapshot,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncArizonaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isArizonaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Arizona campaign finance sync disabled; no Arizona data fetched");
    return;
  }

  const snapshot = await buildArizonaCandidateFinanceSnapshot({
    candidateName: options.candidateName,
    candidateCommitteeId: options.candidateCommitteeId,
    candidateFilerId: options.candidateFilerId,
    electionYear: options.electionYear,
    includeOutside: options.includeOutside,
    directIncomeLimit: options.directIncomeLimit,
    independentExpenditureLimitPerPosition: options.independentExpenditureLimitPerPosition,
    outsideGroupIncomeLimitPerGroup: options.outsideGroupIncomeLimitPerGroup,
    outsideMaxGroups: options.outsideMaxGroups,
    directMaxBreakdownsPerCategory: options.directMaxBreakdownsPerCategory,
    outsideMaxBreakdownsPerCategory: options.outsideMaxBreakdownsPerCategory,
    minIndustryAmount: options.minIndustryAmount,
    spotlightClientOptions: options.timeoutMs ? { timeoutMs: options.timeoutMs } : undefined,
  });

  console.log(JSON.stringify(toSyncArizonaCandidateFinanceScriptOutput({ startedAt, options, snapshot }), null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Arizona candidate finance sync failed:", message);
    process.exitCode = 1;
  });
}
