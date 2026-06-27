import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isAlaskaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  ALASKA_APOC_CAMPAIGN_INCOME_URL,
  ALASKA_APOC_IE_CONTRIBUTIONS_URL,
  ALASKA_APOC_IE_EXPENDITURES_URL,
  parseAlaskaApocCampaignIncomeCsv,
  parseAlaskaApocIndependentContributionCsv,
  parseAlaskaApocIndependentExpenditureCsv,
} from "../pipeline/alaskaFinance/alaskaApocClient.js";
import {
  syncAlaskaCandidateFinance,
  type AlaskaCandidateFinanceSyncResult,
} from "../pipeline/alaskaFinance/alaskaCandidateFinanceSync.js";

export type SyncAlaskaCandidateFinanceScriptOptions = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string;
  candidateFilerId: string;
  candidateFilerName: string;
  sourceUrl?: string;
  incomeCsvPath: string;
  independentExpendituresCsvPath?: string;
  independentContributionsCsvPath?: string;
  dryRun: boolean;
  force: boolean;
};

type DryRunDb = {
  query(): Promise<never>;
  connect(): Promise<never>;
};

function parseFlagValue(args: readonly string[], name: string): string | null {
  const inlinePrefix = `${name}=`;
  const values: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith(inlinePrefix)) {
      const value = arg.slice(inlinePrefix.length).trim();
      if (!value) {
        throw new Error(`Missing ${name} value`);
      }
      values.push(value);
      continue;
    }
    if (arg === name) {
      const next = args[index + 1];
      if (!next || next.startsWith("--") || !next.trim()) {
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
  const value = parseFlagValue(args, name)?.trim();
  if (!value) {
    throw new Error(`Missing required ${name} flag`);
  }
  return value;
}

function parseOptionalStringFlag(args: readonly string[], name: string): string | undefined {
  const value = parseFlagValue(args, name)?.trim();
  return value ? value : undefined;
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
  if (year < 2000 || year > 2100) {
    throw new Error(`Invalid --year value: ${value}`);
  }
  return year;
}

export function parseSyncAlaskaCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncAlaskaCandidateFinanceScriptOptions {
  const dryRunFlag = args.includes("--dry-run");
  const writeFlag = args.includes("--write");
  if (dryRunFlag && writeFlag) {
    throw new Error("Provide either --dry-run or --write, not both");
  }

  return {
    candidateId: parseUuidFlag(args, "--candidate-id"),
    electionId: parseUuidFlag(args, "--election-id"),
    candidateName: parseRequiredStringFlag(args, "--candidate-name"),
    electionYear: parseElectionYear(args),
    officeName: parseRequiredStringFlag(args, "--office"),
    district: parseOptionalStringFlag(args, "--district"),
    candidateFilerId: parseRequiredStringFlag(args, "--candidate-filer-id"),
    candidateFilerName: parseRequiredStringFlag(args, "--candidate-filer-name"),
    sourceUrl: parseOptionalStringFlag(args, "--source-url"),
    incomeCsvPath: parseRequiredStringFlag(args, "--income-csv"),
    independentExpendituresCsvPath: parseOptionalStringFlag(args, "--ie-expenditures-csv"),
    independentContributionsCsvPath: parseOptionalStringFlag(args, "--ie-contributions-csv"),
    dryRun: !writeFlag,
    force: args.includes("--force"),
  };
}

function createDryRunDb(): DryRunDb {
  return {
    async query(): Promise<never> {
      throw new Error("Alaska candidate finance dry-run should not execute database queries");
    },
    async connect(): Promise<never> {
      throw new Error("Alaska candidate finance dry-run should not open database transactions");
    },
  };
}

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for Alaska candidate finance sync");
  }
  return databaseUrl;
}

async function readOptionalCsv(path: string | undefined): Promise<string | null> {
  if (!path) {
    return null;
  }
  return readFile(path, "utf8");
}

export function toSyncAlaskaCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncAlaskaCandidateFinanceScriptOptions;
  result: AlaskaCandidateFinanceSyncResult;
}) {
  return {
    type: "alaska_candidate_finance_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    candidate_id: input.options.candidateId,
    election_id: input.options.electionId,
    candidate_name: input.options.candidateName,
    election_year: input.options.electionYear,
    office_name: input.options.officeName,
    candidate_filer_id: input.options.candidateFilerId,
    dry_run: input.options.dryRun,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncAlaskaCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isAlaskaCampaignFinanceSyncEnabled(options.force)) {
    console.log("Alaska campaign finance sync disabled; no Alaska data loaded");
    return;
  }

  const [incomeCsv, expenditureCsv, contributionCsv] = await Promise.all([
    readFile(options.incomeCsvPath, "utf8"),
    readOptionalCsv(options.independentExpendituresCsvPath),
    readOptionalCsv(options.independentContributionsCsvPath),
  ]);
  const pool = options.dryRun ? null : new Pool({ connectionString: getDatabaseUrl() });

  try {
    const result = await syncAlaskaCandidateFinance({
      db: pool ?? createDryRunDb(),
      candidateId: options.candidateId,
      electionId: options.electionId,
      candidateName: options.candidateName,
      electionYear: options.electionYear,
      officeName: options.officeName,
      district: options.district,
      sourceUrl: options.sourceUrl ?? ALASKA_APOC_CAMPAIGN_INCOME_URL,
      incomeRows: parseAlaskaApocCampaignIncomeCsv(incomeCsv, { sourceUrl: ALASKA_APOC_CAMPAIGN_INCOME_URL }),
      independentExpenditureRows: expenditureCsv
        ? parseAlaskaApocIndependentExpenditureCsv(expenditureCsv, { sourceUrl: ALASKA_APOC_IE_EXPENDITURES_URL })
        : [],
      independentContributionRows: contributionCsv
        ? parseAlaskaApocIndependentContributionCsv(contributionCsv, { sourceUrl: ALASKA_APOC_IE_CONTRIBUTIONS_URL })
        : [],
      trustedCommittee: {
        candidateFilerId: options.candidateFilerId,
        candidateFilerName: options.candidateFilerName,
        sourceUrl: options.sourceUrl ?? ALASKA_APOC_CAMPAIGN_INCOME_URL,
      },
      dryRun: options.dryRun,
      now: startedAt,
    });

    console.log(JSON.stringify(toSyncAlaskaCandidateFinanceScriptOutput({ startedAt, options, result }), null, 2));
  } finally {
    await pool?.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Alaska candidate finance sync failed:", message);
    process.exitCode = 1;
  });
}
