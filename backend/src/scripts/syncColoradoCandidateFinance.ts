import { stat } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isColoradoCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR,
  buildColoradoTracerContributionZipUrl,
  getColoradoTracerContributionArtifactCachePaths,
  readColoradoTracerContributionArtifactCacheMetadata,
  readColoradoTracerContributionRows,
} from "../pipeline/coloradoFinance/index.js";
import {
  syncColoradoCandidateFinance,
  type ColoradoCandidateFinanceSyncResult,
} from "../pipeline/coloradoFinance/coloradoCandidateFinanceSync.js";

export type SyncColoradoCandidateFinanceScriptOptions = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  committeeId: string;
  committeeName: string;
  tracerCandidateId?: string;
  sourceUrl?: string;
  contributionSourceUrl?: string;
  dryRun: boolean;
  force: boolean;
  rawZipPath?: string;
  rawCacheDir?: string;
  directMaxBreakdownsPerCategory?: number;
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

export function parseSyncColoradoCandidateFinanceScriptArgs(
  args: readonly string[]
): SyncColoradoCandidateFinanceScriptOptions {
  return {
    candidateId: parseUuidFlag(args, "--candidate-id"),
    electionId: parseUuidFlag(args, "--election-id"),
    candidateName: parseRequiredStringFlag(args, "--candidate-name"),
    electionYear: parseElectionYear(args),
    officeName: parseRequiredStringFlag(args, "--office"),
    committeeId: parseRequiredStringFlag(args, "--committee-id"),
    committeeName: parseRequiredStringFlag(args, "--committee-name"),
    tracerCandidateId: parseOptionalStringFlag(args, "--tracer-candidate-id"),
    sourceUrl: parseOptionalStringFlag(args, "--source-url"),
    contributionSourceUrl: parseOptionalStringFlag(args, "--contribution-source-url"),
    dryRun: args.includes("--dry-run"),
    force: args.includes("--force"),
    rawZipPath: parseOptionalStringFlag(args, "--raw-zip"),
    rawCacheDir: parseOptionalStringFlag(args, "--raw-cache-dir"),
    directMaxBreakdownsPerCategory: parsePositiveIntegerFlag(args, "--max-breakdowns"),
  };
}

function createDryRunDb(): DryRunDb {
  return {
    async query(): Promise<never> {
      throw new Error("Colorado candidate finance dry-run should not execute database queries");
    },
  };
}

function getDatabaseUrl(): string {
  return process.env.DATABASE_URL?.trim() || "postgresql://localhost:5432/voteapp";
}

async function ensureFileExists(path: string): Promise<void> {
  const fileStat = await stat(path);
  if (!fileStat.isFile()) {
    throw new Error(`Colorado TRACER contribution ZIP path is not a file: ${path}`);
  }
}

async function resolveContributionZip(input: SyncColoradoCandidateFinanceScriptOptions): Promise<{
  zipPath: string;
  sourceUrl: string;
}> {
  if (input.rawZipPath) {
    await ensureFileExists(input.rawZipPath);
    return {
      zipPath: input.rawZipPath,
      sourceUrl: input.contributionSourceUrl ?? buildColoradoTracerContributionZipUrl({ year: input.electionYear }),
    };
  }

  const paths = getColoradoTracerContributionArtifactCachePaths({
    cacheDir: input.rawCacheDir ?? process.env.COLORADO_TRACER_CONTRIBUTION_CACHE_DIR?.trim() ?? DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR,
    year: input.electionYear,
  });
  await ensureFileExists(paths.zipPath);
  const metadata = await readColoradoTracerContributionArtifactCacheMetadata(paths.metadataPath);
  return {
    zipPath: paths.zipPath,
    sourceUrl:
      input.contributionSourceUrl ??
      metadata?.remote.url ??
      buildColoradoTracerContributionZipUrl({ year: input.electionYear }),
  };
}

export function toSyncColoradoCandidateFinanceScriptOutput(input: {
  startedAt: Date;
  options: SyncColoradoCandidateFinanceScriptOptions;
  result: ColoradoCandidateFinanceSyncResult;
  zipPath: string;
  contributionRowCount: number;
}) {
  return {
    type: "colorado_candidate_finance_sync",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    candidate_id: input.options.candidateId,
    election_id: input.options.electionId,
    candidate_name: input.options.candidateName,
    election_year: input.options.electionYear,
    office_name: input.options.officeName,
    committee_id: input.options.committeeId,
    dry_run: input.options.dryRun,
    raw_zip_path: input.zipPath,
    contribution_rows_loaded: input.contributionRowCount,
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseSyncColoradoCandidateFinanceScriptArgs(process.argv.slice(2));

  if (!isColoradoCampaignFinanceSyncEnabled(options.force)) {
    console.log("Colorado campaign finance sync disabled; no Colorado data loaded");
    return;
  }

  const pool = options.dryRun ? null : new Pool({ connectionString: getDatabaseUrl() });

  try {
    const contributionZip = await resolveContributionZip(options);
    const contributionRows = await readColoradoTracerContributionRows({
      zipPath: contributionZip.zipPath,
      year: options.electionYear,
      predicate: (row) => row.CO_ID.trim().toUpperCase() === options.committeeId.trim().toUpperCase(),
    });
    const result = await syncColoradoCandidateFinance({
      db: pool ?? createDryRunDb(),
      candidateId: options.candidateId,
      electionId: options.electionId,
      candidateName: options.candidateName,
      electionYear: options.electionYear,
      officeName: options.officeName,
      committeeId: options.committeeId,
      committeeName: options.committeeName,
      tracerCandidateId: options.tracerCandidateId,
      sourceUrl: options.sourceUrl,
      contributionRows,
      contributionSourceUrl: contributionZip.sourceUrl,
      dryRun: options.dryRun,
      directMaxBreakdownsPerCategory: options.directMaxBreakdownsPerCategory,
      now: startedAt,
    });

    console.log(
      JSON.stringify(
        toSyncColoradoCandidateFinanceScriptOutput({
          startedAt,
          options,
          result,
          zipPath: contributionZip.zipPath,
          contributionRowCount: contributionRows.length,
        }),
        null,
        2
      )
    );
  } finally {
    await pool?.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Colorado candidate finance sync failed:", message);
    process.exitCode = 1;
  });
}
