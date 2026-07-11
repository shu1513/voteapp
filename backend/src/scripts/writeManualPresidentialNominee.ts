import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import {
  parsePresidentialNomineePayload,
  type PresidentialNomineePayload,
} from "../contracts/presidentialNomineePayloadContract.js";
import { loadProjectEnv } from "../config/env.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import {
  loadActivePresidentialCycleCandidatesForNomineeResolution,
  resolvePresidentialNomineeCandidate,
  type PresidentialNomineeCandidateForResolution,
  type PresidentialNomineeResolutionResult,
} from "../pipeline/presidential/presidentialNomineeResolver.js";
import {
  promotePresidentialNomineeFromResolution,
  type PromotePresidentialNomineeResult,
} from "../pipeline/presidential/presidentialNomineePromotion.js";

import { assertKnownCliFlags } from "./manualCliFlags.js";
type Queryable = Pick<Pool, "query">;

type PresidentialNomineeCyclePreflightRow = {
  id: string;
  election_year: number;
  stage: string;
  party: string | null;
};

type VerifiedPresidentialNomineePrimaryCycleRow = PresidentialNomineeCyclePreflightRow & {
  stage: "primary";
  party: string;
};

export type ManualPresidentialNomineeScriptOptions = {
  cycleId: string;
  electionYear: number;
  party: string;
  file: string;
  dryRun: boolean;
  confirmedAt: Date;
};

export type ManualPresidentialNomineeWriteResult = {
  type: "manual_presidential_nominee_write";
  dryRun: boolean;
  cycleId: string;
  electionYear: number;
  party: string;
  candidateCount: number;
  resolution: PresidentialNomineeResolutionResult;
  promotion: PromotePresidentialNomineeResult | null;
  noAiProviderCall: true;
};

type ManualPresidentialNomineeWriteDeps = {
  loadCycle?: (db: Queryable, cycleId: string) => Promise<PresidentialNomineeCyclePreflightRow | null>;
  loadCandidates?: (
    db: Queryable,
    cycleId: string
  ) => Promise<PresidentialNomineeCandidateForResolution[]>;
  promoteNominee?: typeof promotePresidentialNomineeFromResolution;
};

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:presidential-nominee:write -- --cycle-id uuid --election-year 2028 --party Democratic --file nominee.json [--confirmed-at ISO_DATE] [--dry-run]",
    "",
    "Payload must match PresidentialNomineePayload. Live runs promote only when nominee resolution is a clean match.",
  ].join("\n");
}

function readValueFlag(args: readonly string[], name: string): string | null {
  const prefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(prefix));
  if (inline) {
    const value = inline.slice(prefix.length);
    if (value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }

  const index = args.indexOf(name);
  if (index >= 0) {
    const value = args[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }

  return null;
}

function readBooleanFlag(args: readonly string[], name: string): boolean {
  const inline = args.find((arg) => arg.startsWith(`${name}=`));
  if (inline) {
    throw new Error(`Boolean flag must not include a value: ${name}`);
  }
  const index = args.indexOf(name);
  if (index < 0) {
    return false;
  }
  const next = args[index + 1];
  if (next && !next.startsWith("--")) {
    throw new Error(`Boolean flag must not include a value: ${name}`);
  }
  return true;
}

function normalizeRequiredFlag(raw: string | null, name: string): string {
  const value = raw?.trim() ?? "";
  if (value.length === 0) {
    throw new Error(`Missing ${name}.\n${usage()}`);
  }
  return value;
}

function parseElectionYear(raw: string | null): number {
  if (!raw) {
    throw new Error(`Missing --election-year.\n${usage()}`);
  }
  const year = Number.parseInt(raw, 10);
  if (!Number.isInteger(year) || String(year) !== raw.trim() || year < 2000 || year > 2100 || year % 4 !== 0) {
    throw new Error(`Invalid --election-year value: ${raw}`);
  }
  return year;
}

function parseConfirmedAt(raw: string | null, now: Date): Date {
  if (!raw) {
    return now;
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid --confirmed-at value: ${raw}`);
  }
  return parsed;
}

export function parseManualPresidentialNomineeScriptArgs(
  args: readonly string[],
  now = new Date()
): ManualPresidentialNomineeScriptOptions {
  const cycleId = normalizeRequiredFlag(
    readValueFlag(args, "--cycle-id") ?? readValueFlag(args, "--presidential-cycle-id"),
    "--cycle-id"
  );
  const electionYear = parseElectionYear(readValueFlag(args, "--election-year") ?? readValueFlag(args, "--year"));
  const party = normalizeRequiredFlag(readValueFlag(args, "--party"), "--party");
  const file = normalizeRequiredFlag(readValueFlag(args, "--file"), "--file");

  return {
    cycleId,
    electionYear,
    party,
    file,
    dryRun: readBooleanFlag(args, "--dry-run"),
    confirmedAt: parseConfirmedAt(readValueFlag(args, "--confirmed-at"), now),
  };
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual presidential nominee write`);
  }
  return value;
}

function requireActiveCandidates(
  candidates: readonly PresidentialNomineeCandidateForResolution[],
  cycleId: string
): void {
  if (candidates.length === 0) {
    throw new Error(`No active presidential primary candidates are available for nominee resolution; cycle_id=${cycleId}`);
  }
}

async function loadPresidentialNomineeCyclePreflight(
  db: Queryable,
  cycleId: string
): Promise<PresidentialNomineeCyclePreflightRow | null> {
  const result = await db.query<PresidentialNomineeCyclePreflightRow>(
    `
      SELECT id::text AS id,
             election_year,
             stage,
             party
      FROM public.presidential_cycles
      WHERE id::text = $1
      LIMIT 1
    `,
    [cycleId]
  );
  return result.rows[0] ?? null;
}

function assertNomineeCycleMatchesOptions(
  cycle: PresidentialNomineeCyclePreflightRow | null,
  options: ManualPresidentialNomineeScriptOptions
): asserts cycle is VerifiedPresidentialNomineePrimaryCycleRow {
  if (!cycle) {
    throw new Error(`Presidential primary cycle not found for cycle_id=${options.cycleId}`);
  }
  if (cycle.stage !== "primary") {
    throw new Error(`manual presidential nominee write requires a primary cycle; cycle stage is ${cycle.stage}`);
  }
  if (cycle.election_year !== options.electionYear) {
    throw new Error(
      `--election-year (${options.electionYear}) does not match presidential cycle election_year (${cycle.election_year})`
    );
  }
  const cycleParty = cycle.party?.trim();
  if (!cycleParty) {
    throw new Error(`manual presidential nominee write requires a primary cycle with a party; cycle party is null`);
  }
  if (cycleParty !== options.party.trim()) {
    throw new Error(`--party (${options.party}) does not match presidential cycle party (${cycle.party ?? "null"})`);
  }
}

export async function runManualPresidentialNomineeWrite(input: {
  options: ManualPresidentialNomineeScriptOptions;
  rawPayload: unknown;
  pool: Pool;
  deps?: ManualPresidentialNomineeWriteDeps;
}): Promise<ManualPresidentialNomineeWriteResult> {
  const parsed = parsePresidentialNomineePayload(input.rawPayload);
  if (!parsed.ok) {
    throw new Error(`Presidential nominee payload failed validation: ${parsed.reason}`);
  }

  const loadCycle = input.deps?.loadCycle ?? loadPresidentialNomineeCyclePreflight;
  const loadCandidates = input.deps?.loadCandidates ?? loadActivePresidentialCycleCandidatesForNomineeResolution;
  const promoteNominee = input.deps?.promoteNominee ?? promotePresidentialNomineeFromResolution;
  const cycle = await loadCycle(input.pool, input.options.cycleId);
  assertNomineeCycleMatchesOptions(cycle, input.options);
  const candidates = await loadCandidates(input.pool, input.options.cycleId);
  const resolution = resolvePresidentialNomineeCandidate({
    payload: parsed.payload,
    candidates,
  });
  if (resolution.status !== "no_nominee_found") {
    requireActiveCandidates(candidates, input.options.cycleId);
  }

  const promotion =
    input.options.dryRun || resolution.status !== "matched"
      ? null
      : await promoteNominee({
          db: input.pool,
          primaryCycleId: cycle.id,
          electionYear: cycle.election_year,
          party: cycle.party,
          resolution,
          confirmedAt: input.options.confirmedAt,
        });

  return {
    type: "manual_presidential_nominee_write",
    dryRun: input.options.dryRun,
    cycleId: cycle.id,
    electionYear: cycle.election_year,
    party: cycle.party,
    candidateCount: candidates.length,
    resolution,
    promotion,
    noAiProviderCall: true,
  };
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:presidential-nominee:write", process.argv.slice(2), [{ name: "--cycle-id", value: "both" }, { name: "--presidential-cycle-id", value: "both" }, { name: "--election-year", value: "both" }, { name: "--year", value: "both" }, { name: "--party", value: "both" }, { name: "--confirmed-at", value: "both" }, { name: "--file", value: "both" }, { name: "--dry-run", value: "none" }]);
  loadProjectEnv();
  const options = parseManualPresidentialNomineeScriptArgs(process.argv.slice(2));
  const rawPayload = await readJsonFile(options.file);
  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });

  try {
    const result = await runManualPresidentialNomineeWrite({
      options,
      rawPayload,
      pool,
    });
    console.log(JSON.stringify(result, null, 2));
    if (!options.dryRun && (result.resolution.status === "unmatched" || result.resolution.status === "ambiguous")) {
      process.exitCode = 1;
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual presidential nominee write failed:", message);
    process.exitCode = 1;
  });
}
