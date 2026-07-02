import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";
import { createClient } from "redis";

import type {
  PresidentialRosterAiConfig,
  PresidentialRosterAiInput,
  PresidentialRosterAiResult,
} from "../ai/enrichPresidentialRoster.js";
import type {
  PresidentialRosterStatusAiInput,
  PresidentialRosterStatusAiResult,
} from "../ai/enrichPresidentialRosterStatus.js";
import { loadProjectEnv } from "../config/env.js";
import {
  parsePresidentialRosterPayload,
  type PresidentialRosterPayload,
  type PresidentialRosterSkippedCandidate,
} from "../contracts/presidentialRosterPayloadContract.js";
import {
  enrichPresidentialRosterCycle,
  type PresidentialRosterEnricherInput,
  type PresidentialRosterEnricherResult,
} from "../pipeline/enrichers/presidentialRosterEnricher.js";

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

type RedisClient = ReturnType<typeof createClient>;

type PresidentialRosterCyclePreflightRow = {
  id: string;
  election_year: number;
  stage: string;
  party: string | null;
};

export type ManualPresidentialRosterScriptOptions = {
  cycleId: string;
  electionYear: number;
  party: string;
  file: string;
  dryRun: boolean;
  runId: string;
};

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:presidential-roster:write -- --cycle-id uuid --election-year 2028 --party Democratic --file roster.json [--run-id id] [--dry-run]",
    "",
    "Payload must match the presidential roster AI payload shape.",
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

function normalizeRequiredFlag(raw: string | null, name: string): string {
  const value = raw?.trim() ?? "";
  if (value.length === 0) {
    throw new Error(`Missing ${name}.\n${usage()}`);
  }
  return value;
}

function buildRunId(options: { electionYear: number; party: string; now: Date }): string {
  const partySlug = options.party
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `manual_presidential_roster:${options.electionYear}:primary:${partySlug}:${options.now.toISOString()}`;
}

export function parseManualPresidentialRosterScriptArgs(
  args: readonly string[],
  now = new Date()
): ManualPresidentialRosterScriptOptions {
  const electionYear = parseElectionYear(readValueFlag(args, "--election-year") ?? readValueFlag(args, "--year"));
  const party = normalizeRequiredFlag(readValueFlag(args, "--party"), "--party");
  const cycleId = normalizeRequiredFlag(readValueFlag(args, "--cycle-id"), "--cycle-id");
  const file = normalizeRequiredFlag(readValueFlag(args, "--file"), "--file");
  const dryRun = readBooleanFlag(args, "--dry-run");
  const runId = readValueFlag(args, "--run-id")?.trim() || buildRunId({ electionYear, party, now });

  return {
    cycleId,
    electionYear,
    party,
    file,
    dryRun,
    runId,
  };
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual presidential roster write`);
  }
  return value;
}

function createDryRunRedisClient(): RedisSendCommandClient {
  return {
    async sendCommand(): Promise<unknown> {
      return 0;
    },
  };
}

async function connectRedis(redisUrl: string): Promise<RedisClient> {
  const redis = createClient({ url: redisUrl });
  redis.on("error", (error) => {
    console.warn("manual presidential roster redis client error:", error);
  });
  await redis.connect();
  return redis;
}

async function loadCyclePreflight(pool: Pool, cycleId: string): Promise<PresidentialRosterCyclePreflightRow | null> {
  const result = await pool.query<PresidentialRosterCyclePreflightRow>(
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

function assertCycleMatchesOptions(
  cycle: PresidentialRosterCyclePreflightRow | null,
  options: ManualPresidentialRosterScriptOptions
): asserts cycle is PresidentialRosterCyclePreflightRow {
  if (!cycle) {
    throw new Error(`Presidential cycle not found for cycle_id=${options.cycleId}`);
  }
  if (cycle.stage !== "primary") {
    throw new Error(
      `manual presidential roster write currently supports primary cycles only; cycle stage is ${cycle.stage}`
    );
  }
  if (cycle.election_year !== options.electionYear) {
    throw new Error(
      `--election-year (${options.electionYear}) does not match presidential cycle election_year (${cycle.election_year})`
    );
  }
  if ((cycle.party ?? "").trim() !== options.party.trim()) {
    throw new Error(
      `--party (${options.party}) does not match presidential cycle party (${cycle.party ?? "null"})`
    );
  }
}

export function buildManualPresidentialRosterEnrichResult(input: {
  payload: PresidentialRosterPayload;
  file: string;
  skippedIneligibleCandidates?: readonly PresidentialRosterSkippedCandidate[];
}): PresidentialRosterAiResult {
  const skipped = input.skippedIneligibleCandidates ?? [];
  return {
    ok: true,
    provider: "manual",
    model: "manual-research:codex",
    candidates: input.payload.candidates,
    aiRawDebug: {
      manual_research: true,
      no_ai_provider_call: true,
      source_file: input.file,
      candidate_count: input.payload.candidates.length,
      ...(skipped.length > 0 ? { roster_skipped_ineligible: [...skipped] } : {}),
    },
  };
}

function noAiStatusVerifier(input: PresidentialRosterStatusAiInput): PresidentialRosterStatusAiResult {
  return {
    ok: true,
    provider: "manual",
    model: "manual-research:codex",
    candidates: input.candidates.map((candidate) => ({
      candidate_id: candidate.candidateId,
      status: "active",
      reason:
        "Manual no-AI roster rerun did not include this existing active candidate; leaving active until a source-backed withdrawn payload is provided.",
      sources: [...(candidate.sources ?? [])],
    })),
    aiRawDebug: {
      manual_research: true,
      no_ai_provider_call: true,
      skipped_status_verification: true,
      omitted_candidate_count: input.candidates.length,
    },
  };
}

function toScriptSummary(result: PresidentialRosterEnricherResult) {
  if (!result.ok) {
    return {
      ok: false,
      error_code: result.errorCode,
      retryable: result.retryable,
    };
  }

  return {
    ok: true,
    candidate_count: result.aiCandidateCount,
    matched_count: result.matchedCount,
    ambiguous_count: result.ambiguousCount,
    unmatched_count: result.unmatchedCount,
    withdrawn_skipped_count: result.withdrawnSkippedCount,
    withdrawn_demoted_count: result.withdrawnDemotedCount,
    emitted_count: result.emittedCount,
    skipped_count: result.skippedCount,
    status_verification: {
      checked_count: result.statusVerification.checkedCount,
      withdrawn_count: result.statusVerification.withdrawnCount,
      active_count: result.statusVerification.activeCount,
      skipped_count: result.statusVerification.skippedCount,
      demoted_count: result.statusVerification.demotedCount,
      no_ai_provider_call: true,
    },
  };
}

export function toManualPresidentialRosterScriptOutput(input: {
  startedAt: Date;
  options: ManualPresidentialRosterScriptOptions;
  result: PresidentialRosterEnricherResult;
}) {
  return {
    type: "manual_presidential_roster_write",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    cycle_id: input.options.cycleId,
    election_year: input.options.electionYear,
    stage: "primary",
    party: input.options.party,
    dry_run: input.options.dryRun,
    run_id: input.options.runId,
    no_ai_provider_call: true,
    summary: toScriptSummary(input.result),
    result: input.result,
  };
}

export async function runManualPresidentialRosterWrite(input: {
  options: ManualPresidentialRosterScriptOptions;
  rawPayload: unknown;
  pool: Pool;
  redis: RedisSendCommandClient;
  enrichRosterCycle?: (input: PresidentialRosterEnricherInput) => Promise<PresidentialRosterEnricherResult>;
}): Promise<PresidentialRosterEnricherResult> {
  const parsed = parsePresidentialRosterPayload(input.rawPayload, {
    expectedParty: input.options.party,
  });
  if (!parsed.ok) {
    return {
      ok: false,
      cycleId: input.options.cycleId,
      electionYear: input.options.electionYear,
      stage: "primary",
      party: input.options.party,
      error: `Presidential roster payload failed validation: ${parsed.reason}`,
      retryable: false,
      errorCode: "SCHEMA_MISMATCH",
    };
  }

  const cycle = await loadCyclePreflight(input.pool, input.options.cycleId);
  assertCycleMatchesOptions(cycle, input.options);

  const enrichRosterCycle = input.enrichRosterCycle ?? enrichPresidentialRosterCycle;

  return enrichRosterCycle({
    db: input.pool,
    redis: input.redis,
    cycleId: input.options.cycleId,
    electionYear: input.options.electionYear,
    stage: "primary",
    party: input.options.party,
    runId: input.options.runId,
    dryRun: input.options.dryRun,
    aiConfig: { timeoutMs: 90_000 },
    enrichRoster: async (
      _aiInput: PresidentialRosterAiInput,
      _config: PresidentialRosterAiConfig
    ): Promise<PresidentialRosterAiResult> =>
      buildManualPresidentialRosterEnrichResult({
        payload: parsed.payload,
        file: input.options.file,
        skippedIneligibleCandidates: parsed.skippedIneligibleCandidates,
      }),
    enrichRosterStatus: async (statusInput) => noAiStatusVerifier(statusInput),
  });
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseManualPresidentialRosterScriptArgs(process.argv.slice(2), startedAt);
  const rawPayload = await readJsonFile(options.file);

  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });
  let redis: RedisClient | null = null;

  try {
    const redisClient: RedisSendCommandClient = options.dryRun
      ? createDryRunRedisClient()
      : ((redis = await connectRedis(requireEnv("REDIS_URL"))) as RedisSendCommandClient);
    const result = await runManualPresidentialRosterWrite({
      options,
      rawPayload,
      pool,
      redis: redisClient,
    });
    console.log(JSON.stringify(toManualPresidentialRosterScriptOutput({ startedAt, options, result }), null, 2));
    if (!result.ok) {
      process.exitCode = 1;
    }
  } finally {
    if (redis) {
      await redis.quit().catch(() => undefined);
    }
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual presidential roster write failed:", message);
    process.exitCode = 1;
  });
}
