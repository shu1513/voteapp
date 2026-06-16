import { Pool } from "pg";
import { createClient } from "redis";
import { pathToFileURL } from "node:url";

import { getPipelineEnv } from "../config/env.js";
import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import {
  enrichPresidentialRosterCycle,
  type PresidentialRosterEnricherResult,
} from "../pipeline/enrichers/presidentialRosterEnricher.js";
import type { PresidentialCycleStage } from "../pipeline/presidential/presidentialCycles.js";

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

type RedisClient = ReturnType<typeof createClient>;

export type EnrichPresidentialRosterScriptOptions = {
  electionYear: number;
  party: string;
  stage: PresidentialCycleStage;
  dryRun: boolean;
  runId: string;
};

function parseFlagValue(args: readonly string[], name: string): string | null {
  const prefix = `${name}=`;
  const inline = args.find((arg) => arg.startsWith(prefix));
  if (inline) {
    return inline.slice(prefix.length);
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

function parseElectionYear(raw: string | null): number {
  if (!raw) {
    throw new Error("Missing required --year flag");
  }
  const year = Number.parseInt(raw, 10);
  if (!Number.isInteger(year) || String(year) !== raw.trim() || year < 2000 || year > 2100 || year % 4 !== 0) {
    throw new Error(`Invalid --year value: ${raw}`);
  }
  return year;
}

function parseStage(raw: string | null): PresidentialCycleStage {
  const stage = raw?.trim().toLowerCase() || "primary";
  if (stage !== "primary" && stage !== "general") {
    throw new Error(`Invalid --stage value: ${raw}`);
  }
  return stage;
}

function parseParty(raw: string | null, stage: PresidentialCycleStage): string {
  const party = raw?.trim() ?? "";
  if (stage !== "primary") {
    throw new Error("presidential roster script currently supports primary cycles only");
  }
  if (party.length === 0) {
    throw new Error("Missing required --party flag for primary presidential roster enrichment");
  }
  return party;
}

function buildRunId(options: { electionYear: number; stage: PresidentialCycleStage; party: string; now: Date }): string {
  const partySlug = options.party
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `presidential_roster:${options.electionYear}:${options.stage}:${partySlug}:${options.now.toISOString()}`;
}

export function parseEnrichPresidentialRosterScriptArgs(
  args: readonly string[],
  now = new Date()
): EnrichPresidentialRosterScriptOptions {
  const stage = parseStage(parseFlagValue(args, "--stage"));
  const electionYear = parseElectionYear(parseFlagValue(args, "--year"));
  const party = parseParty(parseFlagValue(args, "--party"), stage);
  const runId = parseFlagValue(args, "--run-id")?.trim() || buildRunId({ electionYear, stage, party, now });

  return {
    electionYear,
    party,
    stage,
    dryRun: args.includes("--dry-run"),
    runId,
  };
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
    console.warn("presidential roster redis client error:", error);
  });
  await redis.connect();
  return redis;
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
    ai_candidate_count: result.aiCandidateCount,
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
      ...(result.statusVerification.errorCode ? { error_code: result.statusVerification.errorCode } : {}),
      ...(result.statusVerification.error ? { error: result.statusVerification.error } : {}),
    },
  };
}

export function toEnrichPresidentialRosterScriptOutput(input: {
  startedAt: Date;
  options: EnrichPresidentialRosterScriptOptions;
  result: PresidentialRosterEnricherResult;
}) {
  return {
    type: "presidential_roster_enrichment",
    ts: new Date().toISOString(),
    started_at: input.startedAt.toISOString(),
    election_year: input.options.electionYear,
    stage: input.options.stage,
    party: input.options.party,
    dry_run: input.options.dryRun,
    run_id: input.options.runId,
    summary: toScriptSummary(input.result),
    result: input.result,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  if (!isPresidentialElectionsEnabled()) {
    console.log(
      JSON.stringify(
        {
          type: "presidential_roster_enrichment",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          enabled: false,
          summary: { ok: true, skipped: true, reason: "presidential elections disabled" },
        },
        null,
        2
      )
    );
    return;
  }
  const options = parseEnrichPresidentialRosterScriptArgs(process.argv.slice(2), startedAt);

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  let redis: RedisClient | null = null;

  try {
    const redisClient: RedisSendCommandClient = options.dryRun
      ? createDryRunRedisClient()
      : ((redis = await connectRedis(env.REDIS_URL)) as RedisSendCommandClient);

    const result = await enrichPresidentialRosterCycle({
      db: pool,
      redis: redisClient,
      electionYear: options.electionYear,
      stage: options.stage,
      party: options.party,
      runId: options.runId,
      dryRun: options.dryRun,
    });

    console.log(JSON.stringify(toEnrichPresidentialRosterScriptOutput({ startedAt, options, result }), null, 2));
    if (!result.ok) {
      process.exitCode = 1;
    }
  } finally {
    if (redis) {
      await redis.quit();
    }
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("presidential roster enrichment failed:", message);
    process.exitCode = 1;
  });
}
