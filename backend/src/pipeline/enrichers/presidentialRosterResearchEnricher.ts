import { Worker, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import {
  PRESIDENTIAL_ROSTER_RESEARCH_JOB_NAME,
  type PresidentialRosterResearchJobData,
} from "../producers/presidentialRosterResearchProducer.js";
import {
  markPresidentialRosterResearchError,
  markPresidentialRosterResearchSuccess,
} from "../presidential/presidentialRosterResearchWriter.js";
import {
  enrichPresidentialRosterCycle,
  type PresidentialRosterEnricherInput,
  type PresidentialRosterEnricherResult,
} from "./presidentialRosterEnricher.js";

export type PresidentialRosterResearchEnricherOptions = {
  once?: boolean;
  blockMs?: number;
  concurrency?: number;
};

export type PresidentialRosterResearchJobResult = {
  cycle_id: string;
  election_year: number;
  party: string;
  ok: boolean;
  rows_updated: number;
  next_research_at: string | null;
  provider?: string;
  model?: string;
  ai_candidate_count?: number;
  matched_count?: number;
  emitted_count?: number;
  skipped_count?: number;
  error?: string;
  error_code?: string;
};

type RedisSendCommandClient = {
  sendCommand(args: string[]): Promise<unknown>;
};

type RedisClient = ReturnType<typeof createClient>;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Unsupported REDIS_URL protocol: ${parsed.protocol}`);
  }
  const parsedPort = parsed.port ? Number.parseInt(parsed.port, 10) : 6379;
  const parsedDb = parsed.pathname.length > 1 ? Number.parseInt(parsed.pathname.slice(1), 10) : 0;

  if (!Number.isInteger(parsedPort) || parsedPort <= 0) {
    throw new Error(`Invalid REDIS_URL port: ${parsed.port}`);
  }
  if (!Number.isInteger(parsedDb) || parsedDb < 0) {
    throw new Error(`Invalid REDIS_URL db index: ${parsed.pathname}`);
  }

  const opts: ConnectionOptions = {
    host: parsed.hostname,
    port: parsedPort,
    db: parsedDb,
    maxRetriesPerRequest: null,
  };
  if (parsed.username) {
    opts.username = decodeURIComponent(parsed.username);
  }
  if (parsed.password) {
    opts.password = decodeURIComponent(parsed.password);
  }
  if (parsed.protocol === "rediss:") {
    opts.tls = {};
  }
  return opts;
}

function getQueueName(): string {
  return process.env.PRESIDENTIAL_ROSTER_RESEARCH_QUEUE?.trim() || "presidential_roster_research";
}

function assertValidJob(job: PresidentialRosterResearchJobData): PresidentialRosterResearchJobData {
  if (!UUID_PATTERN.test(job.cycle_id)) {
    throw new Error(`Invalid presidential roster research job cycle_id: ${job.cycle_id}`);
  }
  if (
    !Number.isInteger(job.election_year) ||
    job.election_year < 2000 ||
    job.election_year > 2100 ||
    job.election_year % 4 !== 0
  ) {
    throw new Error(`Invalid presidential roster research job election_year: ${job.election_year}`);
  }
  if (job.stage !== "primary") {
    throw new Error(`Unsupported presidential roster research job stage: ${job.stage}`);
  }
  const party = job.party.trim();
  if (party.length === 0) {
    throw new Error("Presidential roster research job party is required");
  }
  const scheduledFor = new Date(job.scheduled_for);
  if (Number.isNaN(scheduledFor.getTime())) {
    throw new Error(`Invalid presidential roster research job scheduled_for: ${job.scheduled_for}`);
  }
  return {
    ...job,
    party,
    scheduled_for: scheduledFor.toISOString(),
  };
}

async function connectRedis(redisUrl: string): Promise<RedisClient> {
  const redis = createClient({ url: redisUrl });
  redis.on("error", (error) => {
    console.warn("presidential roster research redis client error:", error);
  });
  await redis.connect();
  return redis;
}

function summarizeSuccess(
  job: PresidentialRosterResearchJobData,
  result: Extract<PresidentialRosterEnricherResult, { ok: true }>,
  writeResult: { rowsUpdated: number; nextResearchAt: string | null }
): PresidentialRosterResearchJobResult {
  return {
    cycle_id: job.cycle_id,
    election_year: job.election_year,
    party: job.party,
    ok: true,
    rows_updated: writeResult.rowsUpdated,
    next_research_at: writeResult.nextResearchAt,
    provider: result.provider,
    model: result.model,
    ai_candidate_count: result.aiCandidateCount,
    matched_count: result.matchedCount,
    emitted_count: result.emittedCount,
    skipped_count: result.skippedCount,
  };
}

function summarizeFailure(
  job: PresidentialRosterResearchJobData,
  result: Extract<PresidentialRosterEnricherResult, { ok: false }>,
  writeResult: { rowsUpdated: number; nextResearchAt: string | null }
): PresidentialRosterResearchJobResult {
  return {
    cycle_id: job.cycle_id,
    election_year: job.election_year,
    party: job.party,
    ok: false,
    rows_updated: writeResult.rowsUpdated,
    next_research_at: writeResult.nextResearchAt,
    error: result.error,
    error_code: result.errorCode,
  };
}

export async function processPresidentialRosterResearchJob(
  rawJob: PresidentialRosterResearchJobData,
  options: {
    pool?: Pool;
    redis?: RedisSendCommandClient;
    researchedAt?: Date;
    enrichRosterCycle?: (input: PresidentialRosterEnricherInput) => Promise<PresidentialRosterEnricherResult>;
  } = {}
): Promise<PresidentialRosterResearchJobResult> {
  const job = assertValidJob(rawJob);
  const env = getPipelineEnv();
  const pool = options.pool ?? new Pool({ connectionString: env.DATABASE_URL });
  const shouldClosePool = !options.pool;
  let redis: RedisClient | null = null;
  const researchedAt = options.researchedAt ?? new Date();
  const enrichRosterCycle = options.enrichRosterCycle ?? enrichPresidentialRosterCycle;

  try {
    const redisClient =
      options.redis ?? ((redis = await connectRedis(env.REDIS_URL)) as RedisSendCommandClient);
    const result = await enrichRosterCycle({
      db: pool,
      redis: redisClient,
      electionYear: job.election_year,
      stage: job.stage,
      party: job.party,
      runId: job.run_id,
    });

    if (!result.ok) {
      const writeResult = await markPresidentialRosterResearchError(pool, {
        cycleId: job.cycle_id,
        electionYear: job.election_year,
        researchedAt,
        error: result.error,
      });
      return summarizeFailure(job, result, writeResult);
    }

    const writeResult = await markPresidentialRosterResearchSuccess(pool, {
      cycleId: job.cycle_id,
      electionYear: job.election_year,
      researchedAt,
    });
    return summarizeSuccess(job, result, writeResult);
  } catch (error) {
    try {
      await markPresidentialRosterResearchError(pool, {
        cycleId: job.cycle_id,
        electionYear: job.election_year,
        researchedAt,
        error,
      });
    } catch {
      // Preserve the original failure so BullMQ retries the real cause.
    }
    throw error;
  } finally {
    if (redis) {
      await redis.quit();
    }
    if (shouldClosePool) {
      await pool.end();
    }
  }
}

export function createPresidentialRosterResearchWorker(concurrency = 1): Worker<
  PresidentialRosterResearchJobData,
  PresidentialRosterResearchJobResult
> {
  if (!Number.isInteger(concurrency) || concurrency <= 0) {
    throw new Error(`Invalid presidential roster research worker concurrency: ${concurrency}`);
  }

  const env = getPipelineEnv();
  const processor: Processor<
    PresidentialRosterResearchJobData,
    PresidentialRosterResearchJobResult
  > = async (job) => {
    if (job.name !== PRESIDENTIAL_ROSTER_RESEARCH_JOB_NAME) {
      throw new Error(`Unsupported presidential roster research job name: ${job.name}`);
    }
    return processPresidentialRosterResearchJob(job.data);
  };

  return new Worker<PresidentialRosterResearchJobData, PresidentialRosterResearchJobResult>(
    getQueueName(),
    processor,
    {
      connection: toConnectionOptions(env.REDIS_URL),
      concurrency,
    }
  );
}

export async function runPresidentialRosterResearchEnricher(
  options: PresidentialRosterResearchEnricherOptions = {}
): Promise<void> {
  const { once = false, blockMs = 5000, concurrency = 1 } = options;
  const worker = createPresidentialRosterResearchWorker(concurrency);

  if (!once) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      void worker.close().then(resolve, reject);
    }, blockMs);

    worker.once("drained", () => {
      clearTimeout(timeout);
      void worker.close().then(resolve, reject);
    });
    worker.once("failed", (_job, error) => {
      clearTimeout(timeout);
      void worker.close().then(() => reject(error), reject);
    });
    worker.once("error", (error) => {
      clearTimeout(timeout);
      void worker.close().then(() => reject(error), reject);
    });
  });
}
