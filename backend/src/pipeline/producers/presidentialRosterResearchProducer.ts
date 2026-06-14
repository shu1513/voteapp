import { Queue, type JobsOptions } from "bullmq";
import type { ConnectionOptions, Job } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../../config/env.js";
import { DEFAULT_PRESIDENTIAL_PRIMARY_PARTIES } from "../presidential/presidentialCycles.js";
import {
  evaluatePresidentialRosterResearchEligibility,
  type PresidentialRosterResearchCycleStatus,
} from "../presidential/presidentialRosterResearchPolicy.js";

export const PRESIDENTIAL_ROSTER_RESEARCH_JOB_NAME = "presidential_roster_research";

export type PresidentialRosterResearchJobData = {
  cycle_id: string;
  election_year: number;
  stage: "primary";
  party: string;
  scheduled_for: string;
  run_id: string;
};

type PresidentialRosterResearchCycleRow = {
  cycle_id: string;
  election_year: number;
  stage: "primary";
  party: string;
  status: PresidentialRosterResearchCycleStatus;
  roster_research_last_attempted_at: string | null;
  roster_research_next_at: string | null;
};

export type PresidentialRosterResearchProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
  now?: Date;
  maxCyclesPerRun?: number;
};

type ProducerPolicy = {
  enabled: boolean;
  maxCyclesPerRun: number;
};

export type PresidentialRosterResearchProducerResult = {
  enabled: boolean;
  forced: boolean;
  dryRun: boolean;
  now: string;
  maxCyclesPerRun: number;
  cyclesScanned: number;
  dueCycleCount: number;
  selectedCycleCount: number;
  maxCyclesHit: boolean;
  enqueuedJobCount: number;
  updatedJobCount: number;
  skippedActiveJobCount: number;
};

type QueueLike = Pick<Queue<PresidentialRosterResearchJobData>, "add" | "getJob" | "close">;
type UpsertOutcome = "created" | "updated" | "skipped_active";

const ACTIVE_JOB_STATES = new Set(["active"]);

function parsePositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }
  const normalized = raw.trim();
  if (!/^[1-9]\d*$/.test(normalized)) {
    throw new Error(`Invalid positive integer env ${name}: ${raw}`);
  }
  return Number.parseInt(normalized, 10);
}

function assertPositiveIntegerOption(name: string, value: number): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid positive integer option ${name}: ${value}`);
  }
}

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential roster research producer ${label}`);
  }
}

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

function readProducerPolicy(): ProducerPolicy {
  return {
    enabled: process.env.PRESIDENTIAL_ROSTER_RESEARCH_ENABLED === "true",
    maxCyclesPerRun: parsePositiveIntegerEnv("PRESIDENTIAL_ROSTER_RESEARCH_MAX_CYCLES_PER_RUN", 10),
  };
}

function getQueueName(): string {
  return process.env.PRESIDENTIAL_ROSTER_RESEARCH_QUEUE?.trim() || "presidential_roster_research";
}

function defaultJobOptions(): JobsOptions {
  return {
    attempts: 2,
    backoff: {
      type: "exponential",
      delay: 5 * 60 * 1000,
    },
    removeOnComplete: 1000,
    removeOnFail: 1000,
  };
}

function createPresidentialRosterResearchQueue(): Queue<PresidentialRosterResearchJobData> {
  const env = getPipelineEnv();
  return new Queue<PresidentialRosterResearchJobData>(getQueueName(), {
    connection: toConnectionOptions(env.REDIS_URL),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildPresidentialRosterResearchJobId(input: { cycleId: string }): string {
  return `presidential-roster:${input.cycleId}`;
}

async function listPrimaryCycles(pool: Pool): Promise<PresidentialRosterResearchCycleRow[]> {
  const result = await pool.query<PresidentialRosterResearchCycleRow>(
    `
      SELECT
        id AS cycle_id,
        election_year,
        stage,
        party,
        status,
        roster_research_last_attempted_at::text AS roster_research_last_attempted_at,
        roster_research_next_at::text AS roster_research_next_at
      FROM public.presidential_cycles
      WHERE stage = 'primary'
        AND party = ANY($1::text[])
      ORDER BY election_year ASC, party ASC, id ASC
    `,
    [[...DEFAULT_PRESIDENTIAL_PRIMARY_PARTIES]]
  );
  return result.rows;
}

function filterDueCycles(
  rows: readonly PresidentialRosterResearchCycleRow[],
  now: Date
): PresidentialRosterResearchCycleRow[] {
  return rows.filter((row) =>
    evaluatePresidentialRosterResearchEligibility({
      electionYear: row.election_year,
      cycleStatus: row.status,
      lastAttemptedAt: row.roster_research_last_attempted_at,
      nextResearchAt: row.roster_research_next_at,
      now,
    }).eligible
  );
}

async function upsertResearchJob(
  queue: QueueLike,
  row: PresidentialRosterResearchCycleRow,
  runId: string,
  scheduledFor: Date
): Promise<UpsertOutcome> {
  const jobId = buildPresidentialRosterResearchJobId({ cycleId: row.cycle_id });
  const existing = await queue.getJob(jobId);

  if (existing) {
    const state = await (existing as Job).getState();
    if (ACTIVE_JOB_STATES.has(state)) {
      return "skipped_active";
    }
    await existing.remove();
  }

  await queue.add(
    PRESIDENTIAL_ROSTER_RESEARCH_JOB_NAME,
    {
      cycle_id: row.cycle_id,
      election_year: row.election_year,
      stage: "primary",
      party: row.party,
      scheduled_for: scheduledFor.toISOString(),
      run_id: runId,
    },
    {
      ...defaultJobOptions(),
      jobId,
    }
  );

  return existing ? "updated" : "created";
}

export async function runPresidentialRosterResearchProducer(
  options: PresidentialRosterResearchProducerOptions = {}
): Promise<PresidentialRosterResearchProducerResult> {
  const policy = readProducerPolicy();
  const force = Boolean(options.force);
  const dryRun = Boolean(options.dryRun);
  const enabled = force || policy.enabled;
  const now = options.now ?? new Date();
  assertValidDate(now, "now");

  const maxCyclesPerRun = options.maxCyclesPerRun ?? policy.maxCyclesPerRun;
  assertPositiveIntegerOption("maxCyclesPerRun", maxCyclesPerRun);

  if (!enabled) {
    return {
      enabled: false,
      forced: force,
      dryRun,
      now: now.toISOString(),
      maxCyclesPerRun,
      cyclesScanned: 0,
      dueCycleCount: 0,
      selectedCycleCount: 0,
      maxCyclesHit: false,
      enqueuedJobCount: 0,
      updatedJobCount: 0,
      skippedActiveJobCount: 0,
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const queue = dryRun ? null : createPresidentialRosterResearchQueue();
  const runId = `presidential_roster_research_${now.toISOString()}`;

  try {
    const cycles = await listPrimaryCycles(pool);
    const dueCycles = filterDueCycles(cycles, now);
    const selectedCycles = dueCycles.slice(0, maxCyclesPerRun);
    const maxCyclesHit = dueCycles.length > selectedCycles.length;

    let enqueuedJobCount = 0;
    let updatedJobCount = 0;
    let skippedActiveJobCount = 0;

    if (queue) {
      for (const row of selectedCycles) {
        const outcome = await upsertResearchJob(queue, row, runId, now);
        if (outcome === "created") enqueuedJobCount += 1;
        if (outcome === "updated") updatedJobCount += 1;
        if (outcome === "skipped_active") skippedActiveJobCount += 1;
      }
    }

    return {
      enabled: true,
      forced: force,
      dryRun,
      now: now.toISOString(),
      maxCyclesPerRun,
      cyclesScanned: cycles.length,
      dueCycleCount: dueCycles.length,
      selectedCycleCount: selectedCycles.length,
      maxCyclesHit,
      enqueuedJobCount,
      updatedJobCount,
      skippedActiveJobCount,
    };
  } finally {
    if (queue) {
      await queue.close();
    }
    await pool.end();
  }
}
