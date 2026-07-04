import { Queue, type JobsOptions } from "bullmq";
import type { Job } from "bullmq";
import { toConnectionOptions } from "../../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../../config/env.js";
import { isPresidentialFeatureEnabled } from "../../config/featureFlags.js";
import {
  PRESIDENTIAL_NOMINEE_RESEARCH_JOB_NAME,
  type PresidentialNomineeResearchJobData,
} from "../enrichers/presidentialNomineeResearchEnricher.js";
import { DEFAULT_PRESIDENTIAL_PRIMARY_PARTIES } from "../presidential/presidentialCycles.js";
import {
  evaluatePresidentialNomineeResearchEligibility,
  type PresidentialNomineeResearchCycleStatus,
} from "../presidential/presidentialNomineeResearchPolicy.js";

type PresidentialNomineeResearchCycleRow = {
  cycle_id: string;
  election_year: number;
  stage: "primary";
  party: string;
  status: PresidentialNomineeResearchCycleStatus;
  nominee_research_last_attempted_at: string | null;
  nominee_research_next_at: string | null;
};

export type PresidentialNomineeResearchProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
  now?: Date;
  maxCyclesPerRun?: number;
};

type ProducerPolicy = {
  maxCyclesPerRun: number;
};

export type PresidentialNomineeResearchProducerResult = {
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

type QueueLike = Pick<Queue<PresidentialNomineeResearchJobData>, "add" | "getJob" | "close">;
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
    throw new Error(`Invalid presidential nominee research producer ${label}`);
  }
}

function readProducerPolicy(): ProducerPolicy {
  return {
    maxCyclesPerRun: parsePositiveIntegerEnv("PRESIDENTIAL_NOMINEE_RESEARCH_MAX_CYCLES_PER_RUN", 10),
  };
}

function getQueueName(): string {
  return process.env.PRESIDENTIAL_NOMINEE_RESEARCH_QUEUE?.trim() || "presidential_nominee_research";
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

function createPresidentialNomineeResearchQueue(): Queue<PresidentialNomineeResearchJobData> {
  const env = getPipelineEnv();
  return new Queue<PresidentialNomineeResearchJobData>(getQueueName(), {
    connection: toConnectionOptions(env.REDIS_URL),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildPresidentialNomineeResearchJobId(input: { cycleId: string }): string {
  return `presidential-nominee:${input.cycleId}`;
}

async function listPrimaryCycles(pool: Pool): Promise<PresidentialNomineeResearchCycleRow[]> {
  const result = await pool.query<PresidentialNomineeResearchCycleRow>(
    `
      SELECT
        id AS cycle_id,
        election_year,
        stage,
        party,
        status,
        nominee_research_last_attempted_at::text AS nominee_research_last_attempted_at,
        nominee_research_next_at::text AS nominee_research_next_at
      FROM public.presidential_cycles
      WHERE stage = 'primary'
        AND status = 'active'
        AND party = ANY($1::text[])
        AND EXISTS (
          SELECT 1
          FROM public.presidential_cycles AS general_cycle
          WHERE general_cycle.election_year = presidential_cycles.election_year
            AND general_cycle.stage = 'general'
            AND general_cycle.party IS NULL
        )
      ORDER BY election_year ASC, party ASC, id ASC
    `,
    [[...DEFAULT_PRESIDENTIAL_PRIMARY_PARTIES]]
  );
  return result.rows;
}

function filterDueCycles(
  rows: readonly PresidentialNomineeResearchCycleRow[],
  now: Date
): PresidentialNomineeResearchCycleRow[] {
  return rows.filter((row) =>
    evaluatePresidentialNomineeResearchEligibility({
      electionYear: row.election_year,
      cycleStatus: row.status,
      lastAttemptedAt: row.nominee_research_last_attempted_at,
      nextResearchAt: row.nominee_research_next_at,
      now,
    }).eligible
  );
}

async function upsertResearchJob(
  queue: QueueLike,
  row: PresidentialNomineeResearchCycleRow,
  runId: string,
  scheduledFor: Date
): Promise<UpsertOutcome> {
  const jobId = buildPresidentialNomineeResearchJobId({ cycleId: row.cycle_id });
  const existing = await queue.getJob(jobId);

  if (existing) {
    const state = await (existing as Job).getState();
    if (ACTIVE_JOB_STATES.has(state)) {
      return "skipped_active";
    }
    try {
      await existing.remove();
    } catch (error) {
      const latestState = await (existing as Job).getState();
      if (ACTIVE_JOB_STATES.has(latestState)) {
        return "skipped_active";
      }
      throw error;
    }
  }

  await queue.add(
    PRESIDENTIAL_NOMINEE_RESEARCH_JOB_NAME,
    {
      cycle_id: row.cycle_id,
      election_year: row.election_year,
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

export async function runPresidentialNomineeResearchProducer(
  options: PresidentialNomineeResearchProducerOptions = {}
): Promise<PresidentialNomineeResearchProducerResult> {
  const policy = readProducerPolicy();
  const force = Boolean(options.force);
  const dryRun = Boolean(options.dryRun);
  const enabled = isPresidentialFeatureEnabled("PRESIDENTIAL_NOMINEE_RESEARCH_ENABLED", force);
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
  let queue: Queue<PresidentialNomineeResearchJobData> | null = null;
  const runId = `presidential_nominee_research_${now.toISOString()}`;

  try {
    queue = dryRun ? null : createPresidentialNomineeResearchQueue();
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
