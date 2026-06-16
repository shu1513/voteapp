import { Queue, type JobsOptions } from "bullmq";
import type { ConnectionOptions, Job } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../../config/env.js";
import { isPresidentialFeatureEnabled } from "../../config/featureFlags.js";
import {
  ensurePresidentialStatePrimaryDateRows,
  PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS,
  type EnsurePresidentialStatePrimaryDateRowsResult,
} from "../presidential/presidentialPrimaryDates.js";
import {
  evaluatePresidentialPrimaryDateResearchEligibility,
  type PresidentialPrimaryDateResearchStatus,
} from "../presidential/presidentialPrimaryDateResearchPolicy.js";

export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME = "presidential_primary_date_research";

export type PresidentialPrimaryDateResearchJobData = {
  cycle_id: string;
  cycle_name?: string;
  election_year: number;
  party: string;
  state_fips_list: string[];
  scheduled_for: string;
  run_id: string;
};

type PresidentialPrimaryCycleRow = {
  cycle_id: string;
  cycle_name: string;
  election_year: number;
  party: string;
};

type PresidentialPrimaryDateDueRow = PresidentialPrimaryCycleRow & {
  state_fips: string;
  date_research_status: PresidentialPrimaryDateResearchStatus;
  next_research_at: string | null;
};

type ProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
  now?: Date;
  maxRowsPerRun?: number;
  maxStatesPerJob?: number;
  maxJobsPerRun?: number;
};

type ProducerPolicy = {
  maxRowsPerRun: number;
  maxStatesPerJob: number;
  maxJobsPerRun: number;
};

type DueGroup = {
  cycleId: string;
  cycleName: string;
  electionYear: number;
  party: string;
  scheduledFor: Date;
  stateFipsList: string[];
  partitionId: number;
};

export type PresidentialPrimaryDateResearchProducerResult = {
  enabled: boolean;
  forced: boolean;
  dryRun: boolean;
  now: string;
  maxRowsPerRun: number;
  maxStatesPerJob: number;
  maxJobsPerRun: number;
  cyclesScanned: number;
  eligibleCycleCount: number;
  bootstrapRequestedRowCount: number;
  bootstrapInsertedRowCount: number;
  dueRowCount: number;
  dueGroupCount: number;
  selectedGroupCount: number;
  maxRowsHit: boolean;
  maxGroupsHit: boolean;
  enqueuedJobCount: number;
  updatedJobCount: number;
  skippedActiveJobCount: number;
};

type QueueLike = Pick<Queue<PresidentialPrimaryDateResearchJobData>, "add" | "getJob" | "close">;
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
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid positive integer env ${name}: ${raw}`);
  }
  return parsed;
}

function assertPositiveIntegerOption(name: string, value: number): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid positive integer option ${name}: ${value}`);
  }
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
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
    maxRowsPerRun: parsePositiveIntegerEnv("PRESIDENTIAL_PRIMARY_DATES_RESEARCH_MAX_ROWS_PER_RUN", 200),
    maxStatesPerJob: parsePositiveIntegerEnv("PRESIDENTIAL_PRIMARY_DATES_RESEARCH_MAX_STATES_PER_JOB", 10),
    maxJobsPerRun: parsePositiveIntegerEnv("PRESIDENTIAL_PRIMARY_DATES_RESEARCH_MAX_JOBS_PER_RUN", 20),
  };
}

function getQueueName(): string {
  return process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_QUEUE?.trim() || "presidential_primary_date_research";
}

function defaultJobOptions(): JobsOptions {
  return {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 5 * 60 * 1000,
    },
    removeOnComplete: 1000,
    removeOnFail: 1000,
  };
}

function createPresidentialPrimaryDateResearchQueue(): Queue<PresidentialPrimaryDateResearchJobData> {
  const env = getPipelineEnv();
  return new Queue<PresidentialPrimaryDateResearchJobData>(getQueueName(), {
    connection: toConnectionOptions(env.REDIS_URL),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildPresidentialPrimaryDateResearchJobId(input: {
  cycleId: string;
  partitionId: number;
}): string {
  return `presidential-primary-dates:${input.cycleId}:partition:${input.partitionId}`;
}

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential primary date research producer ${label}`);
  }
}

function buildStateFipsPartitionLookup(maxStatesPerJob: number): Map<string, number> {
  const lookup = new Map<string, number>();
  PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS.forEach((stateFips, index) => {
    lookup.set(stateFips, Math.floor(index / maxStatesPerJob));
  });
  return lookup;
}

function chunkRowsByCycle(rows: readonly PresidentialPrimaryDateDueRow[], maxStatesPerJob: number): DueGroup[] {
  const partitionByStateFips = buildStateFipsPartitionLookup(maxStatesPerJob);
  const byCycle = new Map<string, PresidentialPrimaryDateDueRow[]>();
  for (const row of rows) {
    const existing = byCycle.get(row.cycle_id);
    if (existing) {
      existing.push(row);
    } else {
      byCycle.set(row.cycle_id, [row]);
    }
  }

  const groups: DueGroup[] = [];
  for (const cycleRows of byCycle.values()) {
    const first = cycleRows[0];
    if (!first) {
      continue;
    }
    const rowsByPartition = new Map<number, PresidentialPrimaryDateDueRow[]>();
    for (const row of cycleRows) {
      const partitionId = partitionByStateFips.get(row.state_fips);
      if (partitionId === undefined) {
        throw new Error(`Unknown presidential primary date state_fips for partitioning: ${row.state_fips}`);
      }
      const existing = rowsByPartition.get(partitionId);
      if (existing) {
        existing.push(row);
      } else {
        rowsByPartition.set(partitionId, [row]);
      }
    }

    for (const [partitionId, partitionRows] of rowsByPartition) {
      const sortedRows = [...partitionRows].sort((a, b) => a.state_fips.localeCompare(b.state_fips));
      groups.push({
        cycleId: first.cycle_id,
        cycleName: first.cycle_name,
        electionYear: first.election_year,
        party: first.party,
        scheduledFor: new Date(),
        stateFipsList: sortedRows.map((row) => row.state_fips),
        partitionId,
      });
    }
  }

  return groups.sort((a, b) => {
    const cycleDiff = a.electionYear - b.electionYear;
    if (cycleDiff !== 0) return cycleDiff;
    const partyDiff = a.party.localeCompare(b.party);
    if (partyDiff !== 0) return partyDiff;
    return a.partitionId - b.partitionId;
  });
}

async function listPrimaryCycles(pool: Pool): Promise<PresidentialPrimaryCycleRow[]> {
  const result = await pool.query<PresidentialPrimaryCycleRow>(
    `
      SELECT
        id AS cycle_id,
        concat(election_year::text, ' ', party, ' presidential primary') AS cycle_name,
        election_year,
        party
      FROM public.presidential_cycles
      WHERE stage = 'primary'
        AND status = 'active'
      ORDER BY election_year ASC, party ASC, id ASC
    `
  );
  return result.rows;
}

async function listPotentialDueRows(
  pool: Pool,
  cycleIds: readonly string[],
  now: Date,
  maxRows: number
): Promise<PresidentialPrimaryDateDueRow[]> {
  if (cycleIds.length === 0) {
    return [];
  }

  const result = await pool.query<PresidentialPrimaryDateDueRow>(
    `
      SELECT
        pc.id AS cycle_id,
        concat(pc.election_year::text, ' ', pc.party, ' presidential primary') AS cycle_name,
        pc.election_year,
        pc.party,
        pspd.state_fips,
        pspd.date_research_status,
        pspd.next_research_at::text AS next_research_at
      FROM public.presidential_state_primary_dates AS pspd
      JOIN public.presidential_cycles AS pc
        ON pc.id = pspd.cycle_id
      WHERE pc.id = ANY($1::uuid[])
        AND pspd.date_research_status <> 'official_found'
        AND (
          pspd.next_research_at IS NULL
          OR pspd.next_research_at <= $2::timestamptz
        )
      ORDER BY pc.election_year ASC, pc.party ASC, pspd.next_research_at ASC NULLS FIRST, pspd.state_fips ASC
      LIMIT $3::int
    `,
    [cycleIds, now.toISOString(), maxRows]
  );

  return result.rows;
}

function selectEligibleCycleIds(
  cycles: readonly PresidentialPrimaryCycleRow[],
  now: Date
): string[] {
  return cycles
    .filter((cycle) => {
      const eligibility = evaluatePresidentialPrimaryDateResearchEligibility({
        electionYear: cycle.election_year,
        dateResearchStatus: "pending",
        now,
      });
      return eligibility.eligible;
    })
    .map((cycle) => cycle.cycle_id);
}

function filterDueRows(
  rows: readonly PresidentialPrimaryDateDueRow[],
  now: Date
): PresidentialPrimaryDateDueRow[] {
  return rows.filter((row) =>
    evaluatePresidentialPrimaryDateResearchEligibility({
      electionYear: row.election_year,
      dateResearchStatus: row.date_research_status,
      nextResearchAt: row.next_research_at,
      now,
    }).eligible
  );
}

async function upsertResearchJob(
  queue: QueueLike,
  group: DueGroup,
  runId: string,
  scheduledFor: Date
): Promise<UpsertOutcome> {
  const jobId = buildPresidentialPrimaryDateResearchJobId({
    cycleId: group.cycleId,
    partitionId: group.partitionId,
  });
  const existing = await queue.getJob(jobId);
  let stateFipsList = group.stateFipsList;

  if (existing) {
    const state = await (existing as Job).getState();
    if (ACTIVE_JOB_STATES.has(state)) {
      return "skipped_active";
    }
    const existingStateFipsList = Array.isArray(
      (existing as Job<PresidentialPrimaryDateResearchJobData>).data?.state_fips_list
    )
      ? (existing as Job<PresidentialPrimaryDateResearchJobData>).data.state_fips_list
      : [];
    stateFipsList = [...existingStateFipsList, ...group.stateFipsList];
    await existing.remove();
  }

  await queue.add(
    PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME,
    {
      cycle_id: group.cycleId,
      cycle_name: group.cycleName,
      election_year: group.electionYear,
      party: group.party,
      state_fips_list: [...new Set(stateFipsList)].sort(),
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

export async function runPresidentialPrimaryDateResearchProducer(
  options: ProducerOptions = {}
): Promise<PresidentialPrimaryDateResearchProducerResult> {
  const policy = readProducerPolicy();
  const force = Boolean(options.force);
  const dryRun = Boolean(options.dryRun);
  const enabled = isPresidentialFeatureEnabled("PRESIDENTIAL_PRIMARY_DATES_RESEARCH_ENABLED", force);
  const now = options.now ?? new Date();
  assertValidDate(now, "now");

  const maxRowsPerRun = options.maxRowsPerRun ?? policy.maxRowsPerRun;
  const maxStatesPerJob = options.maxStatesPerJob ?? policy.maxStatesPerJob;
  const maxJobsPerRun = options.maxJobsPerRun ?? policy.maxJobsPerRun;
  assertPositiveIntegerOption("maxRowsPerRun", maxRowsPerRun);
  assertPositiveIntegerOption("maxStatesPerJob", maxStatesPerJob);
  assertPositiveIntegerOption("maxJobsPerRun", maxJobsPerRun);

  if (!enabled) {
    return {
      enabled: false,
      forced: force,
      dryRun,
      now: now.toISOString(),
      maxRowsPerRun,
      maxStatesPerJob,
      maxJobsPerRun,
      cyclesScanned: 0,
      eligibleCycleCount: 0,
      bootstrapRequestedRowCount: 0,
      bootstrapInsertedRowCount: 0,
      dueRowCount: 0,
      dueGroupCount: 0,
      selectedGroupCount: 0,
      maxRowsHit: false,
      maxGroupsHit: false,
      enqueuedJobCount: 0,
      updatedJobCount: 0,
      skippedActiveJobCount: 0,
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const queue = dryRun ? null : createPresidentialPrimaryDateResearchQueue();
  const runId = `presidential_primary_date_research_${now.toISOString()}`;

  try {
    const cycles = await listPrimaryCycles(pool);
    const eligibleCycleIds = selectEligibleCycleIds(cycles, now);
    const bootstrap: EnsurePresidentialStatePrimaryDateRowsResult =
      dryRun
        ? {
            requestedCycleCount: eligibleCycleIds.length,
            stateCount: PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS.length,
            requestedRowCount: eligibleCycleIds.length * PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS.length,
            insertedRowCount: 0,
            existingRowCount: 0,
          }
        : eligibleCycleIds.length > 0
        ? await ensurePresidentialStatePrimaryDateRows(pool, eligibleCycleIds)
        : {
            requestedCycleCount: 0,
            stateCount: 0,
            requestedRowCount: 0,
            insertedRowCount: 0,
            existingRowCount: 0,
          };

    const potentialRows = await listPotentialDueRows(pool, eligibleCycleIds, now, maxRowsPerRun);
    const dueRows = filterDueRows(potentialRows, now);
    const groups = chunkRowsByCycle(dueRows, maxStatesPerJob).map((group) => ({
      ...group,
      scheduledFor: now,
    }));
    const selectedGroups = groups.slice(0, maxJobsPerRun);
    const maxGroupsHit = groups.length > selectedGroups.length;
    const maxRowsHit = potentialRows.length >= maxRowsPerRun;

    let enqueuedJobCount = 0;
    let updatedJobCount = 0;
    let skippedActiveJobCount = 0;

    if (queue) {
      for (const group of selectedGroups) {
        const outcome = await upsertResearchJob(queue, group, runId, now);
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
      maxRowsPerRun,
      maxStatesPerJob,
      maxJobsPerRun,
      cyclesScanned: cycles.length,
      eligibleCycleCount: eligibleCycleIds.length,
      bootstrapRequestedRowCount: bootstrap.requestedRowCount,
      bootstrapInsertedRowCount: bootstrap.insertedRowCount,
      dueRowCount: dueRows.length,
      dueGroupCount: groups.length,
      selectedGroupCount: selectedGroups.length,
      maxRowsHit,
      maxGroupsHit,
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
