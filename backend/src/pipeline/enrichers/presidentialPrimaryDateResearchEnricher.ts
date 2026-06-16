import { Worker, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";
import type { PoolClient } from "pg";

import {
  buildPresidentialPrimaryDateAiConfigFromEnv,
  enrichPresidentialPrimaryDates,
  type PresidentialPrimaryDateAiConfig,
} from "../../ai/enrichPresidentialPrimaryDates.js";
import type { AiCandidate } from "../../ai/aiCandidates.js";
import { getPipelineEnv } from "../../config/env.js";
import { isPresidentialElectionsEnabled } from "../../config/featureFlags.js";
import {
  PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME,
  type PresidentialPrimaryDateResearchJobData,
} from "../producers/presidentialPrimaryDateResearchProducer.js";
import {
  markPresidentialPrimaryDateResearchError,
  writePresidentialPrimaryDatePayloadRows,
} from "../presidential/presidentialPrimaryDateWriter.js";

export type PresidentialPrimaryDateResearchEnricherOptions = {
  once?: boolean;
  blockMs?: number;
  concurrency?: number;
};

export type PresidentialPrimaryDateResearchJobResult = {
  cycle_id: string;
  election_year: number;
  party: string;
  requested_state_count: number;
  skipped_state_count: number;
  official_found_count: number;
  not_official_yet_count: number;
  error_count: number;
  rows_updated: number;
  next_research_at: string | null;
  provider?: string;
  model?: string;
  disabled?: boolean;
  error?: string;
};

type DueStateRow = {
  state_fips: string;
  cycle_name: string;
};

type DueStateInfo = {
  stateFipsList: string[];
  cycleName: string | null;
};

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

function getQueueName(): string {
  return process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_QUEUE?.trim() || "presidential_primary_date_research";
}

function normalizeStateFipsList(values: readonly string[]): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const stateFips = value.trim();
    if (!/^[0-9]{2}$/.test(stateFips)) {
      throw new Error(`Invalid presidential primary date research job state_fips: ${value}`);
    }
    if (seen.has(stateFips)) {
      continue;
    }
    seen.add(stateFips);
    normalized.push(stateFips);
  }
  if (normalized.length === 0) {
    throw new Error("Presidential primary date research job requires at least one state_fips");
  }
  return normalized.sort();
}

function assertValidJob(job: PresidentialPrimaryDateResearchJobData): PresidentialPrimaryDateResearchJobData {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(job.cycle_id)) {
    throw new Error(`Invalid presidential primary date research job cycle_id: ${job.cycle_id}`);
  }
  if (!Number.isInteger(job.election_year) || job.election_year < 2000 || job.election_year > 2100 || job.election_year % 4 !== 0) {
    throw new Error(`Invalid presidential primary date research job election_year: ${job.election_year}`);
  }
  if (job.party.trim().length === 0) {
    throw new Error("Presidential primary date research job party is required");
  }
  const scheduledFor = new Date(job.scheduled_for);
  if (Number.isNaN(scheduledFor.getTime())) {
    throw new Error(`Invalid presidential primary date research job scheduled_for: ${job.scheduled_for}`);
  }
  return {
    ...job,
    cycle_name: job.cycle_name?.trim(),
    party: job.party.trim(),
    state_fips_list: normalizeStateFipsList(job.state_fips_list),
    scheduled_for: scheduledFor.toISOString(),
  };
}

async function withTransaction<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let transactionStarted = false;
  try {
    await client.query("BEGIN");
    transactionStarted = true;
    const result = await fn(client);
    await client.query("COMMIT");
    transactionStarted = false;
    return result;
  } catch (error) {
    if (transactionStarted) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error("presidential primary date research rollback failed:", rollbackError);
      }
    }
    throw error;
  } finally {
    client.release();
  }
}

async function listDueStateFipsForJob(
  pool: Pool,
  job: PresidentialPrimaryDateResearchJobData,
  now: Date
): Promise<DueStateInfo> {
  const result = await pool.query<DueStateRow>(
    `
      SELECT pspd.state_fips
           , concat(pc.election_year::text, ' ', pc.party, ' presidential primary') AS cycle_name
      FROM public.presidential_state_primary_dates AS pspd
      JOIN public.presidential_cycles AS pc
        ON pc.id = pspd.cycle_id
      WHERE pspd.cycle_id = $1::uuid
        AND pc.stage = 'primary'
        AND pc.election_year = $2::int
        AND pc.party = $3
        AND pspd.state_fips = ANY($4::text[])
        AND pspd.date_research_status <> 'official_found'
        AND (
          pspd.next_research_at IS NULL
          OR pspd.next_research_at <= $5::timestamptz
        )
      ORDER BY pspd.state_fips ASC
    `,
    [job.cycle_id, job.election_year, job.party, job.state_fips_list, now.toISOString()]
  );
  return {
    stateFipsList: result.rows.map((row) => row.state_fips),
    cycleName: result.rows[0]?.cycle_name ?? null,
  };
}

export async function processPresidentialPrimaryDateResearchJob(
  rawJob: PresidentialPrimaryDateResearchJobData,
  options: {
    pool?: Pool;
    aiConfig?: PresidentialPrimaryDateAiConfig;
    candidates?: readonly AiCandidate[];
    researchedAt?: Date;
  } = {}
): Promise<PresidentialPrimaryDateResearchJobResult> {
  const job = assertValidJob(rawJob);
  if (!isPresidentialElectionsEnabled()) {
    return {
      cycle_id: job.cycle_id,
      election_year: job.election_year,
      party: job.party,
      requested_state_count: job.state_fips_list.length,
      skipped_state_count: job.state_fips_list.length,
      official_found_count: 0,
      not_official_yet_count: 0,
      error_count: 0,
      rows_updated: 0,
      next_research_at: null,
      disabled: true,
    };
  }

  const env = getPipelineEnv();
  const pool = options.pool ?? new Pool({ connectionString: env.DATABASE_URL });
  const shouldClosePool = !options.pool;
  const aiConfig = options.aiConfig ?? buildPresidentialPrimaryDateAiConfigFromEnv();
  const researchedAt = options.researchedAt ?? new Date();

  try {
    const dueStateInfo = await listDueStateFipsForJob(pool, job, researchedAt);
    const dueStateFipsList = dueStateInfo.stateFipsList;
    const skippedStateCount = job.state_fips_list.length - dueStateFipsList.length;
    if (dueStateFipsList.length === 0) {
      return {
        cycle_id: job.cycle_id,
        election_year: job.election_year,
        party: job.party,
        requested_state_count: job.state_fips_list.length,
        skipped_state_count: skippedStateCount,
        official_found_count: 0,
        not_official_yet_count: 0,
        error_count: 0,
        rows_updated: 0,
        next_research_at: null,
      };
    }
    if (!dueStateInfo.cycleName) {
      throw new Error(`Missing presidential primary cycle name for cycle ${job.cycle_id}`);
    }

    const aiResult = await enrichPresidentialPrimaryDates(
      {
        cycleId: job.cycle_id,
        electionName: dueStateInfo.cycleName,
        electionYear: job.election_year,
        party: job.party,
        stateFipsList: dueStateFipsList,
        scheduledFor: job.scheduled_for,
      },
      aiConfig,
      options.candidates
    );

    if (!aiResult.ok) {
      const errorWrite = await withTransaction(pool, (client) =>
        markPresidentialPrimaryDateResearchError(client, {
          cycleId: job.cycle_id,
          electionYear: job.election_year,
          stateFipsList: dueStateFipsList,
          error: aiResult.reason,
          researchedAt,
        })
      );
      return {
        cycle_id: job.cycle_id,
        election_year: job.election_year,
        party: job.party,
        requested_state_count: job.state_fips_list.length,
        skipped_state_count: skippedStateCount,
        official_found_count: 0,
        not_official_yet_count: 0,
        error_count: errorWrite.rowsUpdated,
        rows_updated: errorWrite.rowsUpdated,
        next_research_at: errorWrite.nextResearchAt,
        error: aiResult.reason,
      };
    }

    const writeResult = await withTransaction(pool, async (client) => {
      const validWrite = await writePresidentialPrimaryDatePayloadRows(client, {
        cycleId: job.cycle_id,
        electionYear: job.election_year,
        payload: aiResult.payload,
        researchedAt,
      });
      if (aiResult.failedRows.length === 0) {
        return {
          ...validWrite,
          errorCount: 0,
          errorRowsUpdated: 0,
          errorNextResearchAt: null as string | null,
        };
      }
      const errorWrite = await markPresidentialPrimaryDateResearchError(client, {
        cycleId: job.cycle_id,
        electionYear: job.election_year,
        stateFipsList: aiResult.failedRows.map((failure) => failure.state_fips),
        error: `Partial presidential primary date research failure: ${aiResult.failedRows
          .map((failure) => `${failure.state_fips}: ${failure.reason}`)
          .join("; ")}`,
        researchedAt,
      });
      return {
        ...validWrite,
        errorCount: aiResult.failedRows.length,
        errorRowsUpdated: errorWrite.rowsUpdated,
        errorNextResearchAt: errorWrite.nextResearchAt,
      };
    });

    return {
      cycle_id: job.cycle_id,
      election_year: job.election_year,
      party: job.party,
      requested_state_count: job.state_fips_list.length,
      skipped_state_count: skippedStateCount,
      official_found_count: writeResult.officialFoundCount,
      not_official_yet_count: writeResult.notOfficialYetCount,
      error_count: writeResult.errorCount,
      rows_updated: writeResult.rowsUpdated + writeResult.errorRowsUpdated,
      next_research_at: writeResult.errorNextResearchAt ?? writeResult.nextResearchAt,
      provider: aiResult.provider,
      model: aiResult.model,
    };
  } finally {
    if (shouldClosePool) {
      await pool.end();
    }
  }
}

export function createPresidentialPrimaryDateResearchWorker(concurrency = 1): Worker<
  PresidentialPrimaryDateResearchJobData,
  PresidentialPrimaryDateResearchJobResult
> {
  if (!Number.isInteger(concurrency) || concurrency <= 0) {
    throw new Error(`Invalid presidential primary date research worker concurrency: ${concurrency}`);
  }

  const env = getPipelineEnv();
  const processor: Processor<
    PresidentialPrimaryDateResearchJobData,
    PresidentialPrimaryDateResearchJobResult
  > = async (job) => {
    if (job.name !== PRESIDENTIAL_PRIMARY_DATE_RESEARCH_JOB_NAME) {
      throw new Error(`Unsupported presidential primary date research job name: ${job.name}`);
    }
    return processPresidentialPrimaryDateResearchJob(job.data);
  };

  return new Worker<PresidentialPrimaryDateResearchJobData, PresidentialPrimaryDateResearchJobResult>(
    getQueueName(),
    processor,
    {
      connection: toConnectionOptions(env.REDIS_URL),
      concurrency,
    }
  );
}

export async function runPresidentialPrimaryDateResearchEnricher(
  options: PresidentialPrimaryDateResearchEnricherOptions = {}
): Promise<void> {
  const { once = false, blockMs = 5000, concurrency = 1 } = options;
  const worker = createPresidentialPrimaryDateResearchWorker(concurrency);

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
