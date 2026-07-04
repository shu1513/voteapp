import { Worker, type Processor } from "bullmq";
import { toConnectionOptions } from "../../utils/redisConnection.js";
import { Pool } from "pg";

import type { AiCandidate } from "../../ai/aiCandidates.js";
import type { PresidentialNomineeAiConfig } from "../../ai/enrichPresidentialNominee.js";
import { getPipelineEnv } from "../../config/env.js";
import { isPresidentialElectionsEnabled } from "../../config/featureFlags.js";
import {
  enrichPresidentialNomineeForCycle,
  type PresidentialNomineeEnricherInput,
  type PresidentialNomineeEnricherResult,
} from "./presidentialNomineeEnricher.js";
import {
  promotePresidentialNomineeFromResolution,
  PromotePresidentialNomineeError,
  type PromotePresidentialNomineeResult,
} from "../presidential/presidentialNomineePromotion.js";
import {
  markPresidentialNomineeResearchError,
  markPresidentialNomineeResearchSuccess,
  type PresidentialNomineeResearchWriteResult,
} from "../presidential/presidentialNomineeResearchWriter.js";

export const PRESIDENTIAL_NOMINEE_RESEARCH_JOB_NAME = "presidential_nominee_research";

export type PresidentialNomineeResearchJobData = {
  cycle_id: string;
  election_year: number;
  party: string;
  scheduled_for: string;
  run_id: string;
};

export type PresidentialNomineeResearchEnricherOptions = {
  once?: boolean;
  blockMs?: number;
  concurrency?: number;
};

export type PresidentialNomineeResearchJobResult = {
  cycle_id: string;
  election_year: number;
  party: string;
  ok: boolean;
  provider?: string;
  model?: string;
  candidate_count?: number;
  resolution_status?: string;
  nominee_candidate_id?: string;
  promotion_status?: PromotePresidentialNomineeResult["status"];
  general_cycle_id?: string;
  nominee_research_rows_updated?: number;
  next_research_at?: string | null;
  disabled?: boolean;
  error?: string;
  error_code?: string;
};

type NomineePromotionInput = {
  db: Pool;
  primaryCycleId: string;
  electionYear: number;
  party: string;
  resolution: Extract<PresidentialNomineeEnricherResult, { ok: true }>["resolution"];
  confirmedAt?: Date;
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function getQueueName(): string {
  return process.env.PRESIDENTIAL_NOMINEE_RESEARCH_QUEUE?.trim() || "presidential_nominee_research";
}

function assertValidJob(job: PresidentialNomineeResearchJobData): PresidentialNomineeResearchJobData {
  if (!UUID_PATTERN.test(job.cycle_id)) {
    throw new Error(`Invalid presidential nominee research job cycle_id: ${job.cycle_id}`);
  }
  if (
    !Number.isInteger(job.election_year) ||
    job.election_year < 2000 ||
    job.election_year > 2100 ||
    job.election_year % 4 !== 0
  ) {
    throw new Error(`Invalid presidential nominee research job election_year: ${job.election_year}`);
  }
  const party = job.party.trim();
  if (party.length === 0) {
    throw new Error("Presidential nominee research job party is required");
  }
  const scheduledFor = new Date(job.scheduled_for);
  if (Number.isNaN(scheduledFor.getTime())) {
    throw new Error(`Invalid presidential nominee research job scheduled_for: ${job.scheduled_for}`);
  }
  return {
    ...job,
    party,
    scheduled_for: scheduledFor.toISOString(),
  };
}

function toRetryableNomineeError(result: Extract<PresidentialNomineeEnricherResult, { ok: false }>): Error {
  const error = new Error(`presidential nominee research failed: ${result.error}`);
  error.name = result.errorCode;
  return error;
}

function summarizeNomineeFailure(
  job: PresidentialNomineeResearchJobData,
  result: Extract<PresidentialNomineeEnricherResult, { ok: false }>,
  tracking: PresidentialNomineeResearchWriteResult
): PresidentialNomineeResearchJobResult {
  return {
    cycle_id: job.cycle_id,
    election_year: job.election_year,
    party: job.party,
    ok: false,
    nominee_research_rows_updated: tracking.rowsUpdated,
    next_research_at: tracking.nextResearchAt,
    error: result.error,
    error_code: result.errorCode,
  };
}

function summarizePromotionError(
  job: PresidentialNomineeResearchJobData,
  result: Extract<PresidentialNomineeEnricherResult, { ok: true }>,
  error: PromotePresidentialNomineeError,
  tracking: PresidentialNomineeResearchWriteResult
): PresidentialNomineeResearchJobResult {
  return {
    cycle_id: job.cycle_id,
    election_year: job.election_year,
    party: job.party,
    ok: false,
    provider: result.provider,
    model: result.model,
    candidate_count: result.candidateCount,
    resolution_status: result.resolution.status,
    nominee_research_rows_updated: tracking.rowsUpdated,
    next_research_at: tracking.nextResearchAt,
    error: error.message,
    error_code: error.code,
  };
}

function summarizeSuccess(
  job: PresidentialNomineeResearchJobData,
  result: Extract<PresidentialNomineeEnricherResult, { ok: true }>,
  promotion: PromotePresidentialNomineeResult,
  tracking: PresidentialNomineeResearchWriteResult
): PresidentialNomineeResearchJobResult {
  return {
    cycle_id: job.cycle_id,
    election_year: job.election_year,
    party: job.party,
    ok: true,
    provider: result.provider,
    model: result.model,
    candidate_count: result.candidateCount,
    resolution_status: result.resolution.status,
    promotion_status: promotion.status,
    nominee_research_rows_updated: tracking.rowsUpdated,
    next_research_at: tracking.nextResearchAt,
    ...(result.resolution.status === "matched" ? { nominee_candidate_id: result.resolution.candidateId } : {}),
    ...(promotion.status === "promoted" ? { general_cycle_id: promotion.generalCycleId } : {}),
  };
}

export async function processPresidentialNomineeResearchJob(
  rawJob: PresidentialNomineeResearchJobData,
  options: {
    pool?: Pool;
    aiConfig?: PresidentialNomineeAiConfig;
    aiCandidates?: readonly AiCandidate[];
    researchedAt?: Date;
    enrichNomineeForCycle?: (
      input: PresidentialNomineeEnricherInput
    ) => Promise<PresidentialNomineeEnricherResult>;
    promoteNominee?: (input: NomineePromotionInput) => Promise<PromotePresidentialNomineeResult>;
  } = {}
): Promise<PresidentialNomineeResearchJobResult> {
  const job = assertValidJob(rawJob);
  if (!isPresidentialElectionsEnabled()) {
    return {
      cycle_id: job.cycle_id,
      election_year: job.election_year,
      party: job.party,
      ok: true,
      candidate_count: 0,
      resolution_status: "disabled",
      nominee_research_rows_updated: 0,
      next_research_at: null,
      disabled: true,
    };
  }

  const env = getPipelineEnv();
  const pool = options.pool ?? new Pool({ connectionString: env.DATABASE_URL });
  const shouldClosePool = !options.pool;
  const researchedAt = options.researchedAt ?? new Date();
  const enrichNominee = options.enrichNomineeForCycle ?? enrichPresidentialNomineeForCycle;
  const promoteNominee = options.promoteNominee ?? promotePresidentialNomineeFromResolution;
  let trackedFailure = false;

  try {
    const nomineeResult = await enrichNominee({
      db: pool,
      cycleId: job.cycle_id,
      electionYear: job.election_year,
      party: job.party,
      aiConfig: options.aiConfig,
      aiCandidates: options.aiCandidates,
    });

    if (!nomineeResult.ok) {
      const tracking = await markPresidentialNomineeResearchError(pool, {
        cycleId: job.cycle_id,
        electionYear: job.election_year,
        researchedAt,
        error: nomineeResult.error,
      });
      trackedFailure = true;
      if (nomineeResult.retryable) {
        throw toRetryableNomineeError(nomineeResult);
      }
      return summarizeNomineeFailure(job, nomineeResult, tracking);
    }

    try {
      const promotion = await promoteNominee({
        db: pool,
        primaryCycleId: job.cycle_id,
        electionYear: job.election_year,
        party: job.party,
        resolution: nomineeResult.resolution,
        confirmedAt: researchedAt,
      });
      const tracking = await markPresidentialNomineeResearchSuccess(pool, {
        cycleId: job.cycle_id,
        electionYear: job.election_year,
        researchedAt,
        stopResearch: promotion.status === "promoted",
      });
      return summarizeSuccess(job, nomineeResult, promotion, tracking);
    } catch (error) {
      if (error instanceof PromotePresidentialNomineeError) {
        const tracking = await markPresidentialNomineeResearchError(pool, {
          cycleId: job.cycle_id,
          electionYear: job.election_year,
          researchedAt,
          error,
        });
        trackedFailure = true;
        return summarizePromotionError(job, nomineeResult, error, tracking);
      }
      throw error;
    }
  } catch (error) {
    if (!trackedFailure) {
      try {
        await markPresidentialNomineeResearchError(pool, {
          cycleId: job.cycle_id,
          electionYear: job.election_year,
          researchedAt,
          error,
        });
      } catch {
        // Preserve the original failure so BullMQ retries the real cause.
      }
    }
    throw error;
  } finally {
    if (shouldClosePool) {
      await pool.end();
    }
  }
}

export function createPresidentialNomineeResearchWorker(concurrency = 1): Worker<
  PresidentialNomineeResearchJobData,
  PresidentialNomineeResearchJobResult
> {
  if (!Number.isInteger(concurrency) || concurrency <= 0) {
    throw new Error(`Invalid presidential nominee research worker concurrency: ${concurrency}`);
  }

  const env = getPipelineEnv();
  const processor: Processor<
    PresidentialNomineeResearchJobData,
    PresidentialNomineeResearchJobResult
  > = async (job) => {
    if (job.name !== PRESIDENTIAL_NOMINEE_RESEARCH_JOB_NAME) {
      throw new Error(`Unsupported presidential nominee research job name: ${job.name}`);
    }
    return processPresidentialNomineeResearchJob(job.data);
  };

  return new Worker<PresidentialNomineeResearchJobData, PresidentialNomineeResearchJobResult>(
    getQueueName(),
    processor,
    {
      connection: toConnectionOptions(env.REDIS_URL),
      concurrency,
    }
  );
}

export async function runPresidentialNomineeResearchEnricher(
  options: PresidentialNomineeResearchEnricherOptions = {}
): Promise<void> {
  const { once = false, blockMs = 5000, concurrency = 1 } = options;
  const worker = createPresidentialNomineeResearchWorker(concurrency);

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
