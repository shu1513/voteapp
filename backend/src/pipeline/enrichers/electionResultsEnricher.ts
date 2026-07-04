import { Queue, Worker, type Processor } from "bullmq";
import { toConnectionOptions } from "../../utils/redisConnection.js";
import { Pool } from "pg";
import type { PoolClient } from "pg";
import { createClient } from "redis";

import {
  buildElectionResultAiConfigFromEnv,
  enrichElectionResults,
  type ElectionResultAiConfig,
} from "../../ai/enrichElectionResults.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  ELECTION_RESULT_SEARCH_JOB_NAME,
  type ElectionResultSearchJobData,
} from "../producers/electionResultScheduleProducer.js";
import {
  chunkElectionResultContexts,
  loadElectionResultContexts,
} from "../electionResults/electionResultContextLoader.js";
import {
  createElectionResultRun,
  finishElectionResultRun,
  writeElectionResultPayloadRows,
  type ElectionResultWriteResult,
} from "../electionResults/electionResultWriter.js";
import type { AiCandidate } from "../../ai/aiCandidates.js";
import { enqueueCandidateProfileDrafts } from "../candidates/candidateProfileDraftEmitter.js";
import { clearElectionResultPassEmitted } from "../electionResults/electionResultPassMarkers.js";
import { buildCandidateProfileDraftsForUnmatchedElectionResultWinners } from "../electionResults/electionResultUnmatchedWinnerDrafts.js";

export type ElectionResultsEnricherOptions = {
  once?: boolean;
  blockMs?: number;
  concurrency?: number;
};

export type ElectionResultSearchJobResult = {
  state: string;
  election_date: string;
  pass_type: ElectionResultSearchJobData["pass_type"];
  requested_election_count: number;
  loaded_election_count: number;
  chunk_count: number;
  results_written: number;
  ballot_measure_results_written: number;
  checked_election_count: number;
  candidate_profile_drafts_emitted: number;
  candidate_profile_drafts_skipped: number;
  canonical_candidate_status_updates: number;
  canonical_ballot_measure_updates: number;
  run_ids: string[];
};

type RedisFanoutClient = {
  sendCommand(args: string[]): Promise<unknown>;
  del(key: string): Promise<number>;
};

type ChunkWriteResult = ElectionResultWriteResult & {
  candidateProfileDraftsEmitted: number;
  candidateProfileDraftsSkipped: number;
};

function getSearchQueueName(): string {
  return process.env.ELECTION_RESULT_SEARCH_QUEUE?.trim() || "election_result_search";
}

async function writeChunk(
  pool: Pool,
  input: {
    job: ElectionResultSearchJobData;
    contexts: Awaited<ReturnType<typeof loadElectionResultContexts>>;
    aiConfig: ElectionResultAiConfig;
    candidates?: readonly AiCandidate[];
    getRedisFanoutClient: () => Promise<RedisFanoutClient>;
  }
): Promise<ChunkWriteResult> {
  const aiResult = await enrichElectionResults(
    {
      passType: input.job.pass_type,
      scheduledFor: input.job.scheduled_for,
      contexts: input.contexts,
    },
    input.aiConfig,
    input.candidates
  );
  if (!aiResult.ok) {
    throw new Error(`election result AI failed: ${aiResult.reason}`);
  }

  const runInput = {
    state: input.job.state,
    electionDate: input.job.election_date,
    passType: input.job.pass_type,
    scheduledFor: input.job.scheduled_for,
    runId: input.job.run_id,
  };
  const runDatabaseId = await createElectionResultRun(pool, runInput);
  let client: PoolClient | undefined;
  let transactionStarted = false;
  let writeResult: ElectionResultWriteResult;
  let candidateProfileDraftsEmitted = 0;
  let candidateProfileDraftsSkipped = 0;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    transactionStarted = true;
    writeResult = await writeElectionResultPayloadRows(client, runDatabaseId, {
      ...runInput,
      contexts: input.contexts,
      payload: aiResult.payload,
      provider: aiResult.provider,
      model: aiResult.model,
      sourceVerifications: aiResult.sourceVerifications,
      aiRawDebug: aiResult.aiRawDebug,
    });
    await client.query("COMMIT");
    transactionStarted = false;

    const candidateProfileDrafts = buildCandidateProfileDraftsForUnmatchedElectionResultWinners({
      contexts: input.contexts,
      payload: aiResult.payload,
      runId: input.job.run_id,
    });
    if (candidateProfileDrafts.length > 0) {
      const redis = await input.getRedisFanoutClient();
      const emitted = await enqueueCandidateProfileDrafts(redis, candidateProfileDrafts);
      candidateProfileDraftsEmitted = emitted.emittedCount;
      candidateProfileDraftsSkipped = emitted.skippedCount;
    }

    if (input.job.pass_type === "certified" && writeResult.uncheckedElectionIds.length > 0) {
      const redis = await input.getRedisFanoutClient();
      for (const electionId of writeResult.uncheckedElectionIds) {
        await clearElectionResultPassEmitted(redis, {
          electionId,
          passType: input.job.pass_type,
        });
      }
    }
  } catch (error) {
    if (transactionStarted && client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error("election result write rollback failed:", rollbackError);
      }
    }
    try {
      await finishElectionResultRun(pool, runDatabaseId, {
        status: "failed",
        sourceSummary: {
          provider: aiResult.provider,
          model: aiResult.model,
        },
        rawPayload: aiResult.payload,
      });
    } catch (finishError) {
      console.error("election result run failed-status update failed:", finishError);
    }
    throw error;
  } finally {
    client?.release();
  }

  await finishElectionResultRun(pool, runDatabaseId, {
    status: writeResult.runStatus,
    sourceSummary: {
      provider: aiResult.provider,
      model: aiResult.model,
      result_count: aiResult.payload.results.length,
    },
    rawPayload: aiResult.payload,
  });
  return {
    ...writeResult,
    candidateProfileDraftsEmitted,
    candidateProfileDraftsSkipped,
  };
}

export async function processElectionResultSearchJob(
  job: ElectionResultSearchJobData,
  options: {
    pool?: Pool;
    aiConfig?: ElectionResultAiConfig;
    candidates?: readonly AiCandidate[];
    redisFanoutClient?: RedisFanoutClient;
  } = {}
): Promise<ElectionResultSearchJobResult> {
  const env = getPipelineEnv();
  const pool = options.pool ?? new Pool({ connectionString: env.DATABASE_URL });
  const shouldClosePool = !options.pool;
  const aiConfig = options.aiConfig ?? buildElectionResultAiConfigFromEnv();
  let redisFanoutClient: ReturnType<typeof createClient> | undefined;

  const getRedisFanoutClient = async (): Promise<RedisFanoutClient> => {
    if (options.redisFanoutClient) {
      return options.redisFanoutClient;
    }
    if (!redisFanoutClient) {
      redisFanoutClient = createClient({ url: env.REDIS_URL });
      await redisFanoutClient.connect();
    }
    return redisFanoutClient;
  };

  try {
    const contexts = await loadElectionResultContexts(pool, job.election_ids);
    const chunks = chunkElectionResultContexts(contexts, 10);
    const runIds: string[] = [];
    let resultsWritten = 0;
    let ballotMeasureResultsWritten = 0;
    let checkedElectionCount = 0;
    let candidateProfileDraftsEmitted = 0;
    let candidateProfileDraftsSkipped = 0;
    let canonicalCandidateStatusUpdates = 0;
    let canonicalBallotMeasureUpdates = 0;

    for (const chunk of chunks) {
      const writeResult = await writeChunk(pool, {
        job,
        contexts: chunk,
        aiConfig,
        candidates: options.candidates,
        getRedisFanoutClient,
      });
      runIds.push(writeResult.runId);
      resultsWritten += writeResult.electionRowsWritten;
      ballotMeasureResultsWritten += writeResult.ballotMeasureRowsWritten;
      checkedElectionCount += writeResult.checkedElectionCount;
      candidateProfileDraftsEmitted += writeResult.candidateProfileDraftsEmitted;
      candidateProfileDraftsSkipped += writeResult.candidateProfileDraftsSkipped;
      canonicalCandidateStatusUpdates += writeResult.canonicalCandidateStatusUpdates;
      canonicalBallotMeasureUpdates += writeResult.canonicalBallotMeasureUpdates;
    }

    return {
      state: job.state,
      election_date: job.election_date,
      pass_type: job.pass_type,
      requested_election_count: job.election_ids.length,
      loaded_election_count: contexts.length,
      chunk_count: chunks.length,
      results_written: resultsWritten,
      ballot_measure_results_written: ballotMeasureResultsWritten,
      checked_election_count: checkedElectionCount,
      candidate_profile_drafts_emitted: candidateProfileDraftsEmitted,
      candidate_profile_drafts_skipped: candidateProfileDraftsSkipped,
      canonical_candidate_status_updates: canonicalCandidateStatusUpdates,
      canonical_ballot_measure_updates: canonicalBallotMeasureUpdates,
      run_ids: runIds,
    };
  } finally {
    if (redisFanoutClient?.isOpen) {
      await redisFanoutClient.quit();
    }
    if (shouldClosePool) {
      await pool.end();
    }
  }
}

export function createElectionResultSearchWorker(concurrency = 1): Worker<
  ElectionResultSearchJobData,
  ElectionResultSearchJobResult
> {
  if (!Number.isInteger(concurrency) || concurrency <= 0) {
    throw new Error(`Invalid election result worker concurrency: ${concurrency}`);
  }

  const env = getPipelineEnv();
  const processor: Processor<ElectionResultSearchJobData, ElectionResultSearchJobResult> = async (job) => {
    if (job.name !== ELECTION_RESULT_SEARCH_JOB_NAME) {
      throw new Error(`Unsupported election result search job name: ${job.name}`);
    }
    return processElectionResultSearchJob(job.data);
  };

  return new Worker<ElectionResultSearchJobData, ElectionResultSearchJobResult>(
    getSearchQueueName(),
    processor,
    {
      connection: toConnectionOptions(env.REDIS_URL),
      concurrency,
    }
  );
}

export async function runElectionResultsEnricher(options: ElectionResultsEnricherOptions = {}): Promise<void> {
  const { once = false, blockMs = 5000, concurrency = 1 } = options;
  const worker = createElectionResultSearchWorker(concurrency);

  if (!once) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      void worker.close().then(resolve, reject);
    }, blockMs);

    worker.once("completed", () => {
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
