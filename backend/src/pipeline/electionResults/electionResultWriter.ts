import type { PoolClient, QueryResult, QueryResultRow } from "pg";

import type {
  ElectionResultPayload,
  ParsedElectionResultPayloadRow,
} from "../../contracts/electionResultPayloadContract.js";
import {
  canProjectToCanonicalElectionStatus,
  type BallotMeasureResultOutcome,
  type CandidateElectionStatus,
  type ElectionResultMatchStatus,
  type ElectionResultPassType,
} from "../../types/electionResults.js";
import type { ElectionResultContext } from "./electionResultContextLoader.js";
import { markElectionResultPassAttemptAndChecked } from "./electionResultCheckedTimestamps.js";
import type { ElectionResultSourceVerification } from "./electionResultSourceValidation.js";
import { createElectionResultNotificationEvents } from "../users/electionResultNotificationEvents.js";

export type ElectionResultRunInput = {
  state: string;
  electionDate: string;
  passType: ElectionResultPassType;
  scheduledFor?: string | null;
  runId?: string | null;
};

export type ElectionResultWriteInput = ElectionResultRunInput & {
  contexts: readonly ElectionResultContext[];
  payload: ElectionResultPayload;
  provider: string;
  model: string;
  sourceVerifications?: readonly ElectionResultSourceVerification[];
  aiRawDebug?: Record<string, unknown> | null;
  checkedAt?: Date;
};

export type ElectionResultWriteResult = {
  runId: string;
  runStatus: "completed" | "partial";
  electionRowsWritten: number;
  ballotMeasureRowsWritten: number;
  checkedElectionCount: number;
  uncheckedElectionIds: string[];
  canonicalCandidateStatusUpdates: number;
  canonicalBallotMeasureUpdates: number;
  resultNotificationEventsCreated: number;
};

type RunRow = {
  id: string;
};

type Queryable = {
  query<T extends QueryResultRow = QueryResultRow>(sql: string, values?: unknown[]): Promise<QueryResult<T>>;
};

function contextByElectionId(contexts: readonly ElectionResultContext[]): Map<string, ElectionResultContext> {
  return new Map(contexts.map((context) => [context.electionId, context]));
}

function deriveRunStatus(rows: readonly ParsedElectionResultPayloadRow[]): "completed" | "partial" {
  const hasIncomplete = rows.some((row) =>
    row.result_status === "not_found" ||
    row.result_status === "not_final_yet" ||
    row.match_status === "partial" ||
    row.match_status === "unmatched"
  );
  return hasIncomplete ? "partial" : "completed";
}

function rawResultForRow(
  row: ParsedElectionResultPayloadRow,
  input: Pick<ElectionResultWriteInput, "provider" | "model" | "aiRawDebug">
): Record<string, unknown> {
  return {
    notes: row.notes,
    provider: input.provider,
    model: input.model,
    ...(input.aiRawDebug ? { ai_raw_debug: input.aiRawDebug } : {}),
  };
}

function sourceAuthorityByUrl(
  verifications: readonly ElectionResultSourceVerification[] | undefined
): Map<string, ElectionResultSourceVerification["authority"]> {
  const mapped = new Map<string, ElectionResultSourceVerification["authority"]>();
  for (const verification of verifications ?? []) {
    mapped.set(verification.sourceUrl, verification.authority);
    mapped.set(verification.finalUrl, verification.authority);
  }
  return mapped;
}

export async function createElectionResultRun(
  client: Queryable,
  input: ElectionResultRunInput
): Promise<string> {
  const result = await client.query<RunRow>(
    `
      INSERT INTO public.election_result_runs (
        state,
        election_date,
        pass_type,
        status,
        scheduled_for,
        started_at,
        run_id
      )
      VALUES ($1, $2::date, $3, 'running', $4::timestamptz, now(), $5)
      RETURNING id
    `,
    [
      input.state,
      input.electionDate,
      input.passType,
      input.scheduledFor ?? null,
      input.runId ?? null,
    ]
  );
  const id = result.rows[0]?.id;
  if (!id) {
    throw new Error("Failed to create election_result_runs row");
  }
  return id;
}

export async function finishElectionResultRun(
  client: Queryable,
  runId: string,
  input: {
    status: "completed" | "partial" | "failed";
    sourceSummary?: Record<string, unknown> | null;
    rawPayload?: Record<string, unknown> | null;
  }
): Promise<void> {
  await client.query(
    `
      UPDATE public.election_result_runs
      SET status = $2,
          completed_at = now(),
          source_summary = $3::jsonb,
          raw_payload = $4::jsonb,
          updated_at = now()
      WHERE id = $1
    `,
    [
      runId,
      input.status,
      input.sourceSummary ? JSON.stringify(input.sourceSummary) : null,
      input.rawPayload ? JSON.stringify(input.rawPayload) : null,
    ]
  );
}

async function upsertElectionResultRow(
  client: PoolClient,
  runId: string,
  row: ParsedElectionResultPayloadRow,
  input: Pick<ElectionResultWriteInput, "passType" | "provider" | "model" | "aiRawDebug">
): Promise<void> {
  await client.query(
    `
      INSERT INTO public.election_results (
        election_result_run_id,
        pass_type,
        election_id,
        result_status,
        outcome,
        winners,
        match_status,
        source_url,
        source_type,
        raw_result
      )
      VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10::jsonb)
      ON CONFLICT (election_id, pass_type)
      DO UPDATE SET
        election_result_run_id = EXCLUDED.election_result_run_id,
        result_status = EXCLUDED.result_status,
        outcome = EXCLUDED.outcome,
        winners = EXCLUDED.winners,
        match_status = EXCLUDED.match_status,
        source_url = EXCLUDED.source_url,
        source_type = EXCLUDED.source_type,
        raw_result = EXCLUDED.raw_result,
        retrieved_at = now(),
        updated_at = now()
    `,
    [
      runId,
      input.passType,
      row.election_id,
      row.result_status,
      row.outcome,
      JSON.stringify(row.winners),
      row.match_status,
      row.source_url,
      row.source_type,
      JSON.stringify(rawResultForRow(row, input)),
    ]
  );
}

async function upsertBallotMeasureResultRow(
  client: PoolClient,
  runId: string,
  context: ElectionResultContext,
  row: ParsedElectionResultPayloadRow,
  input: Pick<ElectionResultWriteInput, "passType" | "provider" | "model" | "aiRawDebug">
): Promise<void> {
  if (!context.ballotMeasure) {
    throw new Error(`Missing ballot_measure context for election_id=${context.electionId}`);
  }
  await client.query(
    `
      INSERT INTO public.ballot_measure_results (
        election_result_run_id,
        pass_type,
        ballot_measure_id,
        election_id,
        result_status,
        outcome,
        source_url,
        source_type,
        raw_result
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
      ON CONFLICT (ballot_measure_id, pass_type)
      DO UPDATE SET
        election_result_run_id = EXCLUDED.election_result_run_id,
        result_status = EXCLUDED.result_status,
        outcome = EXCLUDED.outcome,
        source_url = EXCLUDED.source_url,
        source_type = EXCLUDED.source_type,
        raw_result = EXCLUDED.raw_result,
        retrieved_at = now(),
        updated_at = now()
    `,
    [
      runId,
      input.passType,
      context.ballotMeasure.ballotMeasureId,
      context.electionId,
      row.result_status,
      row.outcome,
      row.source_url,
      row.source_type,
      JSON.stringify(rawResultForRow(row, input)),
    ]
  );
}

// Outcomes worth announcing to users: an office race decided (or advanced to
// the next round) and a measure passed/failed. Everything else — unknown,
// too_close, and the terminal-missing statuses — has no result to report.
const DECISIVE_RESULT_OUTCOMES = new Set<ParsedElectionResultPayloadRow["outcome"]>([
  "won",
  "advanced",
  "runoff",
  "passed",
  "failed",
]);

function winnerStatusForOutcome(outcome: ParsedElectionResultPayloadRow["outcome"]): CandidateElectionStatus | null {
  if (outcome === "won") {
    return "won";
  }
  if (outcome === "advanced") {
    return "advanced";
  }
  if (outcome === "runoff") {
    return "runoff";
  }
  return null;
}

function canProjectOfficeRow(input: {
  passType: ElectionResultPassType;
  row: ParsedElectionResultPayloadRow;
  sourceAuthorities: ReadonlyMap<string, ElectionResultSourceVerification["authority"]>;
}): boolean {
  return (
    (input.sourceAuthorities.get(input.row.source_url) ?? "weak") === "verified" &&
    input.row.match_status === "matched" &&
    input.row.winners.length > 0 &&
    input.row.winners.every((winner) => Boolean(winner.candidate_election_id)) &&
    canProjectToCanonicalElectionStatus({
      passType: input.passType,
      resultStatus: input.row.result_status,
      sourceType: input.row.source_type,
    }) &&
    winnerStatusForOutcome(input.row.outcome) !== null
  );
}

async function projectCertifiedOfficeResultIfEligible(
  client: PoolClient,
  row: ParsedElectionResultPayloadRow,
  passType: ElectionResultPassType,
  sourceAuthorities: ReadonlyMap<string, ElectionResultSourceVerification["authority"]>
): Promise<number> {
  if (!canProjectOfficeRow({ passType, row, sourceAuthorities })) {
    return 0;
  }
  const winnerStatus = winnerStatusForOutcome(row.outcome);
  if (!winnerStatus) {
    return 0;
  }
  const winnerCandidateElectionIds = row.winners
    .map((winner) => winner.candidate_election_id)
    .filter((id): id is string => typeof id === "string" && id.length > 0);

  const result = await client.query(
    `
      UPDATE public.candidate_elections
      SET status = CASE
            WHEN id = ANY($2::uuid[]) THEN $3
            ELSE 'lost'
          END,
          updated_at = now()
      WHERE election_id = $1
        AND status <> 'withdrawn'
    `,
    [row.election_id, winnerCandidateElectionIds, winnerStatus]
  );
  return result.rowCount ?? 0;
}

function ballotMeasureCanonicalResult(
  input: {
    passType: ElectionResultPassType;
    row: ParsedElectionResultPayloadRow;
    sourceAuthorities: ReadonlyMap<string, ElectionResultSourceVerification["authority"]>;
  }
): "passed" | "failed" | null {
  if (
    (input.sourceAuthorities.get(input.row.source_url) ?? "weak") !== "verified" ||
    input.passType !== "certified" ||
    input.row.result_status !== "certified" ||
    input.row.source_type !== "official"
  ) {
    return null;
  }
  const outcome = input.row.outcome as BallotMeasureResultOutcome;
  if (outcome === "passed") {
    return "passed";
  }
  if (outcome === "failed") {
    return "failed";
  }
  return null;
}

async function projectCertifiedBallotMeasureResultIfEligible(
  client: PoolClient,
  context: ElectionResultContext,
  row: ParsedElectionResultPayloadRow,
  passType: ElectionResultPassType,
  sourceAuthorities: ReadonlyMap<string, ElectionResultSourceVerification["authority"]>
): Promise<number> {
  const result = ballotMeasureCanonicalResult({ passType, row, sourceAuthorities });
  if (!result || !context.ballotMeasure) {
    return 0;
  }
  const update = await client.query(
    `
      UPDATE public.ballot_measures
      SET result = $2,
          updated_at = now()
      WHERE id = $1
    `,
    [context.ballotMeasure.ballotMeasureId, result]
  );
  return update.rowCount ?? 0;
}

export async function writeElectionResultPayload(
  client: PoolClient,
  input: ElectionResultWriteInput
): Promise<ElectionResultWriteResult> {
  const runId = await createElectionResultRun(client, input);

  try {
    const result = await writeElectionResultPayloadRows(client, runId, input);
    await finishElectionResultRun(client, runId, {
      status: result.runStatus,
      sourceSummary: {
        provider: input.provider,
        model: input.model,
        result_count: input.payload.results.length,
      },
      rawPayload: input.payload,
    });

    return result;
  } catch (error) {
    await finishElectionResultRun(client, runId, {
      status: "failed",
      sourceSummary: {
        provider: input.provider,
        model: input.model,
      },
      rawPayload: input.payload,
    });
    throw error;
  }
}

export async function writeElectionResultPayloadRows(
  client: PoolClient,
  runId: string,
  input: ElectionResultWriteInput
): Promise<ElectionResultWriteResult> {
  const contextById = contextByElectionId(input.contexts);
  const sourceAuthorities = sourceAuthorityByUrl(input.sourceVerifications);
  let electionRowsWritten = 0;
  let ballotMeasureRowsWritten = 0;
  let canonicalCandidateStatusUpdates = 0;
  let canonicalBallotMeasureUpdates = 0;

  for (const row of input.payload.results) {
    const context = contextById.get(row.election_id);
    if (!context) {
      throw new Error(`Missing context for election_id=${row.election_id}`);
    }

    if (context.raceType === "ballot_measure") {
      await upsertBallotMeasureResultRow(client, runId, context, row, input);
      ballotMeasureRowsWritten += 1;
      canonicalBallotMeasureUpdates += await projectCertifiedBallotMeasureResultIfEligible(
        client,
        context,
        row,
        input.passType,
        sourceAuthorities
      );
    } else {
      await upsertElectionResultRow(client, runId, row, input);
      electionRowsWritten += 1;
      canonicalCandidateStatusUpdates += await projectCertifiedOfficeResultIfEligible(
        client,
        row,
        input.passType,
        sourceAuthorities
      );
    }
  }

  const passMarking = await markElectionResultPassAttemptAndChecked(client, {
    rows: input.payload.results,
    passType: input.passType,
    checkedAt: input.checkedAt ?? new Date(),
  });
  const runStatus = deriveRunStatus(input.payload.results);

  // Same transaction as the result upserts: notification events commit
  // atomically with the rows they announce. Only decisive outcomes fan out —
  // not_found / not_final_yet / unknown / too_close rows carry nothing worth
  // emailing — and the (user, election) unique constraint absorbs re-writes.
  const decisiveElectionIds = input.payload.results
    .filter((row) => DECISIVE_RESULT_OUTCOMES.has(row.outcome))
    .map((row) => row.election_id);
  const notification = await createElectionResultNotificationEvents(client, decisiveElectionIds);

  return {
    runId,
    runStatus,
    electionRowsWritten,
    ballotMeasureRowsWritten,
    checkedElectionCount: passMarking.checkedElectionCount,
    uncheckedElectionIds: passMarking.uncheckedElectionIds,
    canonicalCandidateStatusUpdates,
    canonicalBallotMeasureUpdates,
    resultNotificationEventsCreated: notification.createdCount,
  };
}
