import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  parseElectionResultPayload,
  type ElectionResultPayload,
  type ParsedElectionResultPayloadRow,
} from "../contracts/electionResultPayloadContract.js";
import {
  loadElectionResultContexts,
  type ElectionResultContext,
} from "../pipeline/electionResults/electionResultContextLoader.js";
import {
  CERTIFIED_RESULT_MAX_ATTEMPTS,
  CERTIFIED_RESULT_RETRY_INTERVAL_DAYS,
  ELECTION_NIGHT_RESULT_MAX_ATTEMPTS,
} from "../pipeline/electionResults/electionResultRetryPolicy.js";
import {
  validateElectionResultSourceUrls,
  type ElectionResultSourceVerification,
} from "../pipeline/electionResults/electionResultSourceValidation.js";
import {
  createElectionResultRun,
  finishElectionResultRun,
  writeElectionResultPayloadRows,
} from "../pipeline/electionResults/electionResultWriter.js";
import {
  ELECTION_RESULT_PASS_TYPES,
  canProjectToCanonicalElectionStatus,
  type ElectionResultPassType,
} from "../types/electionResults.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { usLatestLocalDateIso } from "../utils/usLocalDate.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Manual (no AI provider) election-results workflow, mirroring the automated
// election_result_search pipeline: the same context loader, payload contract,
// source validation, writer, and checked-timestamp policy — only the research
// step is done by a human/agent instead of an AI call. The automated producer
// stays disabled (ELECTION_RESULTS_SCHEDULER_ENABLED unset); this script is
// the production path for results.
//
// Subcommands:
//   due      List elections past their US-local election date whose results
//            search is still open, using the producer's due conditions.
//   context  Print one election's research context (roster, ballot measure,
//            known sources) for building a results payload.
//   write    Validate a researched payload and write it through the shared
//            writer inside a transaction, marking attempt/checked timestamps.

const MANUAL_PROVIDER = "manual";
const MANUAL_MODEL = "claude-manual-research";

type Subcommand = "due" | "context" | "write";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:election-results:due -- [--state XX] [--limit 200]",
    "  npm run manual:election-results:context -- --election-id uuid",
    "  npm run manual:election-results:write -- --election-id uuid --pass-type election_night|certified --file payload.json [--dry-run]",
    "",
    "The write payload must match the election-result payload contract:",
    '  { "results": [ { "election_id", "result_status", "outcome", "winners", "source_url", "source_type", "notes"? } ] }',
  ].join("\n");
}

function readFlag(argv: readonly string[], name: string): string | null {
  const index = argv.indexOf(name);
  if (index >= 0) {
    const value = argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }
  const prefix = `${name}=`;
  const match = argv.find((token) => token.startsWith(prefix));
  if (!match) {
    return null;
  }
  const value = match.slice(prefix.length);
  if (value.trim().length === 0) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual election results`);
  }
  return value;
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name]?.trim() || String(fallback);
  if (!/^[1-9]\d*$/.test(raw)) {
    throw new Error(`Invalid ${name}: ${raw}. Expected a positive integer.`);
  }
  return Number(raw);
}

type DueRow = {
  election_id: string;
  state: string;
  district_name: string;
  district_type: string;
  race_type: string;
  official_ballot_title: string;
  election_date: string;
  election_stage: string | null;
  election_night_results_checked_at: string | null;
  election_night_results_attempt_count: number;
  certified_results_checked_at: string | null;
  certified_results_attempt_count: number;
  certified_results_last_attempted_at: string | null;
  candidate_count: number;
  has_ballot_measure: boolean;
};

// Mirrors the automated producer's due conditions (attempt caps and the
// certified retry interval included) so manual runs and the dormant producer
// agree about which elections still owe a results search.
const DUE_WHERE = `
  e.election_date <= $1::date
  AND (
    (
      e.election_night_results_checked_at IS NULL
      AND e.election_night_results_attempt_count < ${ELECTION_NIGHT_RESULT_MAX_ATTEMPTS}
    )
    OR (
      e.certified_results_checked_at IS NULL
      AND e.certified_results_attempt_count < ${CERTIFIED_RESULT_MAX_ATTEMPTS}
      AND (
        e.certified_results_last_attempted_at IS NULL
        OR e.certified_results_last_attempted_at <=
          now() - (${CERTIFIED_RESULT_RETRY_INTERVAL_DAYS} * interval '1 day')
      )
    )
  )
`;

function suggestedPass(row: DueRow): ElectionResultPassType {
  const nightOpen =
    !row.election_night_results_checked_at &&
    row.election_night_results_attempt_count < ELECTION_NIGHT_RESULT_MAX_ATTEMPTS;
  return nightOpen ? "election_night" : "certified";
}

async function runDue(pool: Pool, argv: readonly string[]): Promise<void> {
  const asOfDate = usLatestLocalDateIso();
  const stateFilter = readFlag(argv, "--state")?.trim().toUpperCase() ?? null;
  const limit = readPositiveIntegerFlag(argv, "--limit", 200);

  const params: unknown[] = [asOfDate];
  let stateClause = "";
  if (stateFilter) {
    params.push(stateFilter);
    stateClause = `AND d.state = $${params.length}`;
  }

  const totalResult = await pool.query<{ total: number }>(
    `
      SELECT count(*)::int AS total
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE ${DUE_WHERE}
      ${stateClause}
    `,
    params
  );

  params.push(limit);
  const result = await pool.query<DueRow>(
    `
      SELECT
        e.id::text AS election_id,
        d.state,
        d.name AS district_name,
        d.district_type,
        e.race_type,
        e.official_ballot_title,
        e.election_date::text AS election_date,
        e.election_stage,
        e.election_night_results_checked_at::text AS election_night_results_checked_at,
        e.election_night_results_attempt_count,
        e.certified_results_checked_at::text AS certified_results_checked_at,
        e.certified_results_attempt_count,
        e.certified_results_last_attempted_at::text AS certified_results_last_attempted_at,
        (
          SELECT count(*)::int
          FROM public.candidate_elections AS ce
          WHERE ce.election_id = e.id
        ) AS candidate_count,
        EXISTS (
          SELECT 1
          FROM public.ballot_measures AS bm
          WHERE bm.election_id = e.id
        ) AS has_ballot_measure
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE ${DUE_WHERE}
      ${stateClause}
      ORDER BY e.election_date ASC, d.state ASC, e.id ASC
      LIMIT $${params.length}::int
    `,
    params
  );

  console.log(
    JSON.stringify(
      {
        asOfDate,
        stateFilter,
        limit,
        totalDueCount: totalResult.rows[0]?.total ?? 0,
        listedCount: result.rows.length,
        due: result.rows.map((row) => ({
          ...row,
          suggested_pass: suggestedPass(row),
        })),
      },
      null,
      2
    )
  );
}

async function runContext(pool: Pool, argv: readonly string[]): Promise<void> {
  const electionId = readFlag(argv, "--election-id");
  if (!electionId) {
    throw new Error(`Missing --election-id.\n${usage()}`);
  }

  const contexts = await loadElectionResultContexts(pool, [electionId]);
  const context = contexts[0];
  if (!context) {
    throw new Error(`Election not found for election_id=${electionId}`);
  }

  const dueState = await pool.query<{
    election_night_results_checked_at: string | null;
    election_night_results_attempt_count: number;
    certified_results_checked_at: string | null;
    certified_results_attempt_count: number;
    certified_results_last_attempted_at: string | null;
  }>(
    `
      SELECT
        election_night_results_checked_at::text AS election_night_results_checked_at,
        election_night_results_attempt_count,
        certified_results_checked_at::text AS certified_results_checked_at,
        certified_results_attempt_count,
        certified_results_last_attempted_at::text AS certified_results_last_attempted_at
      FROM public.elections
      WHERE id::text = $1
    `,
    [electionId]
  );

  console.log(
    JSON.stringify(
      {
        asOfDate: usLatestLocalDateIso(),
        context,
        resultSearchState: dueState.rows[0] ?? null,
      },
      null,
      2
    )
  );
}

type ProjectionPreviewRow = {
  election_id: string;
  race_type: string;
  result_status: string;
  outcome: string;
  match_status: string;
  source_type: string;
  source_authority: ElectionResultSourceVerification["authority"];
  would_project_canonical: boolean;
};

function buildProjectionPreview(
  payload: ElectionResultPayload,
  contexts: readonly ElectionResultContext[],
  passType: ElectionResultPassType,
  sourceVerifications: readonly ElectionResultSourceVerification[]
): ProjectionPreviewRow[] {
  const contextById = new Map(contexts.map((context) => [context.electionId, context]));
  const authorityByUrl = new Map<string, ElectionResultSourceVerification["authority"]>();
  for (const verification of sourceVerifications) {
    authorityByUrl.set(verification.sourceUrl, verification.authority);
    authorityByUrl.set(verification.finalUrl, verification.authority);
  }

  return payload.results.map((row) => {
    const context = contextById.get(row.election_id);
    const authority = authorityByUrl.get(row.source_url) ?? "weak";
    const passGate = canProjectToCanonicalElectionStatus({
      passType,
      resultStatus: row.result_status,
      sourceType: row.source_type,
    });
    const wouldProject =
      context?.raceType === "ballot_measure"
        ? authority === "verified" && passGate && (row.outcome === "passed" || row.outcome === "failed")
        : authority === "verified" &&
          passGate &&
          row.match_status === "matched" &&
          row.winners.length > 0 &&
          row.winners.every((winner) => Boolean(winner.candidate_election_id)) &&
          (row.outcome === "won" || row.outcome === "advanced" || row.outcome === "runoff");
    return {
      election_id: row.election_id,
      race_type: context?.raceType ?? "unknown",
      result_status: row.result_status,
      outcome: row.outcome,
      match_status: row.match_status,
      source_type: row.source_type,
      source_authority: authority,
      would_project_canonical: wouldProject,
    };
  });
}

function unmatchedWinnerNames(rows: readonly ParsedElectionResultPayloadRow[]): Array<{
  election_id: string;
  candidate_name: string;
  party: string | null;
}> {
  return rows.flatMap((row) =>
    row.winners
      .filter((winner) => !winner.candidate_election_id)
      .map((winner) => ({
        election_id: row.election_id,
        candidate_name: winner.candidate_name ?? "",
        party: winner.party ?? null,
      }))
  );
}

async function runWrite(pool: Pool, argv: readonly string[]): Promise<void> {
  const electionId = readFlag(argv, "--election-id");
  const file = readFlag(argv, "--file");
  const passTypeRaw = readFlag(argv, "--pass-type");
  if (!electionId || !file || !passTypeRaw) {
    throw new Error(`Missing --election-id, --pass-type, or --file.\n${usage()}`);
  }
  const passType = passTypeRaw.trim().toLowerCase();
  if (!(ELECTION_RESULT_PASS_TYPES as readonly string[]).includes(passType)) {
    throw new Error(`Invalid --pass-type: ${passTypeRaw}. Expected election_night or certified.`);
  }
  const dryRun = argv.includes("--dry-run");

  const contexts = await loadElectionResultContexts(pool, [electionId]);
  const context = contexts[0];
  if (!context) {
    throw new Error(`Election not found for election_id=${electionId}`);
  }
  const asOfDate = usLatestLocalDateIso();
  if (context.electionDate > asOfDate) {
    throw new Error(
      `Election ${electionId} has election_date=${context.electionDate}, which is after the latest US local date ${asOfDate}; results cannot be searched before the election is over.`
    );
  }
  if (context.raceType === "ballot_measure" && !context.ballotMeasure) {
    throw new Error(
      `Election ${electionId} is a ballot measure without a ballot_measures row; run manual:ballot-measure:write first so the result has a measure to attach to.`
    );
  }

  const rawPayload = JSON.parse(await readFile(file, "utf8")) as unknown;
  const parsed = parseElectionResultPayload(rawPayload, { passType: passType as ElectionResultPassType, contexts });
  if (!parsed.ok) {
    throw new Error(`Election result payload failed validation: ${parsed.reason}`);
  }

  const sourceValidation = await validateElectionResultSourceUrls(parsed.payload, {
    timeoutMs: readPositiveIntegerEnv("AI_TIMEOUT_MS", 90000),
  });
  if (!sourceValidation.ok) {
    throw new Error(
      [
        `Election result source validation failed: ${sourceValidation.reason}`,
        ...sourceValidation.reviewFeedbackLines,
      ].join("\n")
    );
  }

  const payload = sourceValidation.payload;
  const projectionPreview = buildProjectionPreview(
    payload,
    contexts,
    passType as ElectionResultPassType,
    sourceValidation.sourceVerifications
  );
  const unmatchedWinners = unmatchedWinnerNames(payload.results);
  const warnings: string[] = [];
  if (context.raceType !== "ballot_measure" && context.candidates.length === 0) {
    warnings.push(
      "Election has an empty candidate roster; winners cannot match. Run the manual candidate-roster flow first if winners should link to candidates."
    );
  }
  if (unmatchedWinners.length > 0) {
    warnings.push(
      "Some winners did not match the roster. They are stored by name only; the automated draft emitter is not used by this manual flow, so create missing candidates through the manual roster/profile flow and re-run this write if they should link."
    );
  }

  if (dryRun) {
    console.log(
      JSON.stringify(
        {
          dryRun: true,
          electionId,
          passType,
          officialBallotTitle: context.officialBallotTitle,
          electionDate: context.electionDate,
          sourceVerifications: sourceValidation.sourceVerifications,
          projectionPreview,
          unmatchedWinners,
          warnings,
        },
        null,
        2
      )
    );
    return;
  }

  const runInput = {
    state: context.district.state,
    electionDate: context.electionDate,
    passType: passType as ElectionResultPassType,
    scheduledFor: null,
    runId: `manual:election-results:${electionId}:${passType}`,
  };
  const runDatabaseId = await createElectionResultRun(pool, runInput);
  let client: PoolClient | undefined;
  let transactionStarted = false;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    transactionStarted = true;
    const writeResult = await writeElectionResultPayloadRows(client, runDatabaseId, {
      ...runInput,
      contexts,
      payload,
      provider: MANUAL_PROVIDER,
      model: MANUAL_MODEL,
      sourceVerifications: sourceValidation.sourceVerifications,
      aiRawDebug: null,
    });
    await client.query("COMMIT");
    transactionStarted = false;

    await finishElectionResultRun(pool, runDatabaseId, {
      status: writeResult.runStatus,
      sourceSummary: {
        provider: MANUAL_PROVIDER,
        model: MANUAL_MODEL,
        result_count: payload.results.length,
      },
      rawPayload: payload,
    });

    const after = await pool.query<{
      election_night_results_checked_at: string | null;
      certified_results_checked_at: string | null;
    }>(
      `
        SELECT
          election_night_results_checked_at::text AS election_night_results_checked_at,
          certified_results_checked_at::text AS certified_results_checked_at
        FROM public.elections
        WHERE id::text = $1
      `,
      [electionId]
    );

    console.log(
      JSON.stringify(
        {
          electionId,
          passType,
          runDatabaseId,
          runStatus: writeResult.runStatus,
          electionRowsWritten: writeResult.electionRowsWritten,
          ballotMeasureRowsWritten: writeResult.ballotMeasureRowsWritten,
          checkedElectionCount: writeResult.checkedElectionCount,
          uncheckedElectionIds: writeResult.uncheckedElectionIds,
          canonicalCandidateStatusUpdates: writeResult.canonicalCandidateStatusUpdates,
          canonicalBallotMeasureUpdates: writeResult.canonicalBallotMeasureUpdates,
          projectionPreview,
          unmatchedWinners,
          warnings,
          electionSearchStateAfter: after.rows[0] ?? null,
        },
        null,
        2
      )
    );
  } catch (error) {
    if (transactionStarted && client) {
      await client.query("ROLLBACK").catch(() => undefined);
    }
    await finishElectionResultRun(pool, runDatabaseId, {
      status: "failed",
      sourceSummary: { provider: MANUAL_PROVIDER, model: MANUAL_MODEL },
      rawPayload: payload,
    }).catch((finishError) => {
      console.error("election result run failed-status update failed:", finishError);
    });
    throw error;
  } finally {
    client?.release();
  }
}

async function main(): Promise<void> {
  const [command, ...rest] = process.argv.slice(2);
  if (command !== "due" && command !== "context" && command !== "write") {
    throw new Error(`Unknown subcommand: ${command ?? "(none)"}.\n${usage()}`);
  }
  const subcommand: Subcommand = command;

  const flagSpecs = {
    due: [
      { name: "--state", value: "both" as const },
      { name: "--limit", value: "both" as const },
    ],
    context: [{ name: "--election-id", value: "both" as const }],
    write: [
      { name: "--election-id", value: "both" as const },
      { name: "--pass-type", value: "both" as const },
      { name: "--file", value: "both" as const },
      { name: "--dry-run", value: "none" as const },
    ],
  }[subcommand];
  assertKnownCliFlags(`manual:election-results:${subcommand}`, rest, flagSpecs);

  loadProjectEnv();
  const databaseUrl = requireEnv("DATABASE_URL");
  if (subcommand === "write") {
    requireLocalDatabaseTarget(databaseUrl);
  }

  const pool = new Pool({ connectionString: databaseUrl });
  try {
    if (subcommand === "due") {
      await runDue(pool, rest);
    } else if (subcommand === "context") {
      await runContext(pool, rest);
    } else {
      await runWrite(pool, rest);
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual election results failed:", message);
    process.exitCode = 1;
  });
}
