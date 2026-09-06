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
  computeElectionResultScheduledAtUtc,
  getElectionResultScheduleForState,
} from "../pipeline/electionResults/electionResultSchedules.js";
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
import { readPositiveIntegerEnv } from "../config/envReaders.js";

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
    "  npm run manual:election-results:write -- --election-id uuid --pass-type election_night|certified --file payload.json [--dry-run] [--force]",
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
  night_bucket_open: boolean;
  certified_bucket_open: boolean;
  candidate_count: number;
  has_ballot_measure: boolean;
};

// Keyset batch size for the due scan. The schedule gate runs in TS, so the
// scan pages through open-pass rows until the requested --limit of due rows
// is filled (or rows run out) — a burst of gated rows (one national election
// day can put thousands of same-date rows inside their certification window)
// can never starve genuinely due rows out of the list.
const DUE_SCAN_BATCH_SIZE = 1000;

// Per-pass openness by checked/attempt state, mirroring the automated
// producer (attempt caps and the certified retry interval included) so manual
// runs and the dormant producer agree about which elections still owe a
// results search. The state-specific schedule gate (poll close for
// election_night, certification offset for certified) is applied in TS after
// this query, also mirroring the producer.
//
// One deliberate divergence: the night bucket also requires the certified
// pass to be un-checked. The producer never faces that ordering (its night
// pass runs on election night, weeks before certified), but a manual sweep
// can reach an election only after certification — unofficial election-night
// results are obsolete once certified results are recorded, and without this
// condition such elections would nag the due list until three pointless
// night attempts were burned.
const NIGHT_BUCKET_OPEN_SQL = `
  (
    e.election_night_results_checked_at IS NULL
    AND e.election_night_results_attempt_count < ${ELECTION_NIGHT_RESULT_MAX_ATTEMPTS}
    AND e.certified_results_checked_at IS NULL
  )
`;

const CERTIFIED_BUCKET_OPEN_SQL = `
  (
    e.certified_results_checked_at IS NULL
    AND e.certified_results_attempt_count < ${CERTIFIED_RESULT_MAX_ATTEMPTS}
    AND (
      e.certified_results_last_attempted_at IS NULL
      OR e.certified_results_last_attempted_at <=
        now() - (${CERTIFIED_RESULT_RETRY_INTERVAL_DAYS} * interval '1 day')
    )
  )
`;

const DUE_WHERE = `
  e.election_date <= $1::date
  AND (${NIGHT_BUCKET_OPEN_SQL} OR ${CERTIFIED_BUCKET_OPEN_SQL})
`;

// null = the state has no election-result schedule entry; the producer skips
// such states entirely, but the manual flow is exactly the catch-all for
// them, so callers treat null as "no timing gate" and keep the row due.
async function computePassDueAt(
  pool: Pool,
  cache: Map<string, Date | null>,
  input: { state: string; electionDate: string; passType: ElectionResultPassType }
): Promise<Date | null> {
  const state = input.state.trim().toUpperCase();
  const key = `${state}:${input.electionDate}:${input.passType}`;
  const cached = cache.get(key);
  if (cached !== undefined) {
    return cached;
  }
  const dueAt = getElectionResultScheduleForState(state)
    ? await computeElectionResultScheduledAtUtc(pool, {
        state,
        electionDate: input.electionDate,
        passType: input.passType,
      })
    : null;
  cache.set(key, dueAt);
  return dueAt;
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

  // Apply the state-specific schedule gate per pass, like the producer: an
  // election-night search is not due before the state's poll-close instant,
  // and a certified search is not due before the state's certification
  // offset. Without this gate a manual sweep can burn all certified attempts
  // on not_final_yet writes weeks before certification exists, permanently
  // closing the pass.
  const now = new Date();
  const scheduleCache = new Map<string, Date | null>();
  type AnnotatedRow = DueRow & {
    suggested_pass: ElectionResultPassType;
    schedule_known: boolean;
    night_due_at: string | null;
    certified_due_at: string | null;
  };
  const due: AnnotatedRow[] = [];
  const scheduledLater: AnnotatedRow[] = [];
  let scannedCount = 0;
  let scanExhausted = false;
  let cursor: { electionDate: string; state: string; electionId: string } | null = null;

  while (due.length < limit) {
    const batchParams = [...params];
    let cursorClause = "";
    if (cursor) {
      batchParams.push(cursor.electionDate, cursor.state, cursor.electionId);
      cursorClause = `AND (e.election_date, d.state, e.id) > ($${batchParams.length - 2}::date, $${batchParams.length - 1}, $${batchParams.length}::uuid)`;
    }
    batchParams.push(DUE_SCAN_BATCH_SIZE);

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
          ${NIGHT_BUCKET_OPEN_SQL} AS night_bucket_open,
          ${CERTIFIED_BUCKET_OPEN_SQL} AS certified_bucket_open,
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
        ${cursorClause}
        ORDER BY e.election_date ASC, d.state ASC, e.id ASC
        LIMIT $${batchParams.length}::int
      `,
      batchParams
    );

    for (const row of result.rows) {
      const nightDueAt = row.night_bucket_open
        ? await computePassDueAt(pool, scheduleCache, {
            state: row.state,
            electionDate: row.election_date,
            passType: "election_night",
          })
        : undefined;
      const certifiedDueAt = row.certified_bucket_open
        ? await computePassDueAt(pool, scheduleCache, {
            state: row.state,
            electionDate: row.election_date,
            passType: "certified",
          })
        : undefined;

      const nightOpen =
        row.night_bucket_open && (nightDueAt === null || (nightDueAt !== undefined && now >= nightDueAt));
      const certifiedOpen =
        row.certified_bucket_open && (certifiedDueAt === null || (certifiedDueAt !== undefined && now >= certifiedDueAt));

      // Prefer certified whenever it is open: only certified official results
      // can update canonical candidate/ballot-measure state, so once the
      // certification window has arrived, election-night work is the fallback
      // (the skill records unofficial results through the night pass only
      // when the state turns out not to have certified yet).
      const suggestedPass: ElectionResultPassType = certifiedOpen
        ? "certified"
        : nightOpen
          ? "election_night"
          : row.certified_bucket_open
            ? "certified"
            : "election_night";
      const annotated: AnnotatedRow = {
        ...row,
        suggested_pass: suggestedPass,
        schedule_known: Boolean(getElectionResultScheduleForState(row.state)),
        night_due_at: nightDueAt instanceof Date ? nightDueAt.toISOString() : null,
        certified_due_at: certifiedDueAt instanceof Date ? certifiedDueAt.toISOString() : null,
      };

      if (nightOpen || certifiedOpen) {
        due.push(annotated);
      } else {
        scheduledLater.push(annotated);
      }
    }

    scannedCount += result.rows.length;
    const lastRow = result.rows[result.rows.length - 1];
    if (!lastRow || result.rows.length < DUE_SCAN_BATCH_SIZE) {
      scanExhausted = true;
      break;
    }
    cursor = { electionDate: lastRow.election_date, state: lastRow.state, electionId: lastRow.election_id };
  }

  console.log(
    JSON.stringify(
      {
        asOfDate,
        now: now.toISOString(),
        stateFilter,
        limit,
        scannedCount,
        // false = the scan stopped once --limit due rows were found, so
        // dueCount/scheduledLaterCount cover only the scanned prefix.
        scanExhausted,
        totalOpenElectionCount: totalResult.rows[0]?.total ?? 0,
        dueCount: due.length,
        listedCount: Math.min(due.length, limit),
        scheduledLaterCount: scheduledLater.length,
        due: due.slice(0, limit),
        scheduledLater: scheduledLater.slice(0, limit),
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
  const force = argv.includes("--force");

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

  // Lifecycle and timing gates, matching the automated producer. Every write
  // increments the pass's attempt counter and can mark the pass checked, so a
  // premature write is not harmless: three early certified attempts close
  // certified research permanently. --force is the reviewed escape hatch for
  // legitimate exceptions (a state that certified earlier than its schedule
  // offset, or a correction to an already-closed pass).
  const passState = await pool.query<{
    night_checked: boolean;
    certified_checked: boolean;
    night_attempt_count: number;
    certified_attempt_count: number;
    certified_in_cooldown: boolean;
  }>(
    `
      SELECT
        election_night_results_checked_at IS NOT NULL AS night_checked,
        certified_results_checked_at IS NOT NULL AS certified_checked,
        election_night_results_attempt_count AS night_attempt_count,
        certified_results_attempt_count AS certified_attempt_count,
        (
          certified_results_last_attempted_at IS NOT NULL
          AND certified_results_last_attempted_at >
            now() - (${CERTIFIED_RESULT_RETRY_INTERVAL_DAYS} * interval '1 day')
        ) AS certified_in_cooldown
      FROM public.elections
      WHERE id::text = $1
    `,
    [electionId]
  );
  const state = passState.rows[0];
  const alreadyChecked =
    passType === "election_night" ? (state?.night_checked ?? false) : (state?.certified_checked ?? false);
  if (alreadyChecked && !force) {
    throw new Error(
      `The ${passType} pass for election ${electionId} is already marked checked; this write would be a correction. Re-run with --force if the correction is intended.`
    );
  }
  if (passType === "election_night" && state?.certified_checked && !force) {
    throw new Error(
      `Election ${electionId} already has checked certified results; an election_night write would record obsolete unofficial results. Re-run with --force if that is really intended.`
    );
  }
  // Mirror the due list's attempt-cap and certified-cooldown predicates:
  // every write increments the pass's attempt counter, and the counter cap
  // force-marks the pass checked, so immediate re-runs could permanently
  // close an unresolved pass without ever passing through the due list.
  const attemptCount =
    passType === "election_night" ? (state?.night_attempt_count ?? 0) : (state?.certified_attempt_count ?? 0);
  const attemptCap =
    passType === "election_night" ? ELECTION_NIGHT_RESULT_MAX_ATTEMPTS : CERTIFIED_RESULT_MAX_ATTEMPTS;
  if (attemptCount >= attemptCap && !force) {
    throw new Error(
      `The ${passType} pass for election ${electionId} already used ${attemptCount} of ${attemptCap} attempts; another write would close it regardless of outcome. Re-run with --force if this final write is intended.`
    );
  }
  if (passType === "certified" && state?.certified_in_cooldown && !force) {
    throw new Error(
      `The certified pass for election ${electionId} was attempted within the last ${CERTIFIED_RESULT_RETRY_INTERVAL_DAYS} days and is in its retry cooldown; writing again now would burn an attempt on the same unresolved question. Re-run with --force only for a correction or when the state has certified since the last attempt.`
    );
  }
  const now = new Date();
  const passDueAt = await computePassDueAt(pool, new Map(), {
    state: context.district.state,
    electionDate: context.electionDate,
    passType: passType as ElectionResultPassType,
  });
  if (passDueAt !== null && now < passDueAt && !force) {
    throw new Error(
      `The ${passType} pass for election ${electionId} (${context.district.state}) is not due until ${passDueAt.toISOString()} — before poll close for election_night, before the state's certification offset for certified. Re-run with --force only if the result is genuinely available early from an official source.`
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
  // The failed-status catch covers only BEGIN..COMMIT (matching the
  // enricher): once the transaction commits, the data is live, and a failure
  // in post-commit reporting must not relabel a successful run as failed —
  // that would invite a re-run and a double attempt increment.
  let writeResult;
  let client: PoolClient | undefined;
  let transactionStarted = false;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    transactionStarted = true;
    writeResult = await writeElectionResultPayloadRows(client, runDatabaseId, {
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
        resultNotificationEventsCreated: writeResult.resultNotificationEventsCreated,
        projectionPreview,
        unmatchedWinners,
        warnings,
        electionSearchStateAfter: after.rows[0] ?? null,
      },
      null,
      2
    )
  );
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
      { name: "--force", value: "none" as const },
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
