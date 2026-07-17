// Guarded single-link withdrawal.
//
// A real withdrawal — the candidate actually dropped out of the race — keeps
// the candidate_elections row as history with status = 'withdrawn'. Read
// paths hide withdrawn links from the ballot and election detail pages, the
// results writer already refuses to flip a withdrawn link to won/lost, and
// followers with election alerts on get a digest notification. This wrapper
// is NOT for research errors (a link that should never have existed):
// manual:candidate-elections:unlink removes those silently, with no
// notification, because notifying followers about our own mistake is noise.
//
// Guard rails, all of which must pass before a single row changes:
// - the link (candidate, election) exists and is row-locked;
// - the link status is 'declared': a link that is already withdrawn needs no
//   second withdrawal (and re-running would be a sign of operator confusion),
//   and a link with a result status (won/lost/advanced/runoff) is settled —
//   withdrawing it would falsify a recorded result;
// - the election has not happened yet (US-local boundary): a withdrawal
//   recorded after election day is history the results flow already covers;
// - unsent "on the ballot" (candidate_future_election) events for the pair
//   are deleted in the same transaction, so one digest cannot announce a
//   candidacy and its withdrawal side by side;
// - withdrawal notification events are created in the same transaction as
//   the status change (all-or-nothing);
// - local-database guard, single transaction, --dry-run (executes everything
//   and rolls back, so the reported counts are real).
import { pathToFileURL } from "node:url";

import { Pool, type PoolClient } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { createCandidateElectionWithdrawalNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";
import { US_LATEST_LOCAL_DATE_SQL } from "../utils/usLocalDate.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

type QueryResultLike<T> = { rows: T[]; rowCount?: number | null };

export type WithdrawCandidateElectionClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type WithdrawCandidateElectionOptions = {
  candidateId: string;
  electionId: string;
  dryRun: boolean;
};

export type WithdrawCandidateElectionResult = {
  action: "withdrawn";
  dryRun: boolean;
  candidateId: string;
  electionId: string;
  electionTitle: string;
  electionDate: string;
  staleFutureElectionEventsDeleted: number;
  withdrawalEventsCreated: number;
};

type LinkRow = {
  id: string;
  status: string;
};

type ElectionRow = {
  id: string;
  official_ballot_title: string;
  election_date: string;
  is_upcoming: boolean;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function usage(): string {
  return [
    "Mark one candidate_elections link withdrawn and notify election-alert followers.",
    "",
    "Usage:",
    "  npm run manual:candidate-elections:withdraw -- --candidate-id uuid --election-id uuid --reason text [--dry-run]",
    "",
    "For links that should never have existed (research errors), use",
    "manual:candidate-elections:unlink instead — it removes the link silently.",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index < 0) return null;
  const value = process.argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value.trim();
}

function requireFlag(name: string): string {
  const value = readFlag(name);
  if (!value) throw new Error(`Missing ${name}.\n${usage()}`);
  return value;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required for manual candidate-election withdrawal`);
  return value;
}

export async function runWithdrawCandidateElection(
  client: WithdrawCandidateElectionClient,
  options: WithdrawCandidateElectionOptions
): Promise<WithdrawCandidateElectionResult> {
  // PostgreSQL returns uuid columns lowercased; a valid uppercase input
  // would otherwise fail the row-matching below with a false "not found".
  const candidateId = options.candidateId.toLowerCase();
  const electionId = options.electionId.toLowerCase();
  const { dryRun } = options;

  await client.query("BEGIN");
  try {
    const linkResult = await client.query<LinkRow>(
      `
        SELECT id, status
        FROM public.candidate_elections
        WHERE candidate_id = $1::uuid AND election_id = $2::uuid
        FOR UPDATE
      `,
      [candidateId, electionId]
    );
    const link = linkResult.rows[0];
    if (!link) {
      throw new Error(
        `No candidate_elections link found for candidate ${candidateId} on election ${electionId}`
      );
    }
    if (link.status === "withdrawn") {
      throw new Error(`Link ${link.id} is already withdrawn; nothing to do.`);
    }
    if (link.status !== "declared") {
      throw new Error(
        `Link ${link.id} has status '${link.status}' — a settled result. Withdrawing it would falsify ` +
          "the recorded outcome; if the result rows are wrong, resolve those first (user decision)."
      );
    }

    // Locked like manual:candidate-elections:move locks its elections: the
    // upcoming-election guard must not pass on a date a concurrent
    // manual:election-date:correct is changing mid-transaction.
    const electionResult = await client.query<ElectionRow>(
      `
        SELECT
          id,
          official_ballot_title,
          election_date::text AS election_date,
          (election_date >= ${US_LATEST_LOCAL_DATE_SQL}) AS is_upcoming
        FROM public.elections
        WHERE id = $1::uuid
        FOR UPDATE
      `,
      [electionId]
    );
    const election = electionResult.rows[0];
    if (!election) {
      throw new Error(`Election not found: ${electionId}`);
    }
    if (!election.is_upcoming) {
      throw new Error(
        `Election ${electionId} (${election.election_date}) has already happened; a withdrawal after ` +
          "election day is history the results flow covers, not news. Refusing."
      );
    }

    await client.query(
      `
        UPDATE public.candidate_elections
        SET status = 'withdrawn', updated_at = now()
        WHERE id = $1::uuid
      `,
      [link.id]
    );

    // One digest must not announce a candidacy next to its withdrawal:
    // unsent "on the ballot" events for this pair are wrong now. Sent ones
    // cannot be recalled.
    const staleEvents = await client.query(
      `
        DELETE FROM public.user_candidate_follow_notification_events
        WHERE candidate_id = $1::uuid
          AND election_id = $2::uuid
          AND event_type = 'candidate_future_election'
          AND notified_at IS NULL
      `,
      [candidateId, electionId]
    );

    // The creator types its handle as pg's overloaded query signature; this
    // wrapper's minimal (text, values) client satisfies it at runtime — the
    // creator only ever calls query(text, values).
    const notification = await createCandidateElectionWithdrawalNotificationEvents(
      client as unknown as Pick<PoolClient, "query">,
      { candidateId, electionId }
    );

    if (dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query("COMMIT");
    }
    return {
      action: "withdrawn",
      dryRun,
      candidateId,
      electionId,
      electionTitle: election.official_ballot_title,
      electionDate: election.election_date,
      staleFutureElectionEventsDeleted: staleEvents.rowCount ?? 0,
      withdrawalEventsCreated: notification.createdCount,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidate-elections:withdraw", process.argv.slice(2), [
    { name: "--candidate-id", value: "space" },
    { name: "--election-id", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const candidateId = requireFlag("--candidate-id");
  const electionId = requireFlag("--election-id");
  const reason = requireFlag("--reason");
  const dryRun = process.argv.includes("--dry-run");

  for (const [name, value] of [
    ["--candidate-id", candidateId],
    ["--election-id", electionId],
  ] as const) {
    if (!UUID_RE.test(value)) throw new Error(`Invalid ${name}: ${value}`);
  }
  if (reason.length < 20) {
    throw new Error("--reason must explain the withdrawal in at least 20 characters (cite the source)");
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();
  try {
    const result = await runWithdrawCandidateElection(client, { candidateId, electionId, dryRun });
    console.log(JSON.stringify({ ...result, reason }, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  void main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
