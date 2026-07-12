// Guarded canonical election-date correction.
//
// The election upsert identity is (district_id, official_ballot_title_key,
// election_date), so a date correction can never flow through the normal
// injector: reinjecting the corrected payload inserts a SEPARATE election
// shell and strands the original row's candidate links (live: an Arizona CD9
// primary stored 2026-08-04 while the SOS portal said 2026-07-21). This
// wrapper is the repair path the injector cannot be: it updates the date in
// place on one exactly-identified election, preserving the election ID and
// every downstream candidate_elections / staging link keyed to it.
//
// Guard rails, all of which have to pass before a single row changes:
// - exact election UUID plus the expected current date (a mismatch means the
//   caller is looking at a stale copy of the row — refuse);
// - the corrected date must not collide with another election sharing the
//   identity key (that situation needs a merge, which this wrapper
//   deliberately does not attempt);
// - an official HTTPS source documenting the corrected date is appended to
//   the election's sources;
// - local-database guard, row lock, single transaction, and --dry-run.
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

type QueryResultLike<T> = { rows: T[] };

export type ElectionDateCorrectionClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type ElectionDateCorrectionOptions = {
  electionId: string;
  expectedDate: string;
  correctedDate: string;
  sourceUrl: string;
  dryRun: boolean;
};

export type ElectionDateCorrectionResult =
  | { alreadyCorrected: true; electionId: string; correctedDate: string }
  | {
      alreadyCorrected: false;
      dryRun: boolean;
      electionId: string;
      districtId: string;
      officialBallotTitle: string;
      expectedDate: string;
      correctedDate: string;
      sources: string[];
    };

type ElectionRow = {
  id: string;
  district_id: string;
  official_ballot_title: string;
  official_ballot_title_key: string;
  election_date: string;
  sources: unknown;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function usage(): string {
  return [
    "Correct one canonical election date without changing the election ID or downstream links.",
    "",
    "Usage:",
    "  npm run manual:election-date:correct -- --election-id uuid --expected-date YYYY-MM-DD --corrected-date YYYY-MM-DD --source-url https://... --reason text [--dry-run]",
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
  if (!value) throw new Error(`${name} is required for manual election-date correction`);
  return value;
}

export function assertIsoDate(name: string, value: string): void {
  if (
    !ISO_DATE_RE.test(value) ||
    new Date(`${value}T00:00:00.000Z`).toISOString().slice(0, 10) !== value
  ) {
    throw new Error(`${name} must be a valid YYYY-MM-DD date; received ${value}`);
  }
}

// elections.sources is a jsonb array of URL strings everywhere the pipeline
// writes it; anything else in an existing row is dropped rather than
// round-tripped so a malformed row cannot smuggle non-string entries forward.
export function appendElectionSource(sources: unknown, sourceUrl: string): string[] {
  const existing = Array.isArray(sources)
    ? sources.filter((value): value is string => typeof value === "string" && value.trim().length > 0)
    : [];
  return [...new Set([...existing, sourceUrl])];
}

export async function runElectionDateCorrection(
  client: ElectionDateCorrectionClient,
  options: ElectionDateCorrectionOptions
): Promise<ElectionDateCorrectionResult> {
  const { electionId, expectedDate, correctedDate, sourceUrl, dryRun } = options;

  await client.query("BEGIN");
  try {
    const locked = await client.query<ElectionRow>(
      `
        SELECT id, district_id, official_ballot_title, official_ballot_title_key,
               election_date::text, sources
        FROM public.elections
        WHERE id = $1::uuid
        FOR UPDATE
      `,
      [electionId]
    );
    const row = locked.rows[0];
    if (!row) throw new Error(`Election not found: ${electionId}`);

    if (row.election_date === correctedDate) {
      await client.query("ROLLBACK");
      return { alreadyCorrected: true, electionId, correctedDate };
    }
    if (row.election_date !== expectedDate) {
      throw new Error(
        `Expected election ${electionId} date ${expectedDate}, found ${row.election_date}; refusing correction`
      );
    }

    const conflict = await client.query<{ id: string }>(
      `
        SELECT id
        FROM public.elections
        WHERE district_id = $1::uuid
          AND official_ballot_title_key = $2
          AND election_date = $3::date
          AND id <> $4::uuid
        LIMIT 1
      `,
      [row.district_id, row.official_ballot_title_key, correctedDate, electionId]
    );
    if (conflict.rows[0]) {
      throw new Error(
        `Correction would collide with election ${conflict.rows[0].id}; merge required`
      );
    }

    const sources = appendElectionSource(row.sources, sourceUrl);
    if (dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query(
        `
          UPDATE public.elections
          SET election_date = $2::date,
              sources = $3::jsonb,
              updated_at = now()
          WHERE id = $1::uuid
        `,
        [electionId, correctedDate, JSON.stringify(sources)]
      );
      await client.query("COMMIT");
    }

    return {
      alreadyCorrected: false,
      dryRun,
      electionId,
      districtId: row.district_id,
      officialBallotTitle: row.official_ballot_title,
      expectedDate,
      correctedDate,
      sources,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:election-date:correct", process.argv.slice(2), [
    { name: "--election-id", value: "space" },
    { name: "--expected-date", value: "space" },
    { name: "--corrected-date", value: "space" },
    { name: "--source-url", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const electionId = requireFlag("--election-id");
  const expectedDate = requireFlag("--expected-date");
  const correctedDate = requireFlag("--corrected-date");
  const sourceUrl = requireFlag("--source-url");
  const reason = requireFlag("--reason");
  const dryRun = process.argv.includes("--dry-run");

  if (!UUID_RE.test(electionId)) throw new Error(`Invalid --election-id: ${electionId}`);
  assertIsoDate("--expected-date", expectedDate);
  assertIsoDate("--corrected-date", correctedDate);
  if (expectedDate === correctedDate) throw new Error("Expected and corrected dates must differ");
  const parsedSource = new URL(sourceUrl);
  if (parsedSource.protocol !== "https:") throw new Error("--source-url must use HTTPS");
  if (reason.length < 20) {
    throw new Error("--reason must explain the correction in at least 20 characters");
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();

  try {
    const result = await runElectionDateCorrection(client, {
      electionId,
      expectedDate,
      correctedDate,
      sourceUrl,
      dryRun,
    });
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
