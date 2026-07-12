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
  | {
      alreadyCorrected: true;
      electionId: string;
      correctedDate: string;
      // The already-corrected path still converges provenance: when the stored
      // row is missing the official source (corrected out-of-band), a live
      // re-run appends it. dryRun=true reports what would be appended.
      sourceAppended: boolean;
      dryRun: boolean;
    }
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
// Entries are trimmed before the Set so a whitespace variant of an existing
// URL dedupes instead of surviving as a second entry (defensive only — the
// election payload contract trims on parse and no live row carries
// untrimmed sources).
export function appendElectionSource(sources: unknown, sourceUrl: string): string[] {
  const existing = Array.isArray(sources)
    ? sources
        .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
        .map((value) => value.trim())
    : [];
  return [...new Set([...existing, sourceUrl.trim()])];
}

export async function runElectionDateCorrection(
  client: ElectionDateCorrectionClient,
  options: ElectionDateCorrectionOptions
): Promise<ElectionDateCorrectionResult> {
  const { electionId, expectedDate, correctedDate, sourceUrl, dryRun } = options;

  // Enforced here, not only in main(): a direct caller (test, future script)
  // must not be able to store non-HTTPS provenance.
  if (new URL(sourceUrl).protocol !== "https:") {
    throw new Error("--source-url must use HTTPS");
  }
  // State campaign-finance links denormalize election_year from
  // election_date at link time and every summary/breakdown join keys on that
  // year, so a correction that crosses a calendar year strands finance data
  // on the old year. That repair needs coordinated finance updates this
  // wrapper deliberately does not attempt.
  if (expectedDate.slice(0, 4) !== correctedDate.slice(0, 4)) {
    throw new Error(
      `Cross-calendar-year correction (${expectedDate} -> ${correctedDate}) is not supported: ` +
        "campaign-finance links key summaries by election_year and would be stranded on the old year; " +
        "this needs a coordinated finance repair, not a date-only update"
    );
  }

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
      // Idempotent path, but still converge provenance: a row whose date was
      // already corrected without the official source (out-of-band repair)
      // gets the source appended so re-running the exact command always ends
      // in the same final state.
      // Same trim-normalized comparison appendElectionSource uses, so the
      // sourceAppended flag never reports an append that the merge would
      // dedupe away (or vice versa).
      const merged = appendElectionSource(row.sources, sourceUrl);
      const trimmedSourceUrl = sourceUrl.trim();
      const sourceMissing =
        !Array.isArray(row.sources) ||
        !row.sources.some(
          (value) => typeof value === "string" && value.trim() === trimmedSourceUrl
        );
      if (sourceMissing && !dryRun) {
        await client.query(
          `
            UPDATE public.elections
            SET sources = $2::jsonb,
                updated_at = now()
            WHERE id = $1::uuid
          `,
          [electionId, JSON.stringify(merged)]
        );
        await client.query("COMMIT");
      } else {
        await client.query("ROLLBACK");
      }
      return {
        alreadyCorrected: true,
        electionId,
        correctedDate,
        sourceAppended: sourceMissing,
        dryRun,
      };
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

    // Result rows were collected for the stored (wrong) date. Rewriting the
    // date under them would silently present wrong-date results as this
    // contest's outcome — that cleanup is a user decision, not a side effect.
    const persistedResults = await client.query<{ source: string }>(
      `
        SELECT 'election_results' AS source
        FROM public.election_results WHERE election_id = $1::uuid
        UNION ALL
        SELECT 'ballot_measure_results' AS source
        FROM public.ballot_measure_results WHERE election_id = $1::uuid
        LIMIT 1
      `,
      [electionId]
    );
    if (persistedResults.rows[0]) {
      throw new Error(
        `Election ${electionId} has persisted ${persistedResults.rows[0].source} rows collected under the stored date; ` +
          "refusing date correction — resolve the result rows first (user decision), then re-run"
      );
    }

    const sources = appendElectionSource(row.sources, sourceUrl);
    if (dryRun) {
      await client.query("ROLLBACK");
    } else {
      // Reset result-polling state alongside the date: markers set while the
      // row carried the wrong date (checked stamps, exhausted attempt counts)
      // would make the scheduler skip the election on its corrected date. On
      // the normal never-polled row these are already NULL/0 and the reset is
      // a no-op.
      //
      // Redis-side scheduling state is deliberately NOT touched here. Every
      // completed polling pass persists an election_results row (the writer
      // upserts not_found/not_final_yet rows too), so the persisted-results
      // refusal above already blocks correction on any election that has
      // been polled. The residual Redis state is a producer emit-marker set
      // in the emit-to-write race window — it self-expires (7-day TTL, see
      // electionResultPassMarkers.ts) and the certified pass clears it for
      // unchecked elections — and a queued group job, whose enricher
      // re-reads election_date from the database at execution time.
      await client.query(
        `
          UPDATE public.elections
          SET election_date = $2::date,
              sources = $3::jsonb,
              election_night_results_checked_at = NULL,
              election_night_results_attempt_count = 0,
              election_night_results_last_attempted_at = NULL,
              certified_results_checked_at = NULL,
              certified_results_attempt_count = 0,
              certified_results_last_attempted_at = NULL,
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
