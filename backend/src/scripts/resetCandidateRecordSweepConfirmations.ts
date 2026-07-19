// Guarded reset for poisoned candidate-record sweep confirmations.
//
// The 2026-07-15/16 bulk runs collapsed the per-question record sweep into a
// generic 4-entry template and confirmed thousands of candidates as
// record-less without a real sweep (routing enforcement shipped afterwards in
// PRs #350/#352). Those confirmation rows and the completion stamps they left
// on candidates are load-bearing: the gap-repair backlog selects UNSTAMPED
// candidates only, so a poisoned candidate is invisible to re-research until
// its stamps clear. This wrapper is the supported repair path (code, not
// direct SQL): it deletes the poisoned confirmations and, for candidates that
// still have zero records, clears last_records_searched_at /
// last_records_researched_through so they rejoin the unstamped backlog.
// Candidates that do have records keep their stamps — deleting the
// confirmation alone makes manual:records:audit resurface them as suspects.
//
// Guard rails, all of which have to pass before a single row changes:
// - explicit confirmed-at date window (the incident days), never "everything";
// - structural cohort guard: only untagged ledgers with exactly
//   COHORT_ENTRY_COUNT evidence entries match the collapsed template — every
//   post-#350 write carries question_id tags and can never match;
// - --expected-total from a prior --dry-run must equal the live resettable
//   count (a mismatch means the database moved — re-run the dry-run);
// - candidates that are retired (deleted/merged) or under an active
//   records-search claim are skipped and reported, never touched;
// - local-database guard, row locks, single transaction, --dry-run.
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { DEFAULT_LEASE_HOURS } from "../pipeline/candidates/candidateRecordsSearchClaim.js";
import { assertIsoDate } from "./correctManualElectionDate.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";

type QueryResultLike<T> = { rows: T[] };

export type SweepConfirmationResetClient = {
  query<T = unknown>(text: string, values?: unknown[]): Promise<QueryResultLike<T>>;
};

export type SweepConfirmationResetOptions = {
  confirmedFrom: string;
  confirmedTo: string;
  /** Required on live runs; validated against the resettable count when set. */
  expectedTotal: number | null;
  dryRun: boolean;
};

// The collapsed 07-15 template always carried exactly 4 entries. A legitimate
// post-#350 never_held ledger also has 4 entries, but every post-#350 write
// is question_id-tagged, so "untagged AND exactly 4 entries" cannot match it.
export const COHORT_ENTRY_COUNT = 4;

const SAMPLE_LIMIT = 10;
const QUESTION_SIGNATURE_LIMIT = 20;
const QUESTION_SNIPPET_LENGTH = 60;

export type SweepEvidenceShape = {
  entryCount: number | null;
  hasQuestionIdTags: boolean;
  /** Per-entry question snippets, for the signature report. */
  questions: string[];
};

// Deliberately independent of the audit's evidence parser: this shape check
// decides what gets DELETED, so it reads the raw entries array (no
// blank-finding filtering) and treats anything unparseable as a mismatch.
export function readSweepEvidenceShape(evidence: unknown): SweepEvidenceShape {
  if (typeof evidence !== "object" || evidence === null || Array.isArray(evidence)) {
    return { entryCount: null, hasQuestionIdTags: false, questions: [] };
  }
  const entries = (evidence as { entries?: unknown }).entries;
  if (!Array.isArray(entries)) {
    return { entryCount: null, hasQuestionIdTags: false, questions: [] };
  }
  let hasQuestionIdTags = false;
  const questions: string[] = [];
  for (const entry of entries) {
    if (typeof entry === "object" && entry !== null && !Array.isArray(entry)) {
      const record = entry as { question?: unknown; question_id?: unknown };
      if (record.question_id !== undefined) {
        hasQuestionIdTags = true;
      }
      questions.push(
        typeof record.question === "string"
          ? record.question.trim().slice(0, QUESTION_SNIPPET_LENGTH)
          : "(no question)"
      );
    } else {
      questions.push("(malformed entry)");
    }
  }
  return { entryCount: entries.length, hasQuestionIdTags, questions };
}

export type SweepConfirmationCohortRow = {
  candidate_id: string;
  display_name: string;
  confirmed_at: string;
  evidence: unknown;
  candidate_retired: boolean;
  active_claim: boolean;
  record_count: number;
};

type CandidateSample = { candidateId: string; displayName: string };

export type SweepConfirmationResetResult = {
  dryRun: boolean;
  confirmedFrom: string;
  confirmedTo: string;
  windowRowCount: number;
  resettable: {
    total: number;
    zeroRecordCount: number;
    withRecordsCount: number;
    zeroRecordSample: CandidateSample[];
    withRecordsSample: CandidateSample[];
  };
  skipped: {
    shapeMismatchCount: number;
    shapeMismatchSample: CandidateSample[];
    retiredCandidateCount: number;
    retiredCandidateSample: CandidateSample[];
    activeClaimCount: number;
    activeClaimSample: CandidateSample[];
  };
  /** Distinct question tuples across resettable rows — verify these are the
   * collapsed template before running live. */
  questionSignatures: { questions: string[]; count: number }[];
  deletedConfirmations: number;
  clearedStamps: number;
};

function sample(rows: SweepConfirmationCohortRow[]): CandidateSample[] {
  return rows
    .slice(0, SAMPLE_LIMIT)
    .map((row) => ({ candidateId: row.candidate_id, displayName: row.display_name }));
}

function listQuestionSignatures(
  rows: SweepConfirmationCohortRow[]
): { questions: string[]; count: number }[] {
  const groups = new Map<string, { questions: string[]; count: number }>();
  for (const row of rows) {
    const shape = readSweepEvidenceShape(row.evidence);
    const key = shape.questions.join(" || ");
    const group = groups.get(key);
    if (group) {
      group.count += 1;
    } else {
      groups.set(key, { questions: shape.questions, count: 1 });
    }
  }
  return [...groups.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, QUESTION_SIGNATURE_LIMIT);
}

export async function runSweepConfirmationReset(
  client: SweepConfirmationResetClient,
  options: SweepConfirmationResetOptions
): Promise<SweepConfirmationResetResult> {
  const { confirmedFrom, confirmedTo, expectedTotal, dryRun } = options;

  // Enforced here, not only in main(): a direct caller must not be able to
  // run live without stating the count a dry-run told it to expect.
  if (!dryRun && expectedTotal === null) {
    throw new Error(
      "--expected-total is required for a live run: run with --dry-run first and pass the reported resettable total back"
    );
  }
  if (confirmedFrom > confirmedTo) {
    throw new Error(
      `--confirmed-from ${confirmedFrom} is after --confirmed-to ${confirmedTo}`
    );
  }

  await client.query("BEGIN");
  try {
    // Lock both the confirmation rows (about to be deleted) and the candidate
    // rows (stamps about to be cleared) so a concurrent claim or manual write
    // serializes against this transaction.
    const cohort = await client.query<SweepConfirmationCohortRow>(
      `
        SELECT
          sc.candidate_id::text AS candidate_id,
          c.display_name,
          sc.confirmed_at::text AS confirmed_at,
          sc.evidence,
          (c.deleted_at IS NOT NULL OR c.merged_into_candidate_id IS NOT NULL) AS candidate_retired,
          (
            c.records_search_claimed_at IS NOT NULL
            AND c.records_search_claimed_at > now() - make_interval(hours => $3::int)
          ) AS active_claim,
          (
            SELECT count(*)::int
            FROM public.candidate_records r
            WHERE r.candidate_id = sc.candidate_id
          ) AS record_count
        FROM public.candidate_record_sweep_confirmations sc
        JOIN public.candidates c ON c.id = sc.candidate_id
        WHERE sc.confirmed_at >= $1::date
          AND sc.confirmed_at < $2::date + 1
        ORDER BY sc.confirmed_at, sc.candidate_id
        FOR UPDATE OF sc, c
      `,
      [confirmedFrom, confirmedTo, DEFAULT_LEASE_HOURS]
    );

    const shapeMismatch: SweepConfirmationCohortRow[] = [];
    const retired: SweepConfirmationCohortRow[] = [];
    const activeClaim: SweepConfirmationCohortRow[] = [];
    const resettable: SweepConfirmationCohortRow[] = [];
    for (const row of cohort.rows) {
      const shape = readSweepEvidenceShape(row.evidence);
      if (shape.entryCount !== COHORT_ENTRY_COUNT || shape.hasQuestionIdTags) {
        shapeMismatch.push(row);
      } else if (row.candidate_retired) {
        retired.push(row);
      } else if (row.active_claim) {
        activeClaim.push(row);
      } else {
        resettable.push(row);
      }
    }

    if (expectedTotal !== null && expectedTotal !== resettable.length) {
      throw new Error(
        `--expected-total ${expectedTotal} does not match the live resettable count ${resettable.length}; ` +
          "the database moved since the dry-run — re-run with --dry-run and verify the new report before resetting"
      );
    }

    const zeroRecord = resettable.filter((row) => row.record_count === 0);
    const withRecords = resettable.filter((row) => row.record_count > 0);

    let deletedConfirmations = 0;
    let clearedStamps = 0;
    if (dryRun || resettable.length === 0) {
      await client.query("ROLLBACK");
    } else {
      const deleted = await client.query<{ candidate_id: string }>(
        `
          DELETE FROM public.candidate_record_sweep_confirmations
          WHERE candidate_id = ANY($1::uuid[])
          RETURNING candidate_id::text AS candidate_id
        `,
        [resettable.map((row) => row.candidate_id)]
      );
      deletedConfirmations = deleted.rows.length;
      if (deletedConfirmations !== resettable.length) {
        throw new Error(
          `Deleted ${deletedConfirmations} confirmations but expected ${resettable.length}; rolled back`
        );
      }
      if (zeroRecord.length > 0) {
        const cleared = await client.query<{ id: string }>(
          `
            UPDATE public.candidates
            SET last_records_searched_at = NULL,
                last_records_researched_through = NULL,
                updated_at = now()
            WHERE id = ANY($1::uuid[])
            RETURNING id::text AS id
          `,
          [zeroRecord.map((row) => row.candidate_id)]
        );
        clearedStamps = cleared.rows.length;
        if (clearedStamps !== zeroRecord.length) {
          throw new Error(
            `Cleared stamps on ${clearedStamps} candidates but expected ${zeroRecord.length}; rolled back`
          );
        }
      }
      await client.query("COMMIT");
    }

    return {
      dryRun,
      confirmedFrom,
      confirmedTo,
      windowRowCount: cohort.rows.length,
      resettable: {
        total: resettable.length,
        zeroRecordCount: zeroRecord.length,
        withRecordsCount: withRecords.length,
        zeroRecordSample: sample(zeroRecord),
        withRecordsSample: sample(withRecords),
      },
      skipped: {
        shapeMismatchCount: shapeMismatch.length,
        shapeMismatchSample: sample(shapeMismatch),
        retiredCandidateCount: retired.length,
        retiredCandidateSample: sample(retired),
        activeClaimCount: activeClaim.length,
        activeClaimSample: sample(activeClaim),
      },
      questionSignatures: listQuestionSignatures(resettable),
      deletedConfirmations,
      clearedStamps,
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

function usage(): string {
  return [
    "Reset poisoned candidate-record sweep confirmations (2026-07-15 incident repair).",
    "Deletes untagged 4-entry template confirmations in the window; candidates left",
    "with zero records also get last_records_searched_at /",
    "last_records_researched_through cleared so they rejoin the unstamped backlog.",
    "",
    "Usage:",
    "  npm run manual:records:reset-confirmations -- --confirmed-from YYYY-MM-DD --confirmed-to YYYY-MM-DD --reason text --dry-run",
    "  npm run manual:records:reset-confirmations -- --confirmed-from YYYY-MM-DD --confirmed-to YYYY-MM-DD --expected-total N --reason text",
    "",
    "Always dry-run first: verify the questionSignatures are the collapsed",
    "template and pass the reported resettable.total as --expected-total.",
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
  if (!value) throw new Error(`${name} is required for sweep-confirmation reset`);
  return value;
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:records:reset-confirmations", process.argv.slice(2), [
    { name: "--confirmed-from", value: "space" },
    { name: "--confirmed-to", value: "space" },
    { name: "--expected-total", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const confirmedFrom = requireFlag("--confirmed-from");
  const confirmedTo = requireFlag("--confirmed-to");
  const reason = requireFlag("--reason");
  const expectedTotalRaw = readFlag("--expected-total");
  const dryRun = process.argv.includes("--dry-run");

  assertIsoDate("--confirmed-from", confirmedFrom);
  assertIsoDate("--confirmed-to", confirmedTo);
  if (reason.length < 20) {
    throw new Error("--reason must explain the reset in at least 20 characters");
  }
  let expectedTotal: number | null = null;
  if (expectedTotalRaw !== null) {
    expectedTotal = Number(expectedTotalRaw);
    if (!Number.isInteger(expectedTotal) || expectedTotal < 0) {
      throw new Error(`--expected-total must be a non-negative integer; received ${expectedTotalRaw}`);
    }
  }

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });
  const client = await pool.connect();

  try {
    const result = await runSweepConfirmationReset(client, {
      confirmedFrom,
      confirmedTo,
      expectedTotal,
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
