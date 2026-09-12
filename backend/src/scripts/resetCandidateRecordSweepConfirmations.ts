// Guarded reset for poisoned candidate-record sweep confirmations.
//
// The 2026-07-15/16 bulk runs collapsed the per-question record sweep into a
// generic 4-entry template and confirmed thousands of candidates as
// record-less without a real sweep (routing enforcement shipped afterwards in
// PRs #350/#352). Those confirmation rows and the completion stamps they left
// on candidates are load-bearing: the gap-repair backlog selects UNSTAMPED
// candidates only, so a poisoned candidate is invisible to re-research until
// its stamps clear. This wrapper is the supported repair path (code, not
// direct SQL): it deletes the poisoned confirmations and clears
// last_records_searched_at / last_records_researched_through on every reset
// candidate so all of them rejoin the unstamped backlog for a real sweep.
// Candidates with existing records get the same treatment — their stamps were
// written by the same collapsed run, and with the confirmation deleted no
// audit surface would ever flag them again (the suspect list only covers
// zero-record candidates, the detectors only read persisted confirmations),
// so keeping the stamp would hide them from repair permanently. Their records
// are never touched; the re-sweep is additive.
//
// A second cohort (--cohort august-21-template) covers the 2026-08-21 bulk
// run: question_id-tagged 7-entry ledgers whose findings were the same fixed
// sentences on every candidate. A spot-check of 10 found real dated actions
// the ledgers denied. Only ledgers where EVERY finding is one of those
// sentences match; any candidate-specific finding keeps the ledger.
//
// A third cohort (--cohort records-retired-out) covers candidates whose
// latest sweep stored only weak rows (directory listings, bios, primary
// results) that later cleanups retired. They sit stamped with zero records
// and an only_general_labels or empty-claim ledger, so the queue skips them;
// a spot-check found real votes those sweeps missed. Ledgers claiming
// no_records_found are never in this cohort.
//
// Guard rails, all of which have to pass before a single row changes:
// - explicit confirmed-at date window (the incident days), never "everything";
// - structural cohort guard: only untagged ledgers with exactly
//   COHORT_ENTRY_COUNT evidence entries match the collapsed template — every
//   post-#350 write carries question_id tags and can never match (or, for
//   the august-21-template cohort, every finding is a fixed template sentence);
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
  cohort: SweepResetCohort;
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

export type SweepResetCohort = "july-15-untagged" | "august-21-template" | "records-retired-out";
export const SWEEP_RESET_COHORTS: readonly SweepResetCohort[] = [
  "july-15-untagged",
  "august-21-template",
  "records-retired-out",
];

// Every finding the 2026-08-21 bulk run wrote, verbatim.
export const AUGUST_21_TEMPLATE_FINDINGS: ReadonlySet<string> = new Set([
  "The stored office or service record was compared; no additional dated substantive roll-call action found.",
  "No additional dated legislation sponsorship found.",
  "No additional dated executive action found.",
  "No dated court, ethics, disciplinary, or campaign-finance proceeding found.",
  "The stored service record was compared; no additional dated leadership action found.",
  "No additional dated public action found.",
  "No dated endorsement record found.",
]);

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

// True only when the ledger has entries and every one of them carries a
// finding from AUGUST_21_TEMPLATE_FINDINGS. Anything unparseable is false.
export function isAugust21TemplateLedger(evidence: unknown): boolean {
  if (typeof evidence !== "object" || evidence === null || Array.isArray(evidence)) {
    return false;
  }
  const entries = (evidence as { entries?: unknown }).entries;
  if (!Array.isArray(entries) || entries.length === 0) {
    return false;
  }
  return entries.every((entry) => {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      return false;
    }
    const finding = (entry as { finding?: unknown }).finding;
    return typeof finding === "string" && AUGUST_21_TEMPLATE_FINDINGS.has(finding.trim());
  });
}

export function matchesResetCohort(evidence: unknown, cohort: SweepResetCohort): boolean {
  if (cohort === "august-21-template") {
    return isAugust21TemplateLedger(evidence);
  }
  const shape = readSweepEvidenceShape(evidence);
  return shape.entryCount === COHORT_ENTRY_COUNT && !shape.hasQuestionIdTags;
}

export type SweepConfirmationCohortRow = {
  candidate_id: string;
  context_type: "election" | "presidential_cycle";
  context_id: string;
  display_name: string;
  confirmed_at: string;
  evidence: unknown;
  candidate_retired: boolean;
  active_claim: boolean;
  record_count: number;
  retired_record_count: number;
  /** confirmed_at >= the candidate's last_records_searched_at. */
  covers_latest_search: boolean;
  confirmed_gap_ids: string[];
};

// records-retired-out: the ledger backs the candidate's latest search, every
// record that search stored was later retired, and the ledger makes no
// no_records_found claim (those are evidenced confirmed nulls, kept).
export function isRecordsRetiredOutLedger(row: SweepConfirmationCohortRow): boolean {
  return (
    row.record_count === 0 &&
    row.retired_record_count > 0 &&
    row.covers_latest_search &&
    !row.confirmed_gap_ids.includes("candidate_records.no_records_found")
  );
}

type CandidateSample = { candidateId: string; displayName: string };

export type SweepConfirmationResetResult = {
  cohort: SweepResetCohort;
  dryRun: boolean;
  confirmedFrom: string;
  confirmedTo: string;
  windowRowCount: number;
  resettable: {
    total: number;
    // Reporting split only — every resettable candidate gets its
    // confirmation deleted AND its stamps cleared. The split shows how many
    // rejoin the backlog empty vs carrying records the re-sweep will extend.
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
  const { cohort: resetCohort, confirmedFrom, confirmedTo, expectedTotal, dryRun } = options;

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
          sc.context_type,
          sc.context_id::text AS context_id,
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
              AND r.retired_at IS NULL
          ) AS record_count,
          (
            SELECT count(*)::int
            FROM public.candidate_records r
            WHERE r.candidate_id = sc.candidate_id
              AND r.retired_at IS NOT NULL
          ) AS retired_record_count,
          coalesce(sc.confirmed_at >= c.last_records_searched_at, false) AS covers_latest_search,
          sc.confirmed_gap_ids
        FROM public.candidate_record_sweep_confirmations sc
        JOIN public.candidates c ON c.id = sc.candidate_id
        WHERE sc.confirmed_at >= $1::date
          AND sc.confirmed_at < $2::date + 1
        ORDER BY sc.confirmed_at, sc.candidate_id, sc.context_type, sc.context_id
        FOR UPDATE OF sc, c
      `,
      [confirmedFrom, confirmedTo, DEFAULT_LEASE_HOURS]
    );

    const shapeMismatch: SweepConfirmationCohortRow[] = [];
    const retired: SweepConfirmationCohortRow[] = [];
    const activeClaim: SweepConfirmationCohortRow[] = [];
    const resettable: SweepConfirmationCohortRow[] = [];
    for (const row of cohort.rows) {
      const matches =
        resetCohort === "records-retired-out"
          ? isRecordsRetiredOutLedger(row)
          : matchesResetCohort(row.evidence, resetCohort);
      if (!matches) {
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
      // Delete only the exact candidate/context rows locked and classified
      // above. Other valid context ledgers for the same candidate survive.
      const deleted = await client.query<{ candidate_id: string }>(
        `
          DELETE FROM public.candidate_record_sweep_confirmations sc
          USING unnest($1::uuid[], $2::text[], $3::uuid[])
            AS target(candidate_id, context_type, context_id)
          WHERE sc.candidate_id = target.candidate_id
            AND sc.context_type = target.context_type
            AND sc.context_id = target.context_id
          RETURNING sc.candidate_id::text AS candidate_id
        `,
        [
          resettable.map((row) => row.candidate_id),
          resettable.map((row) => row.context_type),
          resettable.map((row) => row.context_id),
        ]
      );
      deletedConfirmations = deleted.rows.length;
      if (deletedConfirmations !== resettable.length) {
        throw new Error(
          `Deleted ${deletedConfirmations} confirmations but expected ${resettable.length}; rolled back`
        );
      }
      // Stamps clear for EVERY reset candidate, including those with
      // records: the cohort's stamps all came from the collapsed run, and a
      // stamped candidate with records and no confirmation is invisible to
      // the suspect list, the detectors, and the unstamped backlog alike —
      // keeping the stamp would end their repair here.
      const resetCandidateIds = [...new Set(resettable.map((row) => row.candidate_id))];
      const cleared = await client.query<{ id: string }>(
        `
          UPDATE public.candidates
          SET last_records_searched_at = NULL,
              last_records_researched_through = NULL,
              updated_at = now()
          WHERE id = ANY($1::uuid[])
          RETURNING id::text AS id
        `,
        [resetCandidateIds]
      );
      clearedStamps = cleared.rows.length;
      if (clearedStamps !== resetCandidateIds.length) {
        throw new Error(
          `Cleared stamps on ${clearedStamps} candidates but expected ${resetCandidateIds.length}; rolled back`
        );
      }
      await client.query("COMMIT");
    }

    return {
      cohort: resetCohort,
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
    "Deletes untagged 4-entry template confirmations in the window and clears",
    "last_records_searched_at / last_records_researched_through on every reset",
    "candidate (records-holding ones too — their stamps came from the same collapsed",
    "run) so all of them rejoin the unstamped backlog for a real sweep.",
    "",
    "--cohort july-15-untagged (default), august-21-template (every finding",
    "is one of the fixed 2026-08-21 bulk-run sentences), or records-retired-out",
    "(latest sweep's records all retired since; no no_records_found claim).",
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
    { name: "--cohort", value: "space" },
    { name: "--confirmed-from", value: "space" },
    { name: "--confirmed-to", value: "space" },
    { name: "--expected-total", value: "space" },
    { name: "--reason", value: "space" },
    { name: "--dry-run", value: "none" },
  ]);
  loadProjectEnv();

  const cohortRaw = readFlag("--cohort") ?? "july-15-untagged";
  if (!SWEEP_RESET_COHORTS.includes(cohortRaw as SweepResetCohort)) {
    throw new Error(`--cohort must be one of ${SWEEP_RESET_COHORTS.join(", ")}; received ${cohortRaw}`);
  }
  const cohort = cohortRaw as SweepResetCohort;
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
      cohort,
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
