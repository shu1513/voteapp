/**
 * Sweep-evidence guard for manual candidate-record writes.
 *
 * A zero-record or neutral-only manual records pass asserts that the
 * per-question discovery sweep was actually finished. That assertion used to
 * be honor-system (a bare CLI flag), which let an unfinished sweep write a
 * false `no_records_found` gap and stamp `last_records_searched_at` — after
 * which the candidate is never re-searched. This module makes the assertion
 * carry its evidence: the operator must supply the per-question evidence
 * table they were already required to keep, and the writer refuses the
 * completeness claim without it.
 *
 * The guard deliberately validates only shape, not truth: the smallest
 * complete question list (judicial candidates) has three questions, so three
 * entries is the floor. Era coverage and officeholder-vs-challenger question
 * counts are research-derived facts the writer cannot verify from the
 * database, so no attempt is made to check them here.
 *
 * Validated confirmations are also persisted (candidate_record_sweep_confirmations,
 * one row per candidate, newest sweep wins) so manual:records:audit can tell
 * an evidence-backed confirmed-null candidate apart from a skipped sweep.
 */

import type { PoolClient } from "pg";

export const SWEEP_EVIDENCE_MIN_ENTRIES = 3;

export const SWEEP_COMPLETENESS_GAP_IDS: ReadonlySet<string> = new Set([
  "candidate_records.no_records_found",
  "candidate_records.only_general_labels",
]);

export type SweepEvidenceEntry = {
  question: string;
  finding: string;
};

export type SweepEvidenceParseResult =
  | { ok: true; entries: SweepEvidenceEntry[] }
  | { ok: false; reason: string };

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

/**
 * A completeness assertion is being made when the verified record set is
 * empty (which stamps search completion even without any flag) or when the
 * operator passes a sweep-completeness confirmed-gap id.
 */
export function sweepEvidenceRequired(input: {
  recordCount: number;
  confirmedGapIds: ReadonlySet<string>;
}): boolean {
  if (input.recordCount === 0) {
    return true;
  }
  for (const id of input.confirmedGapIds) {
    if (SWEEP_COMPLETENESS_GAP_IDS.has(id)) {
      return true;
    }
  }
  return false;
}

export function parseSweepEvidencePayload(payload: unknown): SweepEvidenceParseResult {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "evidence payload must be an object with an entries array" };
  }
  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.entries)) {
    return { ok: false, reason: "evidence payload.entries must be an array" };
  }
  const entries: SweepEvidenceEntry[] = [];
  const seenQuestions = new Map<string, number>();
  for (const [index, row] of input.entries.entries()) {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      return { ok: false, reason: `evidence entries[${index}] must be an object` };
    }
    const entry = row as Record<string, unknown>;
    if (!isNonEmptyString(entry.question)) {
      return { ok: false, reason: `evidence entries[${index}].question must be a non-empty string` };
    }
    if (!isNonEmptyString(entry.finding)) {
      return {
        ok: false,
        reason: `evidence entries[${index}].finding must be a non-empty string (use "nothing found" for empty answers)`,
      };
    }
    const question = entry.question.trim();
    const normalizedQuestion = question.toLowerCase().replace(/\s+/g, " ");
    const duplicateOf = seenQuestions.get(normalizedQuestion);
    if (duplicateOf !== undefined) {
      return {
        ok: false,
        reason: `evidence entries[${index}].question duplicates entries[${duplicateOf}] — each row must be a distinct discovery question (asking the same question for a different era/session? name the era in the question text)`,
      };
    }
    seenQuestions.set(normalizedQuestion, index);
    entries.push({ question, finding: entry.finding.trim() });
  }
  if (entries.length < SWEEP_EVIDENCE_MIN_ENTRIES) {
    return {
      ok: false,
      reason: `evidence payload.entries needs at least ${SWEEP_EVIDENCE_MIN_ENTRIES} question/finding rows (one per discovery question actually asked); got ${entries.length}`,
    };
  }
  return { ok: true, entries };
}

/**
 * The completeness claims a passing write actually asserts: zero verified
 * records implies no_records_found even without any flag, and the
 * only_general_labels claim can arrive as either an operator flag or a
 * detected quality gap. Non-completeness gap ids are ignored.
 */
export function assertedSweepCompletenessGapIds(input: {
  recordCount: number;
  confirmedGapIds: ReadonlySet<string>;
  qualityGapIds: readonly string[];
}): string[] {
  const asserted = new Set<string>();
  if (input.recordCount === 0) {
    asserted.add("candidate_records.no_records_found");
  }
  for (const id of [...input.confirmedGapIds, ...input.qualityGapIds]) {
    if (SWEEP_COMPLETENESS_GAP_IDS.has(id)) {
      asserted.add(id);
    }
  }
  return [...asserted].sort();
}

/**
 * Persist the validated confirmation inside the writer's transaction. One
 * row per candidate: a newer sweep supersedes the older confirmation.
 */
export async function upsertSweepConfirmation(
  client: Pick<PoolClient, "query">,
  input: {
    candidateId: string;
    confirmedGapIds: readonly string[];
    entries: readonly SweepEvidenceEntry[];
    contextType: "election" | "presidential_cycle";
    contextId: string;
  }
): Promise<void> {
  await client.query(
    `
      INSERT INTO public.candidate_record_sweep_confirmations
        (candidate_id, confirmed_gap_ids, evidence, context_type, context_id)
      VALUES ($1, $2::text[], $3::jsonb, $4, $5)
      ON CONFLICT (candidate_id)
      DO UPDATE SET
        confirmed_gap_ids = EXCLUDED.confirmed_gap_ids,
        evidence = EXCLUDED.evidence,
        context_type = EXCLUDED.context_type,
        context_id = EXCLUDED.context_id,
        confirmed_at = now(),
        updated_at = now()
    `,
    [
      input.candidateId,
      [...input.confirmedGapIds],
      JSON.stringify({ entries: input.entries }),
      input.contextType,
      input.contextId,
    ]
  );
}

/**
 * A live write that did NOT require evidence found real, stance-labeled
 * records — any earlier completeness confirmation no longer describes the
 * candidate's state, so remove it inside the same transaction. Without this,
 * the table would keep asserting no_records_found for a candidate who now
 * has records (the current audit gates on zero records so it stays correct,
 * but any future reader of this table would be misled).
 */
export async function deleteSweepConfirmation(
  client: Pick<PoolClient, "query">,
  candidateId: string
): Promise<void> {
  await client.query(
    `DELETE FROM public.candidate_record_sweep_confirmations WHERE candidate_id = $1`,
    [candidateId]
  );
}

export function sweepEvidenceMissingError(context: string): Error {
  return new Error(
    [
      `A zero-record or neutral-only ${context} pass asserts a FINISHED discovery sweep, so it requires --evidence-file evidence.json.`,
      `The file must contain {"entries": [{"question": "...", "finding": "..."}, ...]} — one row per discovery question actually asked (minimum ${SWEEP_EVIDENCE_MIN_ENTRIES}).`,
      "If the question list has not been finished, finish it (or run the remaining questions) instead of asserting completeness.",
    ].join("\n")
  );
}
