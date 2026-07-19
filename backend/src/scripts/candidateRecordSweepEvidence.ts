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
 * entries is the floor. Truth of the findings stays unverifiable, but the
 * ROUTING of the sweep no longer relies on skill discipline: the 2026-07-15
 * bulk runs collapsed every candidate onto a generic officeholder-framed
 * template (first-time candidates were never asked the career question) and
 * their 4-entry ledgers passed this guard. Full-history completeness claims
 * now require each entry to be tagged with a question_id and the tagged set
 * to cover the candidate's route (officeholder / never-held / judicial) —
 * see resolveSweepRoute and listMissingSweepRouteQuestionIds. Era coverage
 * remains research-derived and unchecked.
 *
 * Validated confirmations are also persisted (candidate_record_sweep_confirmations,
 * one row per candidate, newest sweep wins) so manual:records:audit can tell
 * an evidence-backed confirmed-null candidate apart from a skipped sweep.
 * A stance-bearing FULL-history sweep that supplies its ledger persists too,
 * with an empty claim set (confirmed_gap_ids = '{}'): "sweep ran with
 * evidence; stance-labeled records found; no completeness claims". Delta
 * (windowed) writes never persist their window ledger — window evidence
 * cannot back a full-history claim.
 */

import type { PoolClient } from "pg";

export const SWEEP_EVIDENCE_MIN_ENTRIES = 3;

export const SWEEP_COMPLETENESS_GAP_IDS: ReadonlySet<string> = new Set([
  "candidate_records.no_records_found",
  "candidate_records.only_general_labels",
]);

/**
 * Discovery routes and their canonical question ids, mirroring the three
 * question lists in the manual-research skill (references/records.md) and the
 * AI discovery prompt. A route's every id must appear on at least one tagged
 * evidence entry before a full-history completeness claim is accepted; a
 * question that cannot apply (e.g. `executive` for a legislator who never
 * held an executive role) still gets its one-line entry — that IS the answer.
 * Era-split sweeps tag multiple entries with the same id.
 */
export const SWEEP_ROUTE_QUESTION_IDS = {
  officeholder: [
    "rollcalls",
    "sponsorship",
    "executive",
    "proceedings",
    "leadership",
    "outside_chamber",
    "endorsements",
  ],
  never_held_office: ["career", "orgs_advocacy", "court_legal", "endorsements"],
  judicial: ["cases", "discipline", "endorsements"],
} as const satisfies Record<string, readonly string[]>;

export type SweepRoute = keyof typeof SWEEP_ROUTE_QUESTION_IDS;

const ALL_SWEEP_QUESTION_IDS: ReadonlySet<string> = new Set(
  Object.values(SWEEP_ROUTE_QUESTION_IDS).flat()
);

export type SweepEvidenceEntry = {
  question: string;
  finding: string;
  /**
   * Canonical discovery-question id this entry answers (null for extra
   * entries outside the route's list: archive scans, office-area follow-ups).
   */
  questionId: string | null;
};

export type SweepEvidenceParseResult =
  | {
      ok: true;
      entries: SweepEvidenceEntry[];
      /**
       * Top-level `has_held_public_office` from the evidence file: the
       * operator's research-derived routing answer, used (and persisted)
       * only when candidates.has_held_public_office is still NULL.
       */
      hasHeldPublicOffice: boolean | null;
    }
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
  if (
    input.has_held_public_office !== undefined &&
    typeof input.has_held_public_office !== "boolean"
  ) {
    return {
      ok: false,
      reason: "evidence payload.has_held_public_office must be a boolean when present",
    };
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
    let questionId: string | null = null;
    if (entry.question_id !== undefined && entry.question_id !== null) {
      if (typeof entry.question_id !== "string" || !ALL_SWEEP_QUESTION_IDS.has(entry.question_id)) {
        return {
          ok: false,
          reason: `evidence entries[${index}].question_id must be one of: ${[...ALL_SWEEP_QUESTION_IDS].sort().join(", ")}; got ${JSON.stringify(entry.question_id)}. Omit question_id on extra entries (archive scans, office-area follow-ups).`,
        };
      }
      questionId = entry.question_id;
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
    entries.push({ question, finding: entry.finding.trim(), questionId });
  }
  if (entries.length < SWEEP_EVIDENCE_MIN_ENTRIES) {
    return {
      ok: false,
      reason: `evidence payload.entries needs at least ${SWEEP_EVIDENCE_MIN_ENTRIES} question/finding rows (one per discovery question actually asked); got ${entries.length}`,
    };
  }
  return {
    ok: true,
    entries,
    hasHeldPublicOffice: (input.has_held_public_office as boolean | undefined) ?? null,
  };
}

export type SweepRouteResolution =
  | {
      ok: true;
      route: SweepRoute;
      /**
       * Non-null when candidates.has_held_public_office is NULL and the
       * evidence file supplied the answer: the writer persists it inside the
       * write transaction so the next sweep routes from the database.
       */
      persistHasHeldPublicOffice: boolean | null;
    }
  | { ok: false; reason: string };

/**
 * Derive which question list a full-history completeness claim must cover.
 * Judicial contests route on the election's discovery_contest_family alone;
 * everything else routes on has-EVER-held-public-office — the database
 * column when set, else the evidence file's has_held_public_office answer.
 * A contradiction between the two is refused rather than silently resolved:
 * one of them is wrong, and the operator has the research in front of them.
 */
export function hasHeldPublicOfficeContradiction(input: {
  candidateHasHeldPublicOffice: boolean | null;
  evidenceHasHeldPublicOffice: boolean | null;
}): string | null {
  if (
    input.candidateHasHeldPublicOffice === null ||
    input.evidenceHasHeldPublicOffice === null ||
    input.candidateHasHeldPublicOffice === input.evidenceHasHeldPublicOffice
  ) {
    return null;
  }
  return `evidence file says has_held_public_office=${input.evidenceHasHeldPublicOffice} but candidates.has_held_public_office=${input.candidateHasHeldPublicOffice}. One of them is wrong: if the evidence file is wrong, fix it; if the stored value is stale, correct it with a profile re-write carrying the researched answer and --replace-profile-fields has_held_public_office (manual:candidate-profile:write or manual:presidential-profile:write), then rerun this records write.`;
}

/**
 * Holding an office NOW implies having held one — the same rule the profile
 * contract and merge guard enforce. Without this check here, a records
 * write on a column-NULL candidate could claim has_held_public_office=false,
 * take the shorter never_held question list, and persist the false answer,
 * even when candidates.current_office plainly says otherwise.
 */
export function currentOfficeRoutingContradiction(input: {
  candidateCurrentOffice: string | null;
  hasHeldPublicOffice: boolean | null;
}): string | null {
  const office = input.candidateCurrentOffice?.trim() ?? "";
  if (office === "" || input.hasHeldPublicOffice !== false) {
    return null;
  }
  return `candidates.current_office ("${office}") contradicts has_held_public_office=false — a candidate holding a public office now HAS held public office. If the office is real, the routing answer must be true; if current_office is stale or holds an occupation, clear or replace it with a profile write (--clear-profile-fields current_office / --replace-profile-fields current_office), then rerun this records write.`;
}

export function resolveSweepRoute(input: {
  discoveryContestFamily: string | null;
  candidateCurrentOffice: string | null;
  candidateHasHeldPublicOffice: boolean | null;
  evidenceHasHeldPublicOffice: boolean | null;
}): SweepRouteResolution {
  const { candidateHasHeldPublicOffice, evidenceHasHeldPublicOffice } = input;
  const contradiction = hasHeldPublicOfficeContradiction({
    candidateHasHeldPublicOffice,
    evidenceHasHeldPublicOffice,
  });
  if (contradiction !== null) {
    return { ok: false, reason: contradiction };
  }
  // Checked on the EFFECTIVE answer and before the judicial branch: the
  // judicial route also persists a column-NULL candidate's evidence answer,
  // so a false claim against a set current_office must not slip through it.
  const officeContradiction = currentOfficeRoutingContradiction({
    candidateCurrentOffice: input.candidateCurrentOffice,
    hasHeldPublicOffice: candidateHasHeldPublicOffice ?? evidenceHasHeldPublicOffice,
  });
  if (officeContradiction !== null) {
    return { ok: false, reason: officeContradiction };
  }
  const persistHasHeldPublicOffice =
    candidateHasHeldPublicOffice === null ? evidenceHasHeldPublicOffice : null;
  if (input.discoveryContestFamily === "judicial_office") {
    return { ok: true, route: "judicial", persistHasHeldPublicOffice };
  }
  const hasHeld = candidateHasHeldPublicOffice ?? evidenceHasHeldPublicOffice;
  if (hasHeld === null) {
    return {
      ok: false,
      reason:
        'Cannot route the sweep-completeness check: candidates.has_held_public_office is NULL and the evidence file has no top-level "has_held_public_office". Answer it from the profile research (has this candidate EVER held public office, current or former?) and add "has_held_public_office": true|false to the evidence file.',
    };
  }
  return {
    ok: true,
    route: hasHeld ? "officeholder" : "never_held_office",
    persistHasHeldPublicOffice,
  };
}

/**
 * The route question ids not yet covered by any tagged entry. Empty means
 * the claim's question list was fully worked; anything else blocks the
 * completeness claim.
 */
export function listMissingSweepRouteQuestionIds(
  entries: readonly Pick<SweepEvidenceEntry, "questionId">[],
  route: SweepRoute
): string[] {
  const tagged = new Set(entries.map((entry) => entry.questionId).filter((id) => id !== null));
  return SWEEP_ROUTE_QUESTION_IDS[route].filter((id) => !tagged.has(id));
}

/**
 * The full route-coverage gate both records writers run before persisting a
 * full-history ledger: resolve the route (or refuse), then require every
 * route question id on at least one tagged entry. Throws with the
 * operator-facing message; returns the route and the routing answer to
 * persist (non-null only when candidates.has_held_public_office is NULL and
 * the evidence file supplied it).
 */
export function enforceSweepRouteCoverage(input: {
  discoveryContestFamily: string | null;
  candidateCurrentOffice: string | null;
  candidateHasHeldPublicOffice: boolean | null;
  evidenceHasHeldPublicOffice: boolean | null;
  entries: readonly SweepEvidenceEntry[];
}): { route: SweepRoute; persistHasHeldPublicOffice: boolean | null } {
  const resolution = resolveSweepRoute({
    discoveryContestFamily: input.discoveryContestFamily,
    candidateCurrentOffice: input.candidateCurrentOffice,
    candidateHasHeldPublicOffice: input.candidateHasHeldPublicOffice,
    evidenceHasHeldPublicOffice: input.evidenceHasHeldPublicOffice,
  });
  if (!resolution.ok) {
    throw new Error(`Sweep evidence routing failed: ${resolution.reason}`);
  }
  const missingQuestionIds = listMissingSweepRouteQuestionIds(input.entries, resolution.route);
  if (missingQuestionIds.length > 0) {
    throw new Error(
      `Sweep evidence does not cover the ${resolution.route} question list: missing question_id ${missingQuestionIds.join(", ")}. Tag each entry with its question_id (era-split sweeps tag several entries with the same id; extra entries like archive scans omit it). A question that cannot apply still gets its one-line entry — the finding says why it does not apply.`
    );
  }
  return {
    route: resolution.route,
    persistHasHeldPublicOffice: resolution.persistHasHeldPublicOffice,
  };
}

/**
 * Persist the first evidence-backed routing answer inside the write
 * transaction. Guarded on IS NULL so a set value is never overwritten — but
 * the guard alone is not enough under concurrency: two writers can both read
 * NULL before either commits, resolve OPPOSITE routes, and the loser's
 * conditional update would silently match zero rows while its
 * opposite-routed confirmation still committed. So a zero-row update
 * re-reads the column: same value → another writer persisted the same
 * answer, fine; different value → throw, rolling back this writer's
 * confirmation with it.
 */
export async function persistHasHeldPublicOfficeAnswer(
  client: Pick<PoolClient, "query">,
  candidateId: string,
  value: boolean
): Promise<void> {
  const updated = await client.query(
    `
      UPDATE public.candidates
      SET has_held_public_office = $2,
          updated_at = now()
      WHERE id = $1
        AND has_held_public_office IS NULL
    `,
    [candidateId, value]
  );
  if ((updated.rowCount ?? 0) > 0) {
    return;
  }
  const current = await client.query<{ has_held_public_office: boolean | null }>(
    `SELECT has_held_public_office FROM public.candidates WHERE id = $1`,
    [candidateId]
  );
  const stored = current.rows[0]?.has_held_public_office ?? null;
  if (stored !== value) {
    throw new Error(
      `candidates.has_held_public_office is now ${stored} but this write resolved has_held_public_office=${value} from its evidence file — a concurrent write landed first with the opposite answer. Nothing was written; re-check the research and rerun against the stored value.`
    );
  }
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
 * Whether a supplied, validated --evidence-file's entries should flow into
 * the writer's confirmation-handling branch. Full-history writes always
 * keep them: the ledger is persisted (with an empty claim set when the
 * sweep found stance-labeled records). Delta writes keep them only when the
 * zero-record path REQUIRED them — there they gate a timestamp refresh of
 * the prior full-history confirmation, never an upsert; a non-required
 * window ledger stays external (validated and counted only).
 */
export function retainSuppliedSweepEvidence(input: {
  evidenceRequired: boolean;
  deltaMode: boolean;
}): boolean {
  return input.evidenceRequired || !input.deltaMode;
}

/**
 * Persist the validated confirmation inside the writer's transaction. One
 * row per candidate: a newer sweep supersedes the older confirmation. An
 * empty confirmedGapIds list is valid and means the evidenced full sweep
 * found stance-labeled records and asserts no completeness claim.
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
      // Stored shape mirrors the evidence-file contract (snake_case
      // question_id, omitted when untagged) so audits read one format.
      JSON.stringify({
        entries: input.entries.map((entry) => ({
          question: entry.question,
          finding: entry.finding,
          ...(entry.questionId != null ? { question_id: entry.questionId } : {}),
        })),
      }),
      input.contextType,
      input.contextId,
    ]
  );
}

/**
 * Re-assert an existing confirmation without touching its content: a
 * zero-record DELTA write re-verified the claim for its window, and the
 * audit treats a confirmation older than the latest search stamp as
 * historical — so confirmed_at must advance with the stamp. The original
 * full-sweep evidence, gap ids, and context are the support for the
 * full-history claim and must NOT be replaced by window-only evidence
 * (the window evidence lives in the writer's --evidence-file and the run
 * report). Caller guarantees the row exists.
 */
export async function refreshSweepConfirmationTimestamp(
  client: Pick<PoolClient, "query">,
  candidateId: string
): Promise<void> {
  await client.query(
    `
      UPDATE public.candidate_record_sweep_confirmations
      SET confirmed_at = now(),
          updated_at = now()
      WHERE candidate_id = $1
    `,
    [candidateId]
  );
}

/**
 * A live write that found real, stance-labeled records without carrying a
 * ledger falsifies any earlier COMPLETENESS confirmation — the table would
 * otherwise keep asserting no_records_found / only_general_labels for a
 * candidate who now has records — so remove such rows inside the same
 * transaction. Empty-claim-set rows ("sweep ran, stances found") are NOT
 * falsified by finding more records; they stay and age out against the
 * search stamp like any confirmation (the audit already treats a
 * confirmation older than the latest stamp as historical).
 *
 * Only safe where the write ALSO advances last_records_searched_at (the
 * district writer does): the stamp is what dates a surviving row as
 * historical. A writer that advances no stamp must use
 * deleteSweepConfirmation instead.
 */
export async function deleteSweepCompletenessConfirmation(
  client: Pick<PoolClient, "query">,
  candidateId: string
): Promise<void> {
  await client.query(
    `
      DELETE FROM public.candidate_record_sweep_confirmations
      WHERE candidate_id = $1
        AND confirmed_gap_ids && $2::text[]
    `,
    [candidateId, [...SWEEP_COMPLETENESS_GAP_IDS]]
  );
}

/**
 * Unconditional variant for writers that advance no per-candidate search
 * stamp (manual:presidential-records:write — markCandidateRecordsSearchCompleted
 * is district-path only). Without an advancing stamp, a surviving
 * empty-claim-set row could never be dated as historical, so "newest sweep
 * wins" demands removal when a newer sweep arrives without a ledger.
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
      `The file must contain {"entries": [{"question": "...", "finding": "...", "question_id": "..."}, ...]} — one row per discovery question actually asked (minimum ${SWEEP_EVIDENCE_MIN_ENTRIES}), with question_id tags covering the candidate's route question list.`,
      "If the question list has not been finished, finish it (or run the remaining questions) instead of asserting completeness.",
    ].join("\n")
  );
}
