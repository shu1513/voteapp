import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import {
  validateCandidateRecordDiscoveryPayload,
  type CandidateRecordDroppedRecord,
} from "../ai/enrichCandidateRecords.js";
import { loadProjectEnv } from "../config/env.js";
import { requireLocalDatabaseTarget } from "./localDatabaseGuard.js";
import { parseCandidateRecordAreaLabelPayload } from "../contracts/candidateRecordAreaLabelPayloadContract.js";
import {
  loadAllowedResearchAreasForOfficeId,
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
  type CandidateRecordAreaLabelInput,
} from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { loadCandidateElectionOfficeContext } from "../pipeline/candidates/candidateRecordOfficeContext.js";
import { markCandidateRecordsSearchCompleted } from "../pipeline/candidates/candidateRecordsSearchClaim.js";
import { isNonStanceResearchAreaSlug } from "../pipeline/candidates/candidateRecordResearchAreaPolicy.js";
import {
  buildCandidateRecordIdentityKey,
  upsertCandidateRecords,
} from "../pipeline/candidates/candidateRecordStore.js";
import { createCandidateRecordUpdateNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";
import { usLatestLocalDateIso } from "../utils/usLocalDate.js";
import {
  buildManualResearchRepairReport,
  summarizeManualResearchGaps,
  writeManualResearchRepairReport,
  type ManualResearchRepairGap,
} from "./manualResearchRepairReport.js";
import {
  SWEEP_COMPLETENESS_GAP_IDS,
  assertedSweepCompletenessGapIds,
  deleteSweepCompletenessConfirmation,
  enforceSweepRouteCoverage,
  currentOfficeRoutingContradiction,
  hasHeldPublicOfficeContradiction,
  parseSweepEvidencePayload,
  persistHasHeldPublicOfficeAnswer,
  refreshSweepConfirmationTimestamp,
  retainSuppliedSweepEvidence,
  sweepEvidenceMissingError,
  sweepEvidenceRequired,
  upsertSweepConfirmation,
  type SweepEvidenceEntry,
  type SweepRoute,
} from "./candidateRecordSweepEvidence.js";

import { assertKnownCliFlags } from "./manualCliFlags.js";
import { WALL_CLOCK_FORCE_EXIT_GRACE_MS, withWallClockTimeout } from "./wallClockTimeout.js";
function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-records:write -- --candidate-id uuid --election-id uuid --records-file records.json --labels-file labels.json [--since-date YYYY-MM-DD] [--strict-quality-gate] [--confirmed-gap id] [--evidence-file evidence.json] [--repair-report-file file] [--dry-run]",
    "",
    "records.json must match CandidateRecordDiscoveryPayload. labels.json must match CandidateRecordAreaLabelPayload.",
    'A zero-record payload, an all-neutral (general/integrity_and_ethics-only) label set, --confirmed-gap candidate_records.no_records_found, or --confirmed-gap candidate_records.only_general_labels asserts a FINISHED discovery sweep — in any mode, strict or not — and requires --evidence-file with the per-question evidence table: {"entries": [{"question": "...", "finding": "...", "question_id": "..."}, ...]}.',
    "A supplied --evidence-file on a stance-bearing FULL-history write is persisted too (candidate_record_sweep_confirmations with an empty claim set), so keep supplying the ledger — the output reports sweepEvidence.persisted (dry-run: wouldPersist).",
    "",
    "Every full-history ledger must COVER its route's question list via question_id tags: judicial contests (discovery_contest_family=judicial_office) need cases, discipline, endorsements; officeholders (has EVER held public office) need rollcalls, sponsorship, executive, proceedings, leadership, outside_chamber, endorsements; never-held candidates need career, orgs_advocacy, court_legal, endorsements. Era-split sweeps tag several entries with the same question_id; extra entries (archive scans, office-area follow-ups) omit it. Non-judicial routing reads candidates.has_held_public_office; when that column is NULL the evidence file must carry a top-level \"has_held_public_office\": true|false, which the write persists.",
    "",
    "--since-date runs a DELTA (windowed) refresh: it must be on/before the candidate's last_records_researched_through checkpoint (later would skip dates forever), and every record must have event_date >= since-date (out-of-window rows are an error, not a silent drop — remove them and their labels so indices stay aligned). Delta mode makes no full-history claims: the no_records_found / only_general_labels quality gaps are skipped and ALL --confirmed-gap flags are disallowed. A zero-record delta write still requires --evidence-file with the WINDOW-scoped per-question evidence table.",
  ].join("\n");
}

function readFlag(name: string): string | null {
  const index = process.argv.indexOf(name);
  if (index >= 0) {
    const value = process.argv[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }
  const prefix = `${name}=`;
  const match = process.argv.find((token) => token.startsWith(prefix));
  if (!match) {
    return null;
  }
  const value = match.slice(prefix.length);
  if (value.trim().length === 0) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

function readRepeatedFlag(name: string): string[] {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < process.argv.length; index += 1) {
    const token = process.argv[index];
    if (token === name) {
      const value = process.argv[index + 1];
      if (!value || value.startsWith("--") || value.trim().length === 0) {
        throw new Error(`Missing value for ${name}.\n${usage()}`);
      }
      values.push(value.trim());
      index += 1;
      continue;
    }
    if (token?.startsWith(prefix)) {
      const value = token.slice(prefix.length).trim();
      if (value.length === 0) {
        throw new Error(`Missing value for ${name}.\n${usage()}`);
      }
      values.push(value);
    }
  }
  return values;
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual candidate records write`);
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

function summarizeDroppedRecords(droppedRecords: readonly CandidateRecordDroppedRecord[]): string {
  const preview = droppedRecords
    .slice(0, 5)
    .map((dropped, index) => {
      const label = dropped.record.description || dropped.record.source_url || `row ${index + 1}`;
      return `${dropped.failureKind}/${dropped.failureType}: ${label} (${dropped.reason})`;
    })
    .join("; ");
  const extra = droppedRecords.length > 5 ? `; +${droppedRecords.length - 5} more` : "";
  return `${preview}${extra}`;
}

function normalizeConfirmedGaps(values: readonly string[]): Set<string> {
  return new Set(values.map((value) => value.trim()).filter((value) => value.length > 0));
}

const NO_RECORDS_FOUND_GAP_ID = "candidate_records.no_records_found";
const ONLY_GENERAL_LABELS_GAP_ID = "candidate_records.only_general_labels";
const BLOCKING_CANDIDATE_RECORD_QUALITY_GAP_IDS = new Set([
  NO_RECORDS_FOUND_GAP_ID,
  ONLY_GENERAL_LABELS_GAP_ID,
]);

export function applyConfirmedGaps(
  gaps: readonly ManualResearchRepairGap[],
  confirmedGapIds: ReadonlySet<string>
): ManualResearchRepairGap[] {
  return gaps.map((gap) => {
    if (gap.outcome !== "needs_repair" || !confirmedGapIds.has(gap.id)) {
      return gap;
    }
    return {
      ...gap,
      outcome: gap.id === ONLY_GENERAL_LABELS_GAP_ID ? "confirmed_neutral" : "confirmed_null",
      reason: `${gap.reason} Operator marked this gap confirmed after focused repair research.`,
    };
  });
}

export function droppedRecordToGap(
  dropped: CandidateRecordDroppedRecord,
  index: number
): ManualResearchRepairGap {
  const sourceUrl = dropped.record.source_url || undefined;
  const base = {
    id: `candidate_records.dropped.${index}`,
    stage: "candidate_records" as const,
    objectType: "candidate_record" as const,
    outcome: "needs_repair" as const,
    sourceUrl,
    eventDate: dropped.record.event_date || undefined,
    description: dropped.record.description || undefined,
    failureKind: dropped.failureKind,
    failureType: dropped.failureType,
    reason: dropped.reason,
  };
  if (dropped.failureKind === "quality_gap") {
    return {
      ...base,
      promptFile: "src/ai/providers/candidateRecordDiscoveryPrompt.ts",
      focusedResearchPass:
        "Do a deeper record-only research pass. Do not replace this with another candidacy, filing, ballot-listing, campaign promise, or campaign-launch row. Find an actual action, public service record, organizational leadership record, vote, official decision, litigation/enforcement record, endorsement, or other source-backed conduct; otherwise leave it dropped and rely on candidate_records.no_records_found or candidate_records.only_general_labels after focused repair.",
    };
  }
  return {
    ...base,
    promptFile: "src/ai/providers/candidateRecordSourceRepairPrompt.ts",
    focusedResearchPass: "Run a focused candidate-record source/schema repair pass for this dropped row. Replace bad URLs, fix invalid dates/descriptions, or mark no reliable replacement.",
  };
}

function buildRecordLabelParseGap(reason: string): ManualResearchRepairGap {
  return {
    id: "candidate_record_labels.payload",
    stage: "candidate_record_labels",
    objectType: "candidate_record_label",
    outcome: "needs_repair",
    failureKind: "label_validation",
    reason,
    promptFile: "src/ai/providers/candidateRecordAreaLabelPrompt.ts",
    focusedResearchPass: "Run a focused candidate-record area-label pass for the verified records. Ensure every record has at least one allowed label, non-stance areas omit stance, and stance-bearing areas include for/against.",
  };
}

function buildRecordLabelValidationGaps(reasons: readonly string[]): ManualResearchRepairGap[] {
  return reasons.map((reason, index) => ({
    id: `candidate_record_labels.validation.${index}`,
    stage: "candidate_record_labels",
    objectType: "candidate_record_label",
    outcome: "needs_repair",
    labelIndex: index,
    failureKind: "label_validation",
    reason,
    promptFile: "src/ai/providers/candidateRecordAreaLabelPrompt.ts",
    focusedResearchPass: "Run a focused area/stance repair pass for this label. Use only allowed research areas and assign stance only when the evidence supports it.",
  }));
}

export function buildCandidateRecordQualityGaps(input: {
  recordCount: number;
  labels: readonly { research_area_slug: string }[];
}): ManualResearchRepairGap[] {
  const gaps: ManualResearchRepairGap[] = [];
  if (input.recordCount === 0) {
    gaps.push({
      id: NO_RECORDS_FOUND_GAP_ID,
      stage: "candidate_records",
      objectType: "candidate_record_set",
      outcome: "needs_repair",
      failureKind: "quality_gap",
      reason: "Candidate record discovery produced zero verified records.",
      promptFile: "src/ai/providers/candidateRecordDiscoveryPrompt.ts",
      focusedResearchPass: "Run a focused no-records verification pass for this candidate/election context. If no reliable action records exist, mark candidate_records.no_records_found confirmed_null.",
    });
  }
  if (
    input.recordCount > 0 &&
    input.labels.length > 0 &&
    input.labels.every((label) => isNonStanceResearchAreaSlug(label.research_area_slug))
  ) {
    gaps.push({
      id: ONLY_GENERAL_LABELS_GAP_ID,
      stage: "candidate_record_labels",
      objectType: "candidate_record_set",
      outcome: "needs_repair",
      failureKind: "quality_gap",
      reason: "Every candidate record label is non-stance/general. This may be correct for neutral records, but it needs a focused issue-record check before production-grade import.",
      promptFile: "src/ai/providers/candidateRecordDiscoveryPrompt.ts",
      focusedResearchPass: "Run a focused substantive-record search for votes, official actions, endorsements, litigation/enforcement, public service, or other source-backed conduct. If records are genuinely neutral only, mark candidate_records.only_general_labels confirmed_neutral.",
    });
  }
  return gaps;
}

export function isBlockingCandidateRecordQualityGap(gap: ManualResearchRepairGap): boolean {
  return (
    gap.outcome === "needs_repair" &&
    BLOCKING_CANDIDATE_RECORD_QUALITY_GAP_IDS.has(gap.id)
  );
}

/**
 * Delta (windowed) refresh support: --since-date scopes a manual records pass
 * to events on/after the window start (normally the candidate's
 * last_records_researched_through, per the followed-candidate refresh
 * workflow). The window start must be a real past-or-today calendar date;
 * anything else would silently window out every record and stamp a false
 * completion.
 */
export function parseSinceDate(value: string, todayIsoDate: string): string {
  const normalized = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error(`--since-date must be a full YYYY-MM-DD date, got: ${value}`);
  }
  const parsed = new Date(`${normalized}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== normalized) {
    throw new Error(`--since-date is not a real calendar date: ${value}`);
  }
  if (normalized > todayIsoDate) {
    throw new Error(`--since-date cannot be in the future: ${value} (today is ${todayIsoDate})`);
  }
  return normalized;
}

/**
 * The labels file addresses records by index into the operator's records
 * file, so a delta write cannot silently window-filter rows the way the AI
 * lifecycle does — a filtered mid-list row would slide later labels onto the
 * wrong records. Out-of-window rows are therefore a hard error the operator
 * fixes in the files.
 */
export function listRecordsBeforeSinceDate<T extends { event_date: string; description: string }>(
  records: readonly T[],
  sinceDate: string
): Array<{ index: number; record: T }> {
  const outOfWindow: Array<{ index: number; record: T }> = [];
  for (const [index, record] of records.entries()) {
    if (record.event_date < sinceDate) {
      outOfWindow.push({ index, record });
    }
  }
  return outOfWindow;
}

/**
 * A delta write's window must start at (or before) the candidate's stored
 * research checkpoint. The completion stamp advances
 * last_records_researched_through to today no matter what window was
 * searched, so a --since-date later than the checkpoint would leave the
 * dates in between permanently unsearched — the gap never resurfaces on any
 * due list. Starting earlier than the checkpoint merely re-covers ground
 * (identity-key dedupe absorbs the overlap), so it is allowed.
 * Returns an error reason, or null when the window is safe.
 */
export function validateSinceDateAgainstCheckpoint(
  sinceDate: string,
  researchedThrough: string | null
): string | null {
  if (!researchedThrough) {
    return "Candidate has no last_records_researched_through checkpoint, so there is no covered history for a delta to extend. Run a FULL discovery sweep (no --since-date) instead.";
  }
  if (sinceDate > researchedThrough) {
    return `--since-date ${sinceDate} is after the candidate's research checkpoint ${researchedThrough}; the dates in between would be skipped forever (the completion stamp advances the checkpoint to today). Use --since-date ${researchedThrough} (the due list prints it) or earlier.`;
  }
  return null;
}

export type DeltaZeroRecordConfirmationDecision =
  | { action: "leave" }
  | { action: "refresh" }
  | { action: "error"; reason: string };

/**
 * A zero-record DELTA write asserts only "no new records in the window", so
 * it must not create a fresh full-history confirmation. But it still stamps
 * last_records_searched_at, and manual:records:audit treats a confirmation
 * older than the latest search stamp as historical — so for a candidate with
 * zero records overall, leaving the prior no_records_found confirmation
 * untouched would resurface an already-confirmed candidate as an audit
 * suspect. Decision:
 *  - candidate has records → confirmation table is irrelevant to the audit
 *    gate; leave it untouched.
 *  - candidate has zero records and a prior no_records_found confirmation →
 *    the full-history claim is still true (prior sweep covered history, this
 *    window found nothing new); re-assert it by bumping confirmed_at only.
 *    The original full-sweep evidence stays — window-only evidence must not
 *    replace the support for a full-history claim.
 *  - candidate has zero records and a confirmation that never claimed
 *    no_records_found (empty claim set / only_general_labels — both imply
 *    records existed) → records were removed externally since; refuse with
 *    a message that says so instead of "the sweep was never closed".
 *  - candidate has zero records and no such confirmation → the full-history
 *    question was never evidence-closed; a windowed pass cannot close it.
 */
export function decideDeltaZeroRecordConfirmation(input: {
  existingRecordCount: number;
  priorConfirmedGapIds: readonly string[] | null;
}): DeltaZeroRecordConfirmationDecision {
  if (input.existingRecordCount > 0) {
    return { action: "leave" };
  }
  if (input.priorConfirmedGapIds?.includes(NO_RECORDS_FOUND_GAP_ID)) {
    return { action: "refresh" };
  }
  if (input.priorConfirmedGapIds !== null) {
    // A confirmation exists but never claimed no_records_found (an
    // empty-claim-set or only_general_labels row) — both imply records
    // existed when it was written, yet the candidate now has zero stored
    // records. Something removed them since; "the sweep was never closed"
    // would be actively misleading here.
    return {
      action: "error",
      reason:
        "Delta write found zero records, but the candidate's sweep confirmation says records existed when it was written — candidate_records rows have been removed since. A windowed pass cannot re-close the full-history question; run a FULL discovery sweep (no --since-date) with the per-question evidence table instead.",
    };
  }
  return {
    action: "error",
    reason:
      "Delta write found zero records for a candidate with zero stored records and no evidence-backed no_records_found confirmation. The full-history sweep was never closed, and a windowed pass cannot close it. Run a FULL discovery sweep (no --since-date) with the per-question evidence table instead.",
  };
}

async function writeRecordsRepairReport(input: {
  reportFile: string | null;
  manualKey: string;
  candidateId: string;
  electionId: string;
  recordsFile: string;
  labelsFile: string;
  candidateDisplayName?: string | null;
  gaps: ManualResearchRepairGap[];
}): Promise<void> {
  await writeManualResearchRepairReport(
    input.reportFile,
    buildManualResearchRepairReport({
      command: "manual:candidate-records:write",
      manualKey: input.manualKey,
      target: {
        candidateId: input.candidateId,
        electionId: input.electionId,
        recordsFile: input.recordsFile,
        labelsFile: input.labelsFile,
        candidateDisplayName: input.candidateDisplayName ?? null,
      },
      gaps: input.gaps,
    })
  );
}

async function deleteStaleCandidateRecordAreaTags(
  client: Pick<PoolClient, "query">,
  labels: readonly CandidateRecordAreaLabelInput[],
  researchAreaIdBySlug: ReadonlyMap<string, string>
): Promise<{ deleted: number }> {
  const researchAreaIdsByRecordId = new Map<string, Set<string>>();
  for (const label of labels) {
    const researchAreaId = researchAreaIdBySlug.get(label.researchAreaSlug);
    if (!researchAreaId) {
      throw new Error(`Cannot delete stale candidate_record_area_tags: missing research area id for slug '${label.researchAreaSlug}'`);
    }
    const ids = researchAreaIdsByRecordId.get(label.candidateRecordId) ?? new Set<string>();
    ids.add(researchAreaId);
    researchAreaIdsByRecordId.set(label.candidateRecordId, ids);
  }

  let deleted = 0;
  for (const [candidateRecordId, researchAreaIds] of researchAreaIdsByRecordId.entries()) {
    const result = await client.query(
      `
        DELETE FROM public.candidate_record_area_tags
        WHERE candidate_record_id = $1
          AND NOT (research_area_id = ANY($2::uuid[]))
      `,
      [candidateRecordId, [...researchAreaIds]]
    );
    deleted += result.rowCount ?? 0;
  }

  return { deleted };
}

async function main(): Promise<void> {
  assertKnownCliFlags("manual:candidate-records:write", process.argv.slice(2), [{ name: "--candidate-id", value: "space" }, { name: "--election-id", value: "space" }, { name: "--records-file", value: "space" }, { name: "--labels-file", value: "space" }, { name: "--since-date", value: "space" }, { name: "--repair-report-file", value: "space" }, { name: "--confirmed-gap", value: "space" }, { name: "--evidence-file", value: "space" }, { name: "--strict-quality-gate", value: "none" }, { name: "--dry-run", value: "none" }]);
  loadProjectEnv();

  const candidateId = readFlag("--candidate-id");
  const electionId = readFlag("--election-id");
  const recordsFile = readFlag("--records-file");
  const labelsFile = readFlag("--labels-file");
  const repairReportFile = readFlag("--repair-report-file");
  const evidenceFile = readFlag("--evidence-file");
  const strictQualityGate = hasFlag("--strict-quality-gate");
  const confirmedGapIds = normalizeConfirmedGaps(readRepeatedFlag("--confirmed-gap"));
  const rawSinceDate = readFlag("--since-date");
  const sinceDate = rawSinceDate
    ? parseSinceDate(rawSinceDate, usLatestLocalDateIso())
    : null;
  if (!candidateId || !electionId || !recordsFile || !labelsFile) {
    throw new Error(`Missing required flag.\n${usage()}`);
  }
  if (sinceDate && confirmedGapIds.size > 0) {
    // No --confirmed-gap flag can do anything in a delta write: the two
    // completeness ids assert full-history claims a windowed pass must not
    // make, and every other id is only consumed by the full-sweep quality
    // gate, which delta mode skips. Rejecting them all beats silently
    // accepting a flag that has no effect (e.g. one reused from a prior
    // repair-report invocation).
    const flags = [...confirmedGapIds].sort();
    const completenessFlags = flags.filter((id) => SWEEP_COMPLETENESS_GAP_IDS.has(id));
    throw new Error(
      completenessFlags.length > 0
        ? `--confirmed-gap ${completenessFlags.join(", ")} asserts a full-history claim and is not allowed with --since-date. A delta write only asserts the window; run a full sweep (no --since-date) to make completeness claims.`
        : `--confirmed-gap has no effect in a delta (--since-date) write and is not allowed: ${flags.join(", ")}. Delta mode skips the full-sweep quality gate; repair dropped rows in the payload files instead.`
    );
  }
  const manualKey = `manual:candidate-records:${electionId}:${candidateId}`;

  const rawRecords = await readJsonFile(recordsFile);
  const validatedRecords = await withWallClockTimeout(
    validateCandidateRecordDiscoveryPayload(
      rawRecords,
      readPositiveIntegerEnv("AI_TIMEOUT_MS", 90000)
    ),
    "candidate record citation validation",
    { forceExitAfterMs: WALL_CLOCK_FORCE_EXIT_GRACE_MS }
  );
  if (!validatedRecords.ok) {
    const gaps: ManualResearchRepairGap[] = [
      {
        id: "candidate_records.payload",
        stage: "candidate_records",
        objectType: "candidate_record_set",
        outcome: "needs_repair",
        failureKind: "schema",
        reason: validatedRecords.reason,
        promptFile: "src/ai/providers/candidateRecordDiscoveryPrompt.ts",
        focusedResearchPass: "Run a focused candidate-record payload repair pass. Fix only the schema issue, then rerun the manual records writer.",
      },
    ];
    await writeRecordsRepairReport({
      reportFile: repairReportFile,
      manualKey,
      candidateId,
      electionId,
      recordsFile,
      labelsFile,
      candidateDisplayName: null,
      gaps,
    });
    throw new Error(`Candidate records payload failed validation: ${validatedRecords.reason}`);
  }
  // The labels file addresses records by index into the operator's records
  // file. ANY dropped record (source, schema, or quality gate) shifts that
  // indexing, so every drop blocks the manual import: previously a quality-gate
  // drop slid later labels onto the wrong surviving records, or surfaced as a
  // misleading "labels contains invalid row" error. The operator must repair or
  // remove the dropped row so record and label indices describe the same list.
  if (validatedRecords.droppedRecords.length > 0) {
    const gaps = validatedRecords.droppedRecords.map(droppedRecordToGap);
    await writeRecordsRepairReport({
      reportFile: repairReportFile,
      manualKey,
      candidateId,
      electionId,
      recordsFile,
      labelsFile,
      candidateDisplayName: null,
      gaps,
    });
    const qualityDropped = validatedRecords.droppedRecords.filter(
      (record) => record.failureKind === "quality_gap"
    );
    const hint =
      qualityDropped.length > 0
        ? " Quality-gate drops must be repaired or removed from the records file so label record_index values stay aligned with the records actually imported."
        : "";
    throw new Error(
      `Candidate records payload needs focused repair before import; dropped=${validatedRecords.droppedRecords.length}; ${summarizeDroppedRecords(validatedRecords.droppedRecords)}.${hint}`
    );
  }
  if (sinceDate) {
    const outOfWindow = listRecordsBeforeSinceDate(validatedRecords.records, sinceDate);
    if (outOfWindow.length > 0) {
      const preview = outOfWindow
        .slice(0, 5)
        .map(({ index, record }) => `index=${index} event_date=${record.event_date}: ${record.description.slice(0, 80)}`)
        .join("; ");
      const extra = outOfWindow.length > 5 ? `; +${outOfWindow.length - 5} more` : "";
      throw new Error(
        `Delta write rejects records dated before --since-date ${sinceDate}: ${preview}${extra}. Remove these rows (and their labels, keeping record_index aligned) from the payload files — records before the window start were covered by the previous sweep.`
      );
    }
  }

  const dryRun = hasFlag("--dry-run");

  const databaseUrl = requireEnv("DATABASE_URL");
  requireLocalDatabaseTarget(databaseUrl);
  const pool = new Pool({ connectionString: databaseUrl });

  try {
    const context = await loadCandidateElectionOfficeContext(pool, candidateId, electionId);
    if (!context) {
      throw new Error(`Candidate/election link not found for candidate_id=${candidateId} election_id=${electionId}`);
    }
    if (!context.officeId) {
      throw new Error(`Election has no office_id for candidate-record labeling; election_id=${electionId}`);
    }
    if (sinceDate) {
      const checkpoint = await pool.query<{ last_records_researched_through: string | null }>(
        `SELECT last_records_researched_through::text AS last_records_researched_through
         FROM public.candidates WHERE id = $1::uuid`,
        [candidateId]
      );
      const windowError = validateSinceDateAgainstCheckpoint(
        sinceDate,
        checkpoint.rows[0]?.last_records_researched_through ?? null
      );
      if (windowError) {
        throw new Error(windowError);
      }
    }

    const allowedAreas = await loadAllowedResearchAreasForOfficeId(pool, context.officeId);
    if (allowedAreas.length === 0) {
      throw new Error(`No allowed research areas for office_id=${context.officeId}`);
    }
    const allowedSlugs = new Set(allowedAreas.map((area) => area.slug));
    const rawLabels = await readJsonFile(labelsFile);
    const parsedLabels = parseCandidateRecordAreaLabelPayload(rawLabels, {
      allowedResearchAreaSlugs: allowedSlugs,
      recordCount: validatedRecords.records.length,
      requireLabelForEveryRecord: true,
    });
    if (!parsedLabels.ok) {
      const gaps = [buildRecordLabelParseGap(parsedLabels.reason)];
      await writeRecordsRepairReport({
        reportFile: repairReportFile,
        manualKey,
        candidateId,
        electionId,
        recordsFile,
        labelsFile,
        candidateDisplayName: context.candidateDisplayName,
        gaps,
      });
      throw new Error(`Candidate record labels payload failed validation: ${parsedLabels.reason}`);
    }
    // Delta mode skips the full-history quality gaps: "no new records in the
    // window" is not no_records_found, and window-only neutral labels are not
    // only_general_labels. The zero-record evidence requirement below still
    // applies (window-scoped question table).
    const qualityGaps = sinceDate
      ? []
      : applyConfirmedGaps(
          [
            ...buildCandidateRecordQualityGaps({
              recordCount: validatedRecords.records.length,
              labels: parsedLabels.payload.labels,
            }),
          ],
          confirmedGapIds
        );
    const blockingQualityGaps = qualityGaps.filter(isBlockingCandidateRecordQualityGap);

    // Sweep-completeness guard: an empty verified record set stamps
    // `last_records_searched_at` even without any confirmed-gap flag, an
    // all-neutral label set makes the same finished-issue-search claim in
    // ANY mode (strict or not), and the no_records_found /
    // only_general_labels flags assert a finished discovery sweep outright.
    // Every such claim requires the per-question evidence table; a bare
    // assertion is refused (false gaps poison the candidate forever — the
    // completion stamp stops future re-search). Evaluated AFTER the quality
    // gaps are built so the neutral-only case is caught even when the
    // operator passes no flag and skips --strict-quality-gate.
    const evidenceIsRequired =
      sweepEvidenceRequired({
        recordCount: validatedRecords.records.length,
        confirmedGapIds,
      }) || qualityGaps.some((gap) => SWEEP_COMPLETENESS_GAP_IDS.has(gap.id));
    // A supplied --evidence-file is always parsed and validated, even when
    // the completeness gate does not require it (a stance-bearing record
    // set): reporting `entryCount: null` for a ledger the operator supplied
    // read as "the evidence was silently ignored" across eight live runs.
    // A validated ledger on a FULL-history write is persisted (empty claim
    // set for the stance-bearing case) so the audit can tell an evidenced
    // sweep from a skipped one; a non-required DELTA window ledger stays
    // external — window evidence cannot back a full-history claim.
    let sweepEvidenceEntries: SweepEvidenceEntry[] | null = null;
    let sweepEvidenceEntryCount: number | null = null;
    let evidenceHasHeldPublicOffice: boolean | null = null;
    if (evidenceFile) {
      const parsedEvidence = parseSweepEvidencePayload(await readJsonFile(evidenceFile));
      if (!parsedEvidence.ok) {
        throw new Error(`Sweep evidence file failed validation: ${parsedEvidence.reason}`);
      }
      sweepEvidenceEntryCount = parsedEvidence.entries.length;
      evidenceHasHeldPublicOffice = parsedEvidence.hasHeldPublicOffice;
      if (
        retainSuppliedSweepEvidence({
          evidenceRequired: evidenceIsRequired,
          deltaMode: sinceDate !== null,
        })
      ) {
        sweepEvidenceEntries = parsedEvidence.entries;
      }
    } else if (evidenceIsRequired) {
      throw sweepEvidenceMissingError("candidate-records");
    }
    // True whenever this write will upsert (or would, in dry-run) the
    // supplied ledger as a candidate_record_sweep_confirmations row; the
    // delta zero-record path only refreshes a prior confirmation, so its
    // window ledger is not persisted.
    const sweepEvidencePersisted = sweepEvidenceEntries !== null && sinceDate === null;

    // Route-coverage gate: a full-history ledger asserts the candidate's
    // whole question list was worked, so it must be routed (officeholder /
    // never-held / judicial) and every route question id must be tagged on
    // at least one entry. This is what stops the 2026-07-15 failure class —
    // a generic officeholder-framed template confirmed first-time candidates
    // record-less without the career question ever being asked. Delta window
    // ledgers are exempt from coverage (they assert only their window and
    // never persist), but a routing answer they carry must still not
    // contradict the stored column — silence there would hide a research
    // conflict from the operator.
    let sweepRoute: SweepRoute | null = null;
    let persistHasHeldPublicOffice: boolean | null = null;
    if (sweepEvidencePersisted && sweepEvidenceEntries) {
      const coverage = enforceSweepRouteCoverage({
        discoveryContestFamily: context.discoveryContestFamily,
        candidateCurrentOffice: context.currentOffice,
        candidateHasHeldPublicOffice: context.hasHeldPublicOffice,
        evidenceHasHeldPublicOffice,
        entries: sweepEvidenceEntries,
      });
      sweepRoute = coverage.route;
      persistHasHeldPublicOffice = coverage.persistHasHeldPublicOffice;
    } else if (sinceDate !== null) {
      const contradiction =
        hasHeldPublicOfficeContradiction({
          candidateHasHeldPublicOffice: context.hasHeldPublicOffice,
          evidenceHasHeldPublicOffice,
        }) ??
        currentOfficeRoutingContradiction({
          candidateCurrentOffice: context.currentOffice,
          hasHeldPublicOffice: evidenceHasHeldPublicOffice,
        });
      if (contradiction !== null) {
        throw new Error(`Sweep evidence routing failed: ${contradiction}`);
      }
    }

    if (repairReportFile && qualityGaps.length > 0) {
      await writeRecordsRepairReport({
        reportFile: repairReportFile,
        manualKey,
        candidateId,
        electionId,
        recordsFile,
        labelsFile,
        candidateDisplayName: context.candidateDisplayName,
        gaps: qualityGaps,
      });
    }
    if (strictQualityGate && blockingQualityGaps.length > 0) {
      throw new Error(
        `Candidate records quality gate failed; run focused gap-repair pass before import. gaps=${blockingQualityGaps.length}; ${summarizeManualResearchGaps(blockingQualityGaps)}`
      );
    }

    if (dryRun) {
      console.log(
        JSON.stringify(
          {
            dryRun: true,
            manualKey,
            candidateId,
            electionId,
            candidateDisplayName: context.candidateDisplayName,
            sinceDate,
            recordCount: validatedRecords.records.length,
            labelCount: parsedLabels.payload.labels.length,
            sourceValidation: {
              sourceUrlsReachable: true,
              droppedRecordCount: validatedRecords.droppedRecords.length,
            },
            qualityGate: {
              strict: strictQualityGate,
              confirmedGaps: [...confirmedGapIds].sort(),
              gaps: qualityGaps,
            },
            sweepEvidence: {
              required: evidenceIsRequired,
              entryCount: sweepEvidenceEntryCount,
              route: sweepRoute,
              // Plan language: --dry-run writes nothing, so a past-tense
              // `persisted` here would be factually wrong for JSON consumers.
              wouldPersist: sweepEvidencePersisted,
              wouldPersistHasHeldPublicOffice: persistHasHeldPublicOffice,
            },
            allowedResearchAreaSlugs: [...allowedSlugs].sort(),
          },
          null,
          2
        )
      );
      return;
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const upsert = await upsertCandidateRecords(
        client,
        validatedRecords.records.map((record) => ({
          candidateId,
          description: record.description,
          sourceUrl: record.source_url,
          eventDate: record.event_date,
        }))
      );

      const labelsForValidation: CandidateRecordAreaLabelInput[] = parsedLabels.payload.labels.map((label) => {
        const record = validatedRecords.records[label.record_index];
        if (!record) {
          throw new Error(`record_index out of range in labels: ${label.record_index}`);
        }
        const identityKey = buildCandidateRecordIdentityKey({
          description: record.description,
          sourceUrl: record.source_url,
          eventDate: record.event_date,
        });
        const candidateRecordId = upsert.recordIdsByIdentityKey.get(identityKey);
        if (!candidateRecordId) {
          throw new Error(`Missing persisted candidate_record id for record_index=${label.record_index}`);
        }
        return {
          candidateRecordId,
          researchAreaSlug: label.research_area_slug,
          stance: label.stance ?? null,
        };
      });

      const validation = validateCandidateRecordAreaLabels(labelsForValidation, allowedSlugs);
      if (!validation.ok) {
        const reason = validation.failures.map((failure) => failure.reason).join("; ");
        const gaps = buildRecordLabelValidationGaps(validation.failures.map((failure) => failure.reason));
        await writeRecordsRepairReport({
          reportFile: repairReportFile,
          manualKey,
          candidateId,
          electionId,
          recordsFile,
          labelsFile,
          candidateDisplayName: context.candidateDisplayName,
          gaps,
        });
        throw new Error(`Candidate record label validation failed: ${reason}`);
      }

      const researchAreaIdBySlug = new Map(allowedAreas.map((area) => [area.slug, area.id]));
      const staleTagDelete = await deleteStaleCandidateRecordAreaTags(
        client,
        validation.normalized,
        researchAreaIdBySlug
      );
      const tagResult = await upsertCandidateRecordAreaTags(
        client,
        validation.normalized,
        researchAreaIdBySlug
      );
      for (const insertedRecordId of upsert.insertedRecordIds) {
        await createCandidateRecordUpdateNotificationEvents(client, insertedRecordId);
      }
      // Stamp the same completion columns the AI lifecycle stamps so a manual
      // record pass (including a confirmed zero-record pass) is
      // distinguishable from a candidate whose records were never searched,
      // and shares the rollover cooldown with the AI path. preserveClaim: this
      // script never claims the candidate, so it must not clear a lease a
      // concurrent worker may hold. The checkpoint is the US-latest local
      // date, not the UTC date: a UTC stamp taken after 5pm Pacific claimed
      // tomorrow as researched, and delta refreshes starting from the
      // checkpoint would skip that local day forever (hit live across eight
      // western-timezone writes).
      const researchedThrough = usLatestLocalDateIso();
      await markCandidateRecordsSearchCompleted(client, candidateId, researchedThrough, { preserveClaim: true });
      // First evidence-backed routing answer for this candidate: persist it
      // so the next sweep (manual or AI) routes from the database instead of
      // re-deriving officeholder status. Throws (rolling back the whole
      // write, confirmation included) if a concurrent write set the column
      // to the opposite answer after this run's pre-transaction read.
      if (persistHasHeldPublicOffice !== null) {
        await persistHasHeldPublicOfficeAnswer(client, candidateId, persistHasHeldPublicOffice);
      }
      // Persist the validated confirmation so manual:records:audit can
      // separate an evidence-backed sweep (confirmed null OR stance-bearing
      // with an empty claim set) from a skipped one.
      if (sweepEvidenceEntries && sinceDate) {
        // Zero-record DELTA write: asserts only the window, so it never
        // creates a fresh full-history confirmation. See
        // decideDeltaZeroRecordConfirmation for why the prior confirmation
        // is refreshed (audit treats confirmations older than the search
        // stamp this write is about to make as historical).
        const existingRecords = await client.query<{ record_count: string }>(
          `SELECT count(*)::text AS record_count FROM public.candidate_records WHERE candidate_id = $1`,
          [candidateId]
        );
        const priorConfirmation = await client.query<{ confirmed_gap_ids: string[] | null }>(
          `SELECT confirmed_gap_ids FROM public.candidate_record_sweep_confirmations WHERE candidate_id = $1`,
          [candidateId]
        );
        const decision = decideDeltaZeroRecordConfirmation({
          existingRecordCount: Number(existingRecords.rows[0]?.record_count ?? "0"),
          priorConfirmedGapIds: priorConfirmation.rows[0]?.confirmed_gap_ids ?? null,
        });
        if (decision.action === "error") {
          throw new Error(decision.reason);
        }
        if (decision.action === "refresh") {
          await refreshSweepConfirmationTimestamp(client, candidateId);
        }
      } else if (sweepEvidenceEntries) {
        await upsertSweepConfirmation(client, {
          candidateId,
          confirmedGapIds: assertedSweepCompletenessGapIds({
            recordCount: validatedRecords.records.length,
            confirmedGapIds,
            qualityGapIds: qualityGaps.map((gap) => gap.id),
          }),
          entries: sweepEvidenceEntries,
          contextType: "election",
          contextId: electionId,
        });
      } else {
        // This write found real stance-labeled records without carrying a
        // ledger; drop any earlier completeness confirmation it falsifies
        // (empty-claim-set rows survive — see deleteSweepCompletenessConfirmation).
        await deleteSweepCompletenessConfirmation(client, candidateId);
      }
      await client.query("COMMIT");

      console.log(
        JSON.stringify(
          {
            manualKey,
            candidateId,
            electionId,
            sinceDate,
            inserted: upsert.inserted,
            updated: upsert.updated,
            processed: upsert.processed,
            tagsDeleted: staleTagDelete.deleted,
            tagsProcessed: tagResult.processed,
            sweepEvidence: {
              required: evidenceIsRequired,
              entryCount: sweepEvidenceEntryCount,
              route: sweepRoute,
              persisted: sweepEvidencePersisted,
              persistedHasHeldPublicOffice: persistHasHeldPublicOffice,
            },
            recordsSearchCompletedThrough: researchedThrough,
          },
          null,
          2
        )
      );
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("manual candidate records write failed:", message);
    process.exitCode = 1;
  });
}
