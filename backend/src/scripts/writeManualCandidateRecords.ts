import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Pool, type PoolClient } from "pg";

import {
  validateCandidateRecordDiscoveryPayload,
  type CandidateRecordDroppedRecord,
} from "../ai/enrichCandidateRecords.js";
import { loadProjectEnv } from "../config/env.js";
import { parseCandidateRecordAreaLabelPayload } from "../contracts/candidateRecordAreaLabelPayloadContract.js";
import {
  loadAllowedResearchAreasForOfficeId,
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
  type CandidateRecordAreaLabelInput,
} from "../pipeline/candidates/candidateRecordAreaTagging.js";
import { loadCandidateElectionOfficeContext } from "../pipeline/candidates/candidateRecordOfficeContext.js";
import { isNonStanceResearchAreaSlug } from "../pipeline/candidates/candidateRecordResearchAreaPolicy.js";
import {
  buildCandidateRecordIdentityKey,
  upsertCandidateRecords,
} from "../pipeline/candidates/candidateRecordStore.js";
import { createCandidateRecordUpdateNotificationEvents } from "../pipeline/users/candidateFollowNotificationEvents.js";
import {
  buildManualResearchRepairReport,
  summarizeManualResearchGaps,
  writeManualResearchRepairReport,
  type ManualResearchRepairGap,
} from "./manualResearchRepairReport.js";

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:candidate-records:write -- --candidate-id uuid --election-id uuid --records-file records.json --labels-file labels.json [--strict-quality-gate] [--confirmed-gap id] [--repair-report-file file] [--dry-run]",
    "",
    "records.json must match CandidateRecordDiscoveryPayload. labels.json must match CandidateRecordAreaLabelPayload.",
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

export function qualityDroppedRecordsToGaps(
  droppedRecords: readonly CandidateRecordDroppedRecord[]
): ManualResearchRepairGap[] {
  return droppedRecords.flatMap((record, index) =>
    record.failureKind === "quality_gap" ? [droppedRecordToGap(record, index)] : []
  );
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
  loadProjectEnv();

  const candidateId = readFlag("--candidate-id");
  const electionId = readFlag("--election-id");
  const recordsFile = readFlag("--records-file");
  const labelsFile = readFlag("--labels-file");
  const repairReportFile = readFlag("--repair-report-file");
  const strictQualityGate = hasFlag("--strict-quality-gate");
  const confirmedGapIds = normalizeConfirmedGaps(readRepeatedFlag("--confirmed-gap"));
  if (!candidateId || !electionId || !recordsFile || !labelsFile) {
    throw new Error(`Missing required flag.\n${usage()}`);
  }
  const manualKey = `manual:candidate-records:${electionId}:${candidateId}`;

  const rawRecords = await readJsonFile(recordsFile);
  const validatedRecords = await validateCandidateRecordDiscoveryPayload(
    rawRecords,
    readPositiveIntegerEnv("AI_TIMEOUT_MS", 90000)
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
  const blockingDroppedRecords = validatedRecords.droppedRecords.filter(
    (record) => record.failureKind !== "quality_gap"
  );
  const qualityDroppedGaps = qualityDroppedRecordsToGaps(validatedRecords.droppedRecords);

  if (blockingDroppedRecords.length > 0) {
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
    throw new Error(
      `Candidate records payload needs focused source/schema repair before import; dropped=${blockingDroppedRecords.length}; ${summarizeDroppedRecords(blockingDroppedRecords)}`
    );
  }

  const dryRun = hasFlag("--dry-run");

  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });

  try {
    const context = await loadCandidateElectionOfficeContext(pool, candidateId, electionId);
    if (!context) {
      throw new Error(`Candidate/election link not found for candidate_id=${candidateId} election_id=${electionId}`);
    }
    if (!context.officeId) {
      throw new Error(`Election has no office_id for candidate-record labeling; election_id=${electionId}`);
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
    const qualityGaps = applyConfirmedGaps(
      [
        ...qualityDroppedGaps,
        ...buildCandidateRecordQualityGaps({
          recordCount: validatedRecords.records.length,
          labels: parsedLabels.payload.labels,
        }),
      ],
      confirmedGapIds
    );
    const blockingQualityGaps = qualityGaps.filter(isBlockingCandidateRecordQualityGap);
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
      await client.query("COMMIT");

      console.log(
        JSON.stringify(
          {
            manualKey,
            candidateId,
            electionId,
            inserted: upsert.inserted,
            updated: upsert.updated,
            processed: upsert.processed,
            tagsDeleted: staleTagDelete.deleted,
            tagsProcessed: tagResult.processed,
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
