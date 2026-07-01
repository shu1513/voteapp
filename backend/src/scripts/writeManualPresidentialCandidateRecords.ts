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
import {
  loadCandidatePresidentialCycleOfficeContext,
  type CandidateRecordPresidentialRole,
} from "../pipeline/candidates/candidateRecordOfficeContext.js";
import {
  buildCandidateRecordIdentityKey,
  upsertCandidateRecords,
} from "../pipeline/candidates/candidateRecordStore.js";
import {
  applyConfirmedGaps,
  buildCandidateRecordQualityGaps,
  droppedRecordToGap,
  isBlockingCandidateRecordQualityGap,
  qualityDroppedRecordsToGaps,
} from "./writeManualCandidateRecords.js";
import {
  buildManualResearchRepairReport,
  summarizeManualResearchGaps,
  writeManualResearchRepairReport,
  type ManualResearchRepairGap,
} from "./manualResearchRepairReport.js";

export type ManualPresidentialCandidateRecordsOptions = {
  candidateId: string;
  presidentialCycleId: string;
  presidentialRole: CandidateRecordPresidentialRole;
  recordsFile: string;
  labelsFile: string;
  repairReportFile: string | null;
  strictQualityGate: boolean;
  confirmedGapIds: Set<string>;
  dryRun: boolean;
};

function usage(): string {
  return [
    "Usage:",
    "  npm run manual:presidential-records:write -- --candidate-id uuid --presidential-cycle-id uuid --presidential-role president|vice_president --records-file records.json --labels-file labels.json [--strict-quality-gate] [--confirmed-gap id] [--repair-report-file file] [--dry-run]",
    "",
    "records.json must match CandidateRecordDiscoveryPayload. labels.json must match CandidateRecordAreaLabelPayload.",
  ].join("\n");
}

function readValueFlag(args: readonly string[], name: string): string | null {
  const index = args.indexOf(name);
  if (index >= 0) {
    const value = args[index + 1];
    if (!value || value.startsWith("--") || value.trim().length === 0) {
      throw new Error(`Missing value for ${name}.\n${usage()}`);
    }
    return value;
  }

  const prefix = `${name}=`;
  const match = args.find((token) => token.startsWith(prefix));
  if (!match) {
    return null;
  }
  const value = match.slice(prefix.length);
  if (value.trim().length === 0) {
    throw new Error(`Missing value for ${name}.\n${usage()}`);
  }
  return value;
}

function readBooleanFlag(args: readonly string[], name: string): boolean {
  const inline = args.find((arg) => arg.startsWith(`${name}=`));
  if (inline) {
    throw new Error(`Boolean flag must not include a value: ${name}`);
  }
  const index = args.indexOf(name);
  if (index < 0) {
    return false;
  }
  const next = args[index + 1];
  if (next && !next.startsWith("--")) {
    throw new Error(`Boolean flag must not include a value: ${name}`);
  }
  return true;
}

function readRepeatedFlag(args: readonly string[], name: string): string[] {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === name) {
      const value = args[index + 1];
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

function normalizeRequiredFlag(raw: string | null, name: string): string {
  const value = raw?.trim() ?? "";
  if (value.length === 0) {
    throw new Error(`Missing ${name}.\n${usage()}`);
  }
  return value;
}

function parsePresidentialRole(raw: string | null): CandidateRecordPresidentialRole {
  const value = normalizeRequiredFlag(raw, "--presidential-role");
  if (value === "president" || value === "vice_president") {
    return value;
  }
  throw new Error(`Invalid --presidential-role value: ${value}.\n${usage()}`);
}

function normalizeConfirmedGaps(values: readonly string[]): Set<string> {
  return new Set(values.map((value) => value.trim()).filter((value) => value.length > 0));
}

export function parseManualPresidentialCandidateRecordsArgs(
  args: readonly string[]
): ManualPresidentialCandidateRecordsOptions {
  return {
    candidateId: normalizeRequiredFlag(readValueFlag(args, "--candidate-id"), "--candidate-id"),
    presidentialCycleId: normalizeRequiredFlag(
      readValueFlag(args, "--presidential-cycle-id"),
      "--presidential-cycle-id"
    ),
    presidentialRole: parsePresidentialRole(readValueFlag(args, "--presidential-role")),
    recordsFile: normalizeRequiredFlag(readValueFlag(args, "--records-file"), "--records-file"),
    labelsFile: normalizeRequiredFlag(readValueFlag(args, "--labels-file"), "--labels-file"),
    repairReportFile: readValueFlag(args, "--repair-report-file"),
    strictQualityGate: readBooleanFlag(args, "--strict-quality-gate"),
    confirmedGapIds: normalizeConfirmedGaps(readRepeatedFlag(args, "--confirmed-gap")),
    dryRun: readBooleanFlag(args, "--dry-run"),
  };
}

async function readJsonFile(path: string): Promise<unknown> {
  const raw = await readFile(path, "utf8");
  return JSON.parse(raw) as unknown;
}

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for manual presidential candidate records write`);
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

function manualKey(options: ManualPresidentialCandidateRecordsOptions): string {
  return `manual:presidential-records:${options.presidentialCycleId}:${options.presidentialRole}:${options.candidateId}`;
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

function buildRecordLabelParseGap(reason: string): ManualResearchRepairGap {
  return {
    id: "candidate_record_labels.payload",
    stage: "candidate_record_labels",
    objectType: "candidate_record_label",
    outcome: "needs_repair",
    failureKind: "label_validation",
    reason,
    promptFile: "src/ai/providers/candidateRecordAreaLabelPrompt.ts",
    focusedResearchPass:
      "Run a focused presidential candidate-record area-label pass for the verified records. Ensure every record has at least one allowed label, non-stance areas omit stance, and stance-bearing areas include for/against.",
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
    focusedResearchPass:
      "Run a focused presidential area/stance repair pass for this label. Use only allowed research areas and assign stance only when the evidence supports it.",
  }));
}

async function writeRecordsRepairReport(input: {
  reportFile: string | null;
  key: string;
  options: ManualPresidentialCandidateRecordsOptions;
  candidateDisplayName?: string | null;
  gaps: ManualResearchRepairGap[];
}): Promise<void> {
  await writeManualResearchRepairReport(
    input.reportFile,
    buildManualResearchRepairReport({
      command: "manual:presidential-records:write",
      manualKey: input.key,
      target: {
        candidateId: input.options.candidateId,
        contextType: "presidential_cycle",
        presidentialCycleId: input.options.presidentialCycleId,
        presidentialRole: input.options.presidentialRole,
        recordsFile: input.options.recordsFile,
        labelsFile: input.options.labelsFile,
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
      throw new Error(
        `Cannot delete stale candidate_record_area_tags: missing research area id for slug '${label.researchAreaSlug}'`
      );
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
  const options = parseManualPresidentialCandidateRecordsArgs(process.argv.slice(2));
  const key = manualKey(options);

  const rawRecords = await readJsonFile(options.recordsFile);
  const validatedRecords = await validateCandidateRecordDiscoveryPayload(
    rawRecords,
    readPositiveIntegerEnv("AI_TIMEOUT_MS", 90_000)
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
        focusedResearchPass:
          "Run a focused presidential candidate-record payload repair pass. Fix only the schema issue, then rerun the manual presidential records writer.",
      },
    ];
    await writeRecordsRepairReport({
      reportFile: options.repairReportFile,
      key,
      options,
      candidateDisplayName: null,
      gaps,
    });
    throw new Error(`Presidential candidate records payload failed validation: ${validatedRecords.reason}`);
  }

  const blockingDroppedRecords = validatedRecords.droppedRecords.filter(
    (record) => record.failureKind !== "quality_gap"
  );
  const qualityDroppedGaps = qualityDroppedRecordsToGaps(validatedRecords.droppedRecords);

  if (blockingDroppedRecords.length > 0) {
    const gaps = validatedRecords.droppedRecords.map(droppedRecordToGap);
    await writeRecordsRepairReport({
      reportFile: options.repairReportFile,
      key,
      options,
      candidateDisplayName: null,
      gaps,
    });
    throw new Error(
      `Presidential candidate records payload needs focused source/schema repair before import; dropped=${blockingDroppedRecords.length}; ${summarizeDroppedRecords(blockingDroppedRecords)}`
    );
  }

  const pool = new Pool({ connectionString: requireEnv("DATABASE_URL") });
  try {
    const context = await loadCandidatePresidentialCycleOfficeContext(
      pool,
      options.candidateId,
      options.presidentialCycleId,
      options.presidentialRole
    );
    if (!context) {
      throw new Error(
        `Candidate/presidential-cycle link not found for candidate_id=${options.candidateId} presidential_cycle_id=${options.presidentialCycleId} role=${options.presidentialRole}`
      );
    }
    if (!context.officeId) {
      throw new Error(
        `Presidential cycle has no office_id for candidate-record labeling; presidential_cycle_id=${options.presidentialCycleId} role=${options.presidentialRole}`
      );
    }

    const allowedAreas = await loadAllowedResearchAreasForOfficeId(pool, context.officeId);
    if (allowedAreas.length === 0) {
      throw new Error(`No allowed research areas for office_id=${context.officeId}`);
    }
    const allowedSlugs = new Set(allowedAreas.map((area) => area.slug));
    const rawLabels = await readJsonFile(options.labelsFile);
    const parsedLabels = parseCandidateRecordAreaLabelPayload(rawLabels, {
      allowedResearchAreaSlugs: allowedSlugs,
      recordCount: validatedRecords.records.length,
      requireLabelForEveryRecord: true,
    });
    if (!parsedLabels.ok) {
      const gaps = [buildRecordLabelParseGap(parsedLabels.reason)];
      await writeRecordsRepairReport({
        reportFile: options.repairReportFile,
        key,
        options,
        candidateDisplayName: context.candidateDisplayName,
        gaps,
      });
      throw new Error(`Presidential candidate record labels payload failed validation: ${parsedLabels.reason}`);
    }

    const qualityGaps = applyConfirmedGaps(
      [
        ...qualityDroppedGaps,
        ...buildCandidateRecordQualityGaps({
          recordCount: validatedRecords.records.length,
          labels: parsedLabels.payload.labels,
        }),
      ],
      options.confirmedGapIds
    );
    const blockingQualityGaps = qualityGaps.filter(isBlockingCandidateRecordQualityGap);
    if (options.repairReportFile && qualityGaps.length > 0) {
      await writeRecordsRepairReport({
        reportFile: options.repairReportFile,
        key,
        options,
        candidateDisplayName: context.candidateDisplayName,
        gaps: qualityGaps,
      });
    }
    if (options.strictQualityGate && blockingQualityGaps.length > 0) {
      throw new Error(
        `Presidential candidate records quality gate failed; run focused gap-repair pass before import. gaps=${blockingQualityGaps.length}; ${summarizeManualResearchGaps(blockingQualityGaps)}`
      );
    }

    if (options.dryRun) {
      console.log(
        JSON.stringify(
          {
            dryRun: true,
            manualKey: key,
            candidateId: options.candidateId,
            contextType: "presidential_cycle",
            presidentialCycleId: options.presidentialCycleId,
            presidentialRole: options.presidentialRole,
            candidateDisplayName: context.candidateDisplayName,
            recordCount: validatedRecords.records.length,
            labelCount: parsedLabels.payload.labels.length,
            sourceValidation: {
              sourceUrlsReachable: true,
              droppedRecordCount: validatedRecords.droppedRecords.length,
            },
            qualityGate: {
              strict: options.strictQualityGate,
              confirmedGaps: [...options.confirmedGapIds].sort(),
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
          candidateId: options.candidateId,
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
          reportFile: options.repairReportFile,
          key,
          options,
          candidateDisplayName: context.candidateDisplayName,
          gaps,
        });
        throw new Error(`Presidential candidate record label validation failed: ${reason}`);
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
      await client.query("COMMIT");

      console.log(
        JSON.stringify(
          {
            manualKey: key,
            candidateId: options.candidateId,
            contextType: "presidential_cycle",
            presidentialCycleId: options.presidentialCycleId,
            presidentialRole: options.presidentialRole,
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
    console.error("manual presidential candidate records write failed:", message);
    process.exitCode = 1;
  });
}
