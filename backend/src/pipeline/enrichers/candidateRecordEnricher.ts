import { Pool } from "pg";
import type { PoolClient } from "pg";
import { createClient } from "redis";

import {
  buildCandidateRecordsConfigFromEnv,
  enrichCandidateRecords,
} from "../../ai/enrichCandidateRecords.js";
import {
  buildCandidateRecordSourcesRepairConfigFromEnv,
  enrichCandidateRecordSourcesRepair,
} from "../../ai/enrichCandidateRecordSourcesRepair.js";
import {
  buildCandidateRecordAreasConfigFromEnv,
  enrichCandidateRecordAreas,
} from "../../ai/enrichCandidateRecordAreas.js";
import { classifyCitationVerificationFailure, verifyHttpUrlReachability } from "../../ai/urlReachability.js";
import { getPipelineEnv } from "../../config/env.js";
import { isPresidentialElectionsEnabled } from "../../config/featureFlags.js";
import {
  STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
  STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
  STAGING_CANDIDATE_RECORD_REJECTED_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_RECORD,
} from "../../config/electionsPipeline.js";
import {
  buildCandidateRecordIdentityKey,
  type CandidateRecordUpsertResult,
  deleteCandidateRecordsForReplacementRefresh,
  type CandidateRecordUpsertInput,
  upsertCandidateRecords,
} from "../candidates/candidateRecordStore.js";
import { createCandidateRecordUpdateNotificationEvents } from "../users/candidateFollowNotificationEvents.js";
import {
  type AllowedResearchArea,
  type CandidateRecordAreaLabelInput,
  loadAllResearchAreas,
  loadAllowedResearchAreasForOfficeId,
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
} from "../candidates/candidateRecordAreaTagging.js";
import {
  runCandidateRecordsSearchLifecycle,
  summarizeCandidateRecordsLifecycleResults,
  type CandidateRecordsSearchMetrics,
} from "../candidates/candidateRecordsSearchLifecycle.js";
import {
  loadCandidateElectionOfficeContext,
  loadCandidatePresidentialCycleOfficeContext,
  type CandidateRecordPresidentialRole,
} from "../candidates/candidateRecordOfficeContext.js";
import {
  buildCandidateRecordRunProcessedMarkerKey,
  CANDIDATE_RECORD_RUN_PROCESSED_MARKER_TTL_SECONDS,
} from "../candidates/candidateRecordRunMarkers.js";
import { normalizeHttpUrl } from "../../utils/normalizeHttpUrl.js";
import { decideCandidateRecordOutcome } from "../candidates/candidateRecordOutcomePolicy.js";

type EnricherOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const MAX_DELIVERY_ATTEMPTS = 8;

type CandidateRecordEnricherBatchStats = {
  entries_total: number;
  acked_count: number;
  retried_count: number;
  parked_count: number;
  invalid_item_skips: number;
  missing_context_skips: number;
  label_validation_rejected_count: number;
  dropped_transient_count: number;
  dropped_permanent_count: number;
  repaired_verified_count: number;
  unresolved_after_repair_count: number;
};

type RecordForTagging = {
  description: string;
  source_url: string;
  event_date: string;
};

type AreaLabelForWrite = {
  record_index: number;
  research_area_slug: string;
  stance?: "for" | "against";
};

class CandidateRecordLabelValidationError extends Error {
  readonly failureCount: number;

  constructor(reason: string, failureCount: number) {
    super(`candidate record label validation failed: ${reason}`);
    this.name = "CandidateRecordLabelValidationError";
    this.failureCount = failureCount;
  }
}

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function toCandidateRecordUpsertInput(
  candidateId: string,
  records: readonly RecordForTagging[]
): CandidateRecordUpsertInput[] {
  return records.map((record) => ({
    candidateId,
    description: record.description,
    sourceUrl: record.source_url,
    eventDate: record.event_date,
  }));
}

function buildLabelsForValidation(input: {
  labels: readonly AreaLabelForWrite[];
  recordsForTagging: readonly RecordForTagging[];
  persistedByIdentity: ReadonlyMap<string, string>;
  candidateId: string;
}): CandidateRecordAreaLabelInput[] {
  return input.labels.map((label) => {
    const sourceRecord = input.recordsForTagging[label.record_index];
    if (!sourceRecord) {
      throw new Error(`record_index out of range in labels: ${label.record_index}`);
    }
    const identityKey = buildCandidateRecordIdentityKey({
      description: sourceRecord.description,
      sourceUrl: sourceRecord.source_url,
      eventDate: sourceRecord.event_date,
    });
    const candidateRecordId = input.persistedByIdentity.get(identityKey);
    if (!candidateRecordId) {
      throw new Error(
        `missing candidate_record id for identity key ${identityKey} (candidate_id=${input.candidateId})`
      );
    }
    return {
      candidateRecordId,
      researchAreaSlug: label.research_area_slug,
      stance: label.stance ?? null,
    };
  });
}

async function rollbackQuietly(client: PoolClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch (error) {
    console.error("candidate-record enricher cleanup warning (ROLLBACK):", toReason(error));
  }
}

async function upsertCandidateRecordsWithNotificationEvents(input: {
  pool: Pool;
  records: readonly CandidateRecordUpsertInput[];
}): Promise<CandidateRecordUpsertResult> {
  const client = await input.pool.connect();
  try {
    await client.query("BEGIN");
    const upsert = await upsertCandidateRecords(client, input.records);
    for (const candidateRecordId of upsert.insertedRecordIds) {
      await createCandidateRecordUpdateNotificationEvents(client, candidateRecordId);
    }
    await client.query("COMMIT");
    return upsert;
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

async function replacePresidentialCandidateRecordsAtomically(input: {
  pool: Pool;
  candidateId: string;
  recordsForTagging: readonly RecordForTagging[];
  labels: readonly AreaLabelForWrite[];
  allowedSlugs: readonly string[];
  allowedAreas: readonly AllowedResearchArea[];
}): Promise<{
  deletedCount: number;
  insertedCount: number;
  dedupedCount: number;
  taggedSpecificCount: number;
  taggedGeneralCount: number;
}> {
  const client = await input.pool.connect();
  try {
    await client.query("BEGIN");
    const deleted = await deleteCandidateRecordsForReplacementRefresh(client, input.candidateId);
    const upsert = await upsertCandidateRecords(
      client,
      toCandidateRecordUpsertInput(input.candidateId, input.recordsForTagging)
    );
    const labelsForValidation = buildLabelsForValidation({
      labels: input.labels,
      recordsForTagging: input.recordsForTagging,
      persistedByIdentity: upsert.recordIdsByIdentityKey,
      candidateId: input.candidateId,
    });
    const validation = validateCandidateRecordAreaLabels(
      labelsForValidation,
      new Set(input.allowedSlugs)
    );
    if (!validation.ok) {
      const reason = validation.failures.map((failure) => failure.reason).join("; ");
      throw new CandidateRecordLabelValidationError(reason, validation.failures.length);
    }

    const researchAreaIdBySlug = new Map(input.allowedAreas.map((area) => [area.slug, area.id]));
    await upsertCandidateRecordAreaTags(client, validation.normalized, researchAreaIdBySlug);
    await client.query("COMMIT");

    const taggedGeneralCount = validation.normalized.filter(
      (label) => label.researchAreaSlug === "general"
    ).length;
    return {
      deletedCount: deleted.deletedCount,
      insertedCount: upsert.inserted,
      dedupedCount: upsert.updated,
      taggedGeneralCount,
      taggedSpecificCount: validation.normalized.length - taggedGeneralCount,
    };
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

function parseCandidateRecordContextType(value: string | undefined): "election" | "presidential_cycle" {
  return value === "presidential_cycle" ? "presidential_cycle" : "election";
}

function parsePresidentialRole(value: string | undefined): CandidateRecordPresidentialRole | null {
  const trimmed = value?.trim();
  return trimmed === "president" || trimmed === "vice_president" ? trimmed : null;
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(
      STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
      STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
      "0",
      { MKSTREAM: true }
    );
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

async function reclaimPendingEntries(
  redis: ReturnType<typeof createClient>,
  consumerName: string,
  batchSize: number
): Promise<Array<{ id: string; message: Record<string, string> }>> {
  const reclaimed: Array<{ id: string; message: Record<string, string> }> = [];
  let cursor = "0-0";

  for (let i = 0; i < RECLAIM_MAX_BATCHES; i += 1) {
    const claim = await redis.xAutoClaim(
      STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
      STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
      consumerName,
      RECLAIM_MIN_IDLE_MS,
      cursor,
      { COUNT: batchSize }
    );
    cursor = claim.nextId;
    if (!claim.messages || claim.messages.length === 0) {
      break;
    }

    reclaimed.push(
      ...claim.messages
        .filter((entry): entry is NonNullable<typeof entry> => entry !== null)
        .map((entry) => ({ id: entry.id, message: entry.message as Record<string, string> }))
    );
  }

  return reclaimed;
}

async function getDeliveryCount(
  redis: ReturnType<typeof createClient>,
  messageId: string
): Promise<number | null> {
  try {
    const raw = await redis.sendCommand([
      "XPENDING",
      STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
      STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
      messageId,
      messageId,
      "1",
    ]);
    if (!Array.isArray(raw) || raw.length === 0) {
      return null;
    }
    const first = raw[0];
    if (!Array.isArray(first) || first.length < 4) {
      return null;
    }
    const deliveriesValue = first[3];
    const deliveries =
      typeof deliveriesValue === "number"
        ? deliveriesValue
        : Number.parseInt(String(deliveriesValue), 10);
    return Number.isFinite(deliveries) ? deliveries : null;
  } catch {
    return null;
  }
}

async function parkMessage(
  redis: ReturnType<typeof createClient>,
  entry: { id: string; message: Record<string, string> },
  reason: string,
  deliveryCount: number | null
): Promise<void> {
  await redis.xAdd(STAGING_CANDIDATE_RECORD_REJECTED_STREAM, "*", {
    reason,
    delivery_count: deliveryCount === null ? "" : String(deliveryCount),
    original_stream_id: entry.id,
    candidate_id: entry.message.candidate_id ?? "",
    election_id: entry.message.election_id ?? "",
    context_type: entry.message.context_type ?? "",
    presidential_cycle_id: entry.message.presidential_cycle_id ?? "",
    presidential_role: entry.message.presidential_role ?? "",
    item_type: entry.message.item_type ?? "",
    run_id: entry.message.run_id ?? "",
  });
  await redis.xAck(STAGING_CANDIDATE_RECORD_DRAFT_STREAM, STAGING_CANDIDATE_RECORD_ENRICHER_GROUP, entry.id);
}

export async function runCandidateRecordEnricher(options: EnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `candidate_record_enricher_${process.pid}_${Date.now()}`;
  const recordsConfig = buildCandidateRecordsConfigFromEnv();
  const repairConfig = buildCandidateRecordSourcesRepairConfigFromEnv();
  const areasConfig = buildCandidateRecordAreasConfigFromEnv();

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      const lifecycleResults: Array<Awaited<ReturnType<typeof runCandidateRecordsSearchLifecycle>>> = [];
      const stats: CandidateRecordEnricherBatchStats = {
        entries_total: entries.length,
        acked_count: 0,
        retried_count: 0,
        parked_count: 0,
        invalid_item_skips: 0,
        missing_context_skips: 0,
        label_validation_rejected_count: 0,
        dropped_transient_count: 0,
        dropped_permanent_count: 0,
        repaired_verified_count: 0,
        unresolved_after_repair_count: 0,
      };

      for (const entry of entries) {
        const candidateId = entry.message.candidate_id;
        const electionId = entry.message.election_id;
        const contextType = parseCandidateRecordContextType(entry.message.context_type);
        const presidentialCycleId = (entry.message.presidential_cycle_id ?? "").trim();
        const presidentialRole = parsePresidentialRole(entry.message.presidential_role);
        const itemType = entry.message.item_type;
        const runId = (entry.message.run_id ?? "").trim();
        const contextLabel =
          contextType === "presidential_cycle"
            ? `presidential_cycle_id=${presidentialCycleId || "unknown"} role=${presidentialRole ?? "unknown"}`
            : `election_id=${electionId ?? "unknown"}`;
        const presidentialDisabled =
          contextType === "presidential_cycle" && !isPresidentialElectionsEnabled();

        try {
          if (presidentialDisabled) {
            await redis.xAck(
              STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
              STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
              entry.id
            );
            stats.acked_count += 1;
            console.log(
              `candidate-record enricher skipped disabled presidential draft candidate_id=${candidateId ?? "unknown"} ${contextLabel}`
            );
            continue;
          }

          const deliveryCount = await getDeliveryCount(redis, entry.id);
          if (deliveryCount !== null && deliveryCount >= MAX_DELIVERY_ATTEMPTS) {
            await parkMessage(
              redis,
              entry,
              `max delivery attempts exceeded (${MAX_DELIVERY_ATTEMPTS})`,
              deliveryCount
            );
            console.warn(
              `candidate-record enricher parked stream_id=${entry.id} candidate_id=${candidateId ?? "unknown"} ${contextLabel} after ${deliveryCount} deliveries`
            );
            stats.parked_count += 1;
            continue;
          }

          const hasRequiredContext =
            contextType === "presidential_cycle"
              ? presidentialCycleId.length > 0 && presidentialRole !== null
              : Boolean(electionId);
          if (!candidateId || !hasRequiredContext || itemType !== STAGING_ITEM_TYPE_CANDIDATE_RECORD) {
            await redis.xAck(
              STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
              STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
              entry.id
            );
            stats.acked_count += 1;
            stats.invalid_item_skips += 1;
            continue;
          }

          const context =
            contextType === "presidential_cycle"
              ? await loadCandidatePresidentialCycleOfficeContext(
                  pool,
                  candidateId,
                  presidentialCycleId,
                  presidentialRole!
                )
              : await loadCandidateElectionOfficeContext(pool, candidateId, electionId ?? "");
          if (!context) {
            await redis.xAck(
              STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
              STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
              entry.id
            );
            stats.acked_count += 1;
            stats.missing_context_skips += 1;
            continue;
          }
          const lifecycleResult = await runCandidateRecordsSearchLifecycle(
            pool,
            {
              candidateId,
              asOf: new Date(),
              ignoreCooldown: contextType === "presidential_cycle",
            },
            async ({ candidateId: claimedCandidateId, window }): Promise<CandidateRecordsSearchMetrics> => {
              const discovered = await enrichCandidateRecords(
                {
                  candidateDisplayName: context.candidateDisplayName,
                  knownCurrentOffice: context.currentOffice,
                  hasHeldPublicOffice: context.hasHeldPublicOffice,
                  districtName: context.districtName,
                  districtType: context.districtType,
                  state: context.state,
                  electionDate: context.electionDate,
                  officialBallotTitle: context.officialBallotTitle,
                  electionStage: context.electionStage,
                  senateClass: context.senateClass,
                  termEndYear: context.termEndYear,
                  discoveryContestFamily: context.discoveryContestFamily,
                  sinceDate: window.sinceDate,
                },
                recordsConfig
              );

              if (!discovered.ok) {
                throw new Error(
                  `candidate record discovery failed (${discovered.errorCode}): ${discovered.reason}`
                );
              }

              const discoveredTotalCount =
                discovered.records.length + discovered.droppedRecords.length;
              let transientDropCount = discovered.droppedRecords.filter(
                (item) => item.failureType === "transient"
              ).length;
              let permanentDropCount = discovered.droppedRecords.length - transientDropCount;
              let repairCallFailedRetryable = false;
              let dropStatsRecorded = false;
              const recordDropStats = (): void => {
                if (dropStatsRecorded) {
                  return;
                }
                stats.dropped_transient_count += transientDropCount;
                stats.dropped_permanent_count += permanentDropCount;
                dropStatsRecorded = true;
              };
              if (discovered.droppedRecords.length > 0) {
                const droppedPreview = discovered.droppedRecords
                  .slice(0, 5)
                  .map((item) => `${item.record.description} (${item.record.source_url}): ${item.reason}`)
                  .join("; ");
                const droppedSuffix =
                  discovered.droppedRecords.length > 5
                    ? ` (+${discovered.droppedRecords.length - 5} more)`
                    : "";
                console.warn(
                  `candidate-record enricher found repairable bad rows candidate_id=${claimedCandidateId} count=${discovered.droppedRecords.length}; attempting repair: ${droppedPreview}${droppedSuffix}`
                );
              }

              let insertedCount = 0;
              let dedupedCount = 0;
              const recordsForTagging = [...discovered.records];
              const persistedByIdentity = new Map<string, string>();

              if (discovered.records.length > 0 && contextType !== "presidential_cycle") {
                const firstPassUpsert = await upsertCandidateRecordsWithNotificationEvents({
                  pool,
                  records: toCandidateRecordUpsertInput(claimedCandidateId, discovered.records),
                });
                insertedCount += firstPassUpsert.inserted;
                dedupedCount += firstPassUpsert.updated;
                for (const [key, id] of firstPassUpsert.recordIdsByIdentityKey) {
                  persistedByIdentity.set(key, id);
                }
              }

              if (discovered.droppedRecords.length > 0) {
                const blockedUrls = [
                  ...new Set(
                    discovered.droppedRecords
                      .filter((item) => item.failureKind === "source_url")
                      .map((item) => item.record.source_url)
                  ),
                ];
                const repair = await enrichCandidateRecordSourcesRepair(
                  {
                    candidateDisplayName: context.candidateDisplayName,
                    districtName: context.districtName,
                    districtType: context.districtType,
                    state: context.state,
                    electionDate: context.electionDate,
                    officialBallotTitle: context.officialBallotTitle,
                    electionStage: context.electionStage,
                    senateClass: context.senateClass,
                    termEndYear: context.termEndYear,
                    blockedUrls,
                    badRecords: discovered.droppedRecords.map((item, badIndex) => ({
                      badIndex,
                      description: item.record.description,
                      sourceUrl: item.record.source_url,
                      eventDate: item.record.event_date,
                      failureReason: item.reason,
                    })),
                  },
                  repairConfig
                );

                if (!repair.ok) {
                  repairCallFailedRetryable = repair.retryable;
                  console.warn(
                    `candidate-record enricher source-repair failed candidate_id=${claimedCandidateId}: ${repair.errorCode} ${repair.reason}`
                  );
                } else {
                  const badByIndex = new Map(
                    discovered.droppedRecords.map((item, idx) => [idx, item] as const)
                  );
                  const blockedUrlSet = new Set(
                    blockedUrls
                      .map((url) => normalizeHttpUrl(url))
                      .filter((url): url is string => typeof url === "string")
                  );
                  const repairedVerifiedRecords: typeof discovered.records = [];
                  const unresolvedDetails: Array<{
                    message: string;
                    failureType: "transient" | "permanent";
                  }> = [];

                  for (const suggestion of repair.repairs) {
                    const originalBad = badByIndex.get(suggestion.bad_index);
                    if (!originalBad) {
                      continue;
                    }

                    const normalizedRepairUrl = normalizeHttpUrl(suggestion.source_url);
                    if (!normalizedRepairUrl) {
                      unresolvedDetails.push({
                        message: `bad_index=${suggestion.bad_index}: invalid source_url format`,
                        failureType: "permanent",
                      });
                      continue;
                    }

                    if (blockedUrlSet.has(normalizedRepairUrl)) {
                      unresolvedDetails.push({
                        message: `bad_index=${suggestion.bad_index}: reused blocked URL ${normalizedRepairUrl}`,
                        failureType: "permanent",
                      });
                      continue;
                    }

                    const verification = await verifyHttpUrlReachability(normalizedRepairUrl, {
                      timeoutMs: Math.min(recordsConfig.timeoutMs, 8_000),
                      allowStatusCodes: [403],
                    });
                    if (!verification.ok) {
                      const failureType = classifyCitationVerificationFailure(verification.reason);
                      unresolvedDetails.push({
                        message: `bad_index=${suggestion.bad_index}: ${verification.reason} (${failureType})`,
                        failureType,
                      });
                      continue;
                    }

                    repairedVerifiedRecords.push({
                      description: suggestion.description,
                      source_url: verification.finalUrl,
                      event_date: suggestion.event_date,
                    });
                    if (originalBad.failureType === "transient") {
                      transientDropCount = Math.max(0, transientDropCount - 1);
                    } else {
                      permanentDropCount = Math.max(0, permanentDropCount - 1);
                    }
                  }

                  if (repair.noReplacementIndexes.length > 0) {
                    for (const index of repair.noReplacementIndexes) {
                      unresolvedDetails.push({
                        message: `bad_index=${index}: no_replacement`,
                        failureType: "permanent",
                      });
                    }
                  }

                  if (repairedVerifiedRecords.length > 0) {
                    if (contextType !== "presidential_cycle") {
                      const secondPassUpsert = await upsertCandidateRecordsWithNotificationEvents({
                        pool,
                        records: toCandidateRecordUpsertInput(claimedCandidateId, repairedVerifiedRecords),
                      });
                      insertedCount += secondPassUpsert.inserted;
                      dedupedCount += secondPassUpsert.updated;
                      for (const [key, id] of secondPassUpsert.recordIdsByIdentityKey) {
                        persistedByIdentity.set(key, id);
                      }
                    }
                    recordsForTagging.push(...repairedVerifiedRecords);
                    stats.repaired_verified_count += repairedVerifiedRecords.length;
                  }

                  if (unresolvedDetails.length > 0) {
                    stats.unresolved_after_repair_count += unresolvedDetails.length;

                    const preview = unresolvedDetails
                      .slice(0, 5)
                      .map((item) => item.message)
                      .join("; ");
                    const suffix =
                      unresolvedDetails.length > 5
                        ? ` (+${unresolvedDetails.length - 5} more)`
                        : "";
                    console.warn(
                      `candidate-record enricher dropped unresolved repaired records candidate_id=${claimedCandidateId}: ${preview}${suffix}`
                    );
                  }
                }
              }

              if (recordsForTagging.length === 0) {
                recordDropStats();
                const outcomeDecision = decideCandidateRecordOutcome({
                  discoveredTotalCount,
                  persistedCount: 0,
                  transientDropCount,
                  permanentDropCount,
                  repairCallFailedRetryable,
                });
                if (outcomeDecision.shouldRetry) {
                  throw new Error(
                    `candidate record run unresolved with transient-only failures (${outcomeDecision.reason}): discovered=${discoveredTotalCount} transient_dropped=${transientDropCount} permanent_dropped=${permanentDropCount}`
                  );
                }

                console.log(
                  `candidate-record enricher completed zero-persist run candidate_id=${claimedCandidateId} reason=${outcomeDecision.reason} discovered=${discoveredTotalCount} transient_dropped=${transientDropCount} permanent_dropped=${permanentDropCount}`
                );
                return {
                  discovered_count: discoveredTotalCount,
                  inserted_count: insertedCount,
                  deduped_count: dedupedCount,
                  tagged_specific_count: 0,
                  tagged_general_count: 0,
                };
              }
              recordDropStats();

              const allowedAreas = context.officeId
                ? await loadAllowedResearchAreasForOfficeId(pool, context.officeId)
                : await loadAllResearchAreas(pool);
              if (allowedAreas.length === 0) {
                throw new Error("no allowed research areas found for candidate record labeling");
              }
              if (!context.officeId) {
                console.log(
                  `candidate-record enricher using all research areas because office_id is null candidate_id=${claimedCandidateId} ${contextLabel}`
                );
              }
              const allowedSlugs = [...new Set(allowedAreas.map((row) => row.slug))];

              const areaLabels = await enrichCandidateRecordAreas(
                {
                  candidateDisplayName: context.candidateDisplayName,
                  districtName: context.districtName,
                  districtType: context.districtType,
                  state: context.state,
                  electionDate: context.electionDate,
                  officialBallotTitle: context.officialBallotTitle,
                  electionStage: context.electionStage,
                  senateClass: context.senateClass,
                  termEndYear: context.termEndYear,
                  allowedResearchAreaSlugs: allowedSlugs,
                  records: recordsForTagging.map((record) => ({
                    description: record.description,
                    sourceUrl: record.source_url,
                    eventDate: record.event_date,
                  })),
                },
                areasConfig
              );

              if (!areaLabels.ok) {
                throw new Error(
                  `candidate record area labeling failed (${areaLabels.errorCode}): ${areaLabels.reason}`
                );
              }

              let taggedGeneralCount = 0;
              let taggedSpecificCount = 0;
              if (contextType === "presidential_cycle") {
                try {
                  const replacement = await replacePresidentialCandidateRecordsAtomically({
                    pool,
                    candidateId: claimedCandidateId,
                    recordsForTagging,
                    labels: areaLabels.labels,
                    allowedSlugs,
                    allowedAreas,
                  });
                  insertedCount += replacement.insertedCount;
                  dedupedCount += replacement.dedupedCount;
                  taggedGeneralCount = replacement.taggedGeneralCount;
                  taggedSpecificCount = replacement.taggedSpecificCount;
                  if (replacement.deletedCount > 0) {
                    console.log(
                      `candidate-record enricher atomically replaced presidential records candidate_id=${claimedCandidateId} deleted=${replacement.deletedCount} inserted=${replacement.insertedCount} deduped=${replacement.dedupedCount}`
                    );
                  }
                } catch (error) {
                  if (error instanceof CandidateRecordLabelValidationError) {
                    stats.label_validation_rejected_count += error.failureCount;
                  }
                  throw error;
                }
              } else {
                const labelsForValidation = buildLabelsForValidation({
                  labels: areaLabels.labels,
                  recordsForTagging,
                  persistedByIdentity,
                  candidateId: claimedCandidateId,
                });
                const validation = validateCandidateRecordAreaLabels(
                  labelsForValidation,
                  new Set(allowedSlugs)
                );
                if (!validation.ok) {
                  stats.label_validation_rejected_count += validation.failures.length;
                  const reason = validation.failures.map((failure) => failure.reason).join("; ");
                  throw new Error(`candidate record label validation failed: ${reason}`);
                }

                const researchAreaIdBySlug = new Map(allowedAreas.map((area) => [area.slug, area.id]));
                await upsertCandidateRecordAreaTags(pool, validation.normalized, researchAreaIdBySlug);

                taggedGeneralCount = validation.normalized.filter(
                  (label) => label.researchAreaSlug === "general"
                ).length;
                taggedSpecificCount = validation.normalized.length - taggedGeneralCount;
              }

              return {
                discovered_count: discoveredTotalCount,
                inserted_count: insertedCount,
                deduped_count: dedupedCount,
                tagged_specific_count: taggedSpecificCount,
                tagged_general_count: taggedGeneralCount,
              };
            }
          );

          lifecycleResults.push(lifecycleResult);

          if (lifecycleResult.status === "completed") {
            console.log(
              `candidate-record enricher completed candidate_id=${candidateId} ${contextLabel} window_mode=${lifecycleResult.window.mode} since=${lifecycleResult.window.sinceDate ?? "full"} discovered=${lifecycleResult.metrics.discovered_count} inserted=${lifecycleResult.metrics.inserted_count} deduped=${lifecycleResult.metrics.deduped_count} tagged_specific=${lifecycleResult.metrics.tagged_specific_count} tagged_general=${lifecycleResult.metrics.tagged_general_count}`
            );
          } else {
            console.log(
              `candidate-record enricher skipped candidate_id=${candidateId} ${contextLabel} reason=${lifecycleResult.reason}`
            );
          }

          if (runId.length > 0) {
            const markerKey = buildCandidateRecordRunProcessedMarkerKey(runId);
            await redis.set(markerKey, lifecycleResult.status, {
              EX: CANDIDATE_RECORD_RUN_PROCESSED_MARKER_TTL_SECONDS,
            });
          }

          await redis.xAck(
            STAGING_CANDIDATE_RECORD_DRAFT_STREAM,
            STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
            entry.id
          );
          stats.acked_count += 1;
        } catch (error) {
          const reason = toReason(error);
          console.warn(
            `candidate-record enricher retrying candidate_id=${candidateId ?? "unknown"} ${contextLabel}: ${reason}`
          );
          stats.retried_count += 1;
          // Leave unacked so reclaim retries.
        }
      }

      if (lifecycleResults.length > 0) {
        const summary = summarizeCandidateRecordsLifecycleResults(lifecycleResults);
        console.log(
          `candidate-record enricher summary claimed=${summary.claimed_count} skipped=${summary.skipped_cooldown_or_claim_count} discovered=${summary.discovered_count} inserted=${summary.inserted_count} deduped=${summary.deduped_count} tagged_specific=${summary.tagged_specific_count} tagged_general=${summary.tagged_general_count}`
        );
      }
      console.log(
        `candidate-record enricher batch_stats entries=${stats.entries_total} acked=${stats.acked_count} retried=${stats.retried_count} parked=${stats.parked_count} invalid_item_skips=${stats.invalid_item_skips} missing_context_skips=${stats.missing_context_skips} label_validation_rejected=${stats.label_validation_rejected_count} dropped_transient=${stats.dropped_transient_count} dropped_permanent=${stats.dropped_permanent_count} repaired_verified=${stats.repaired_verified_count} unresolved_after_repair=${stats.unresolved_after_repair_count}`
      );
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_CANDIDATE_RECORD_ENRICHER_GROUP,
        consumerName,
        [{ key: STAGING_CANDIDATE_RECORD_DRAFT_STREAM, id: ">" }],
        { COUNT: batchSize, BLOCK: blockMs }
      );

      if (!batches || batches.length === 0) {
        if (once) {
          break;
        }
        continue;
      }

      for (const batch of batches) {
        await handleEntries(
          batch.messages.map((message) => ({
            id: message.id,
            message: message.message as Record<string, string>,
          }))
        );
      }

      if (once) {
        break;
      }
    } while (true);
  } finally {
    try {
      await redis.quit();
    } catch (error) {
      console.error("candidate-record enricher cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("candidate-record enricher cleanup warning (pool.end):", toReason(error));
    }
  }
}
