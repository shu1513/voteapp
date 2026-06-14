import { Pool } from "pg";
import { createClient } from "redis";

import {
  buildCandidateProfileConfigFromEnv,
  enrichCandidateProfile,
} from "../../ai/enrichCandidateProfile.js";
import { PRESIDENTIAL_PROFILE_AI_CANDIDATES } from "../../ai/aiCandidates.js";
import { resolveIncludePartyForCandidateContest } from "../../ai/candidatePartisanship.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
  STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
  STAGING_CANDIDATE_PROFILE_REJECTED_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
} from "../../config/electionsPipeline.js";
import { enqueueCandidateRecordDrafts } from "../candidates/candidateRecordDraftEmitter.js";
import {
  normalizeCandidateName,
  splitDisplayNameToFirstLast,
} from "../../utils/candidateIdentity.js";
import {
  findOrCreateCandidateFromProfile,
  hasAtLeastOneHardIdentifier,
} from "../candidates/candidateProfileIdentity.js";
import {
  findPresidentialCycleCandidateIdByFecId,
  markPresidentialCycleCandidateProfileResearched,
  markPresidentialCycleCandidateRunningMateProfileResearched,
  setPresidentialCycleCandidateRunningMate,
  upsertCandidateElection,
  upsertPresidentialCycleCandidate,
} from "../candidates/candidateProfileLinks.js";
import {
  loadPresidentialCycleProfileContext,
  type PresidentialCycleProfileContext,
} from "../presidential/presidentialProfileContext.js";

type EnricherOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type ElectionRow = {
  id: string;
  state: string;
  district_name: string;
  district_type: string;
  election_date: string;
  official_ballot_title: string;
  election_stage: string | null;
  senate_class: string | null;
  term_end_year: string | null;
  is_partisan: boolean | null;
  sources: unknown;
};

type CandidateProfileDraftContextType = "election" | "presidential_cycle";
type PresidentialProfileDraftRole = "president" | "vice_president";

type CandidateProfileResolvedContext =
  | {
      type: "election";
      contextId: string;
      state: string;
      districtName: string;
      districtType: string;
      electionDate: string;
      officialBallotTitle: string;
      electionStage: string | null;
      senateClass: string | null;
      termEndYear: string | null;
      electionIsPartisan: boolean | null;
      includeParty: boolean;
      rosterParty: string | undefined;
      rosterIncumbent: boolean | undefined;
      seedUrls: readonly string[];
    }
  | {
      type: "presidential_cycle";
      contextId: string;
      state: "US";
      districtName: "United States";
      districtType: "presidential";
      electionDate: string | null;
      officialBallotTitle: string;
      electionStage: "primary" | "general";
      senateClass: null;
      termEndYear: null;
      electionIsPartisan: true;
      includeParty: true;
      rosterParty: string;
      rosterIncumbent: undefined;
      seedUrls: readonly string[];
    };

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const MAX_SEED_URLS = 8;
const MAX_DELIVERY_ATTEMPTS = 8;

class ParkCandidateProfileDraftError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ParkCandidateProfileDraftError";
  }
}

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function parseSeedUrls(raw: unknown): string[] {
  if (typeof raw === "string") {
    try {
      return parseSeedUrls(JSON.parse(raw));
    } catch {
      return [];
    }
  }

  if (!Array.isArray(raw)) {
    return [];
  }

  const urls: string[] = [];
  for (const item of raw) {
    if (typeof item !== "string") {
      continue;
    }
    const trimmed = item.trim();
    if (trimmed.length === 0) {
      continue;
    }
    urls.push(trimmed);
  }

  return [...new Set(urls)].slice(0, MAX_SEED_URLS);
}

function mergeSeedUrls(...lists: Array<readonly string[] | undefined>): string[] {
  const merged: string[] = [];
  const seen = new Set<string>();
  for (const list of lists) {
    for (const item of list ?? []) {
      const trimmed = item.trim();
      if (trimmed.length === 0 || seen.has(trimmed)) {
        continue;
      }
      seen.add(trimmed);
      merged.push(trimmed);
      if (merged.length >= MAX_SEED_URLS) {
        return merged;
      }
    }
  }
  return merged;
}

function parseOptionalStringArray(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function parseSerializedStringArray(raw: string | undefined): string[] {
  if (!raw || raw.trim().length === 0) {
    return [];
  }
  try {
    return parseOptionalStringArray(JSON.parse(raw));
  } catch {
    return [];
  }
}

function parseBooleanField(raw: string | undefined): boolean | undefined {
  if (raw === "true") {
    return true;
  }
  if (raw === "false") {
    return false;
  }
  return undefined;
}

function draftContextType(message: Record<string, string>): CandidateProfileDraftContextType {
  return message.context_type === "presidential_cycle" ? "presidential_cycle" : "election";
}

function draftContextId(message: Record<string, string>, contextType: CandidateProfileDraftContextType): string {
  return contextType === "presidential_cycle"
    ? (message.presidential_cycle_id ?? "").trim()
    : (message.election_id ?? "").trim();
}

function draftPresidentialRole(
  message: Record<string, string>,
  contextType: CandidateProfileDraftContextType
): PresidentialProfileDraftRole | null {
  if (contextType !== "presidential_cycle") {
    return null;
  }
  const role = (message.presidential_role ?? "").trim();
  if (role.length === 0) {
    return "president";
  }
  return role === "president" || role === "vice_president" ? role : null;
}

function officialBallotTitleForPresidentialRole(
  context: Extract<CandidateProfileResolvedContext, { type: "presidential_cycle" }>,
  role: PresidentialProfileDraftRole
): string {
  if (role === "president") {
    return context.officialBallotTitle;
  }
  const year = context.electionDate ? new Date(context.electionDate).getUTCFullYear() : null;
  const cycleLabel = context.officialBallotTitle.replace(/^President of the United States,\s*/i, "");
  return `Vice President of the United States, ${cycleLabel || (year ? `${year} presidential cycle` : "presidential cycle")}`;
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(
      STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
      STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
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
      STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
      STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
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
      STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
      STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
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
  await redis.xAdd(STAGING_CANDIDATE_PROFILE_REJECTED_STREAM, "*", {
    reason,
    delivery_count: deliveryCount === null ? "" : String(deliveryCount),
    original_stream_id: entry.id,
    election_id: entry.message.election_id ?? "",
    context_type: entry.message.context_type ?? "",
    presidential_cycle_id: entry.message.presidential_cycle_id ?? "",
    presidential_role: entry.message.presidential_role ?? "",
    parent_presidential_candidate_fec_id: entry.message.parent_presidential_candidate_fec_id ?? "",
    candidate_display_name: entry.message.candidate_display_name ?? "",
    item_type: entry.message.item_type ?? "",
    run_id: entry.message.run_id ?? "",
  });
  await redis.xAck(STAGING_CANDIDATE_PROFILE_DRAFT_STREAM, STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP, entry.id);
}

async function getElectionRow(pool: Pool, electionId: string): Promise<ElectionRow | null> {
  const result = await pool.query<ElectionRow>(
    `
      SELECT
        e.id,
        d.state,
        d.name AS district_name,
        d.district_type,
        e.election_date::text AS election_date,
        e.official_ballot_title,
        e.election_stage::text AS election_stage,
        sm.senate_class,
        sm.term_end_year,
        e.is_partisan,
        e.sources
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      LEFT JOIN public.election_senate_metadata AS sm
        ON sm.election_id = e.id
      WHERE e.id = $1
        AND e.race_type = 'office'
      LIMIT 1
    `,
    [electionId]
  );

  return result.rows[0] ?? null;
}

async function electionAlreadyHasCandidateName(
  pool: Pool,
  electionId: string,
  displayName: string
): Promise<boolean> {
  const incomingName = splitDisplayNameToFirstLast(displayName);
  const incomingFirstLast = normalizeCandidateName(`${incomingName.firstName} ${incomingName.lastName}`);
  if (incomingFirstLast.length === 0) {
    return false;
  }

  const result = await pool.query<{
    first_name: string;
    last_name: string;
  }>(
    `
      SELECT c.first_name, c.last_name
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = $1
        AND c.deleted_at IS NULL
    `,
    [electionId]
  );

  for (const row of result.rows) {
    const existingFirstLast = normalizeCandidateName(`${row.first_name} ${row.last_name}`);
    if (existingFirstLast === incomingFirstLast) {
      return true;
    }
  }

  return false;
}

function effectivePresidentialParty(
  rosterParty: string | undefined,
  context: PresidentialCycleProfileContext
): string | undefined {
  const cycleParty = context.party?.trim();
  if (context.stage === "primary") {
    return cycleParty && cycleParty.length > 0 ? cycleParty : undefined;
  }
  return rosterParty ?? (cycleParty && cycleParty.length > 0 ? cycleParty : undefined);
}

async function resolveElectionDraftContext(input: {
  pool: Pool;
  electionId: string;
  rosterParty: string | undefined;
  rosterIncumbent: boolean | undefined;
  messageSeedUrls: readonly string[];
}): Promise<CandidateProfileResolvedContext | null> {
  const election = await getElectionRow(input.pool, input.electionId);
  if (!election) {
    return null;
  }

  const includeParty = resolveIncludePartyForCandidateContest({
    districtType: election.district_type,
    state: election.state,
    officialBallotTitle: election.official_ballot_title,
    electionIsPartisan: election.is_partisan,
  });
  const rosterParty = includeParty ? input.rosterParty : undefined;

  return {
    type: "election",
    contextId: input.electionId,
    state: election.state,
    districtName: election.district_name,
    districtType: election.district_type,
    electionDate: election.election_date,
    officialBallotTitle: election.official_ballot_title,
    electionStage: election.election_stage,
    senateClass: election.senate_class,
    termEndYear: election.term_end_year,
    electionIsPartisan: election.is_partisan,
    includeParty,
    rosterParty,
    rosterIncumbent: input.rosterIncumbent,
    seedUrls: mergeSeedUrls(input.messageSeedUrls, parseSeedUrls(election.sources)),
  };
}

async function resolvePresidentialCycleDraftContext(input: {
  pool: Pool;
  presidentialCycleId: string;
  rosterParty: string | undefined;
  messageSeedUrls: readonly string[];
}): Promise<CandidateProfileResolvedContext | null> {
  const context = await loadPresidentialCycleProfileContext(input.pool, input.presidentialCycleId);
  if (!context) {
    return null;
  }
  const rosterParty = effectivePresidentialParty(input.rosterParty, context);
  if (!rosterParty) {
    throw new Error(`presidential cycle profile draft is missing party for cycle ${input.presidentialCycleId}`);
  }

  return {
    type: "presidential_cycle",
    contextId: context.cycleId,
    state: context.state,
    districtName: context.districtName,
    districtType: context.districtType,
    electionDate: context.electionDate,
    officialBallotTitle: context.officialBallotTitle,
    electionStage: context.electionStage,
    senateClass: null,
    termEndYear: null,
    electionIsPartisan: context.electionIsPartisan,
    includeParty: true,
    rosterParty,
    rosterIncumbent: undefined,
    seedUrls: mergeSeedUrls(input.messageSeedUrls, context.seedUrls),
  };
}

export async function runCandidateProfileEnricher(options: EnricherOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `candidate_profile_enricher_${process.pid}_${Date.now()}`;
  const aiConfig = buildCandidateProfileConfigFromEnv();

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      for (const entry of entries) {
        const contextType = draftContextType(entry.message);
        const contextId = draftContextId(entry.message, contextType);
        const presidentialRole = draftPresidentialRole(entry.message, contextType);
        const contextLabel =
          contextType === "presidential_cycle"
            ? `presidential_cycle_id=${contextId || "unknown"}`
            : `election_id=${contextId || "unknown"}`;
        const itemType = entry.message.item_type;
        const candidateDisplayName = entry.message.candidate_display_name;
        const runId = entry.message.run_id ?? null;
        const disambiguationHint = entry.message.disambiguation_hint?.trim() || undefined;
        const skipPerElectionNameDedupe = parseBooleanField(entry.message.skip_per_election_name_dedupe) === true;
        const rosterFecIds = parseSerializedStringArray(entry.message.roster_fec_ids);
        const rosterStateFilingIds = parseSerializedStringArray(entry.message.roster_state_filing_ids);
        const parentPresidentialCandidateFecId =
          entry.message.parent_presidential_candidate_fec_id?.trim().toUpperCase() || undefined;
        let deliveryCount: number | null = null;

        try {
          deliveryCount = await getDeliveryCount(redis, entry.id);
          if (deliveryCount !== null && deliveryCount >= MAX_DELIVERY_ATTEMPTS) {
            await parkMessage(
              redis,
              entry,
              `max delivery attempts exceeded (${MAX_DELIVERY_ATTEMPTS})`,
              deliveryCount
            );
            console.warn(
              `candidate-profile enricher parked stream_id=${entry.id} ${contextLabel} candidate=${candidateDisplayName ?? "unknown"} after ${deliveryCount} deliveries`
            );
            continue;
          }

          if (contextType === "presidential_cycle" && presidentialRole === null) {
            await parkMessage(redis, entry, "invalid presidential_role for presidential profile draft", deliveryCount);
            continue;
          }

          if (
            !contextId ||
            !candidateDisplayName ||
            itemType !== STAGING_ITEM_TYPE_CANDIDATE_PROFILE
          ) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          if (contextType === "presidential_cycle" && presidentialRole === "president" && rosterFecIds.length === 0) {
            await parkMessage(
              redis,
              entry,
              "president profile draft requires at least one roster_fec_ids value",
              deliveryCount
            );
            continue;
          }

          if (
            contextType === "presidential_cycle" &&
            presidentialRole === "vice_president" &&
            !parentPresidentialCandidateFecId
          ) {
            await parkMessage(
              redis,
              entry,
              "vice president profile draft requires parent_presidential_candidate_fec_id",
              deliveryCount
            );
            continue;
          }

          if (
            contextType === "election" &&
            !skipPerElectionNameDedupe &&
            (await electionAlreadyHasCandidateName(pool, contextId, candidateDisplayName))
          ) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const rosterParty = entry.message.roster_party?.trim() || undefined;
          const rosterIncumbent = parseBooleanField(entry.message.roster_is_incumbent);
          const messageSeedUrls = parseSeedUrls(entry.message.seed_urls);
          const draftContext =
            contextType === "presidential_cycle"
              ? await resolvePresidentialCycleDraftContext({
                  pool,
                  presidentialCycleId: contextId,
                  rosterParty,
                  messageSeedUrls,
                })
              : await resolveElectionDraftContext({
                  pool,
                  electionId: contextId,
                  rosterParty,
                  rosterIncumbent,
                  messageSeedUrls,
                });
          if (!draftContext) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const profileInput = {
            candidateDisplayName,
            districtName: draftContext.districtName,
            districtType: draftContext.districtType,
            state: draftContext.state,
            electionDate: draftContext.electionDate,
            officialBallotTitle:
              draftContext.type === "presidential_cycle"
                ? officialBallotTitleForPresidentialRole(draftContext, presidentialRole ?? "president")
                : draftContext.officialBallotTitle,
            electionStage: draftContext.electionStage,
            senateClass: draftContext.senateClass,
            termEndYear: draftContext.termEndYear,
            electionIsPartisan: draftContext.electionIsPartisan,
            rosterParty: draftContext.rosterParty,
            rosterIncumbent: draftContext.rosterIncumbent,
            rosterFecIds,
            rosterStateFilingIds,
            disambiguationHint,
            seedUrls: draftContext.seedUrls,
            allowMissingFederalFecIds:
              draftContext.type === "presidential_cycle" && presidentialRole === "vice_president",
          };
          const aiResult =
            draftContext.type === "presidential_cycle"
              ? await enrichCandidateProfile(profileInput, aiConfig, PRESIDENTIAL_PROFILE_AI_CANDIDATES)
              : await enrichCandidateProfile(profileInput, aiConfig);

          if (!aiResult.ok) {
            console.warn(
              `candidate-profile enricher retrying ${contextLabel} candidate=${candidateDisplayName}: ${aiResult.errorCode} ${aiResult.reason}`
            );
            // Leave unacked so reclaim retries.
            continue;
          }

          const profile = aiResult.profile;
          if (skipPerElectionNameDedupe && !hasAtLeastOneHardIdentifier(profile)) {
            await parkMessage(
              redis,
              entry,
              "duplicate-name candidate profile lacks hard identifiers; skipped to avoid mismatched person write",
              deliveryCount
            );
            continue;
          }

          const client = await pool.connect();
          let candidateId: string;
          try {
            await client.query("BEGIN");

            const candidateResult = await findOrCreateCandidateFromProfile({
              client,
              profile,
              state: draftContext.state,
              rosterParty: draftContext.rosterParty,
              includeParty: draftContext.includeParty,
              allowCrossStateHardIdentifierMatch: draftContext.type === "presidential_cycle",
            });
            candidateId = candidateResult.candidateId;

            if (draftContext.type === "presidential_cycle") {
              if (presidentialRole === "vice_president") {
                const parentCandidateId = await findPresidentialCycleCandidateIdByFecId({
                  db: client,
                  cycleId: draftContext.contextId,
                  fecCandidateId: parentPresidentialCandidateFecId ?? "",
                });
                if (!parentCandidateId) {
                  throw new Error(
                    `parent presidential cycle candidate not found for FEC ID ${parentPresidentialCandidateFecId ?? ""}`
                  );
                }
                if (parentCandidateId === candidateId) {
                  throw new ParkCandidateProfileDraftError(
                    `vice president profile resolved to the parent presidential candidate for FEC ID ${parentPresidentialCandidateFecId ?? ""}`
                  );
                }
                await setPresidentialCycleCandidateRunningMate({
                  db: client,
                  cycleId: draftContext.contextId,
                  candidateId: parentCandidateId,
                  runningMateCandidateId: candidateId,
                });
                await markPresidentialCycleCandidateRunningMateProfileResearched({
                  db: client,
                  cycleId: draftContext.contextId,
                  candidateId: parentCandidateId,
                  runningMateCandidateId: candidateId,
                });
              } else {
                await upsertPresidentialCycleCandidate({
                  client,
                  cycleId: draftContext.contextId,
                  candidateId,
                  party: draftContext.rosterParty,
                  sources: profile.sources,
                });
                await markPresidentialCycleCandidateProfileResearched({
                  db: client,
                  cycleId: draftContext.contextId,
                  candidateId,
                });
              }
            } else {
              await upsertCandidateElection({
                client,
                candidateId,
                electionId: draftContext.contextId,
                isIncumbent: draftContext.rosterIncumbent,
              });
            }
            await client.query("COMMIT");
          } catch (error) {
            await client.query("ROLLBACK");
            throw error;
          } finally {
            client.release();
          }

          if (draftContext.type === "election") {
            await enqueueCandidateRecordDrafts(redis, [
              {
                candidateId,
                electionId: draftContext.contextId,
                runId,
              },
            ]);
          } else {
            await enqueueCandidateRecordDrafts(redis, [
              {
                contextType: "presidential_cycle",
                candidateId,
                presidentialCycleId: draftContext.contextId,
                presidentialRole: presidentialRole ?? "president",
                runId,
              },
            ]);
          }

          await redis.xAck(
            STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
            STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
            entry.id
          );
        } catch (error) {
          const reason = toReason(error);
          if (error instanceof ParkCandidateProfileDraftError) {
            await parkMessage(redis, entry, reason, deliveryCount);
            continue;
          }
          console.warn(
            `candidate-profile enricher retrying ${contextLabel} candidate=${candidateDisplayName ?? "unknown"}: ${reason}`
          );
          // Leave unacked so reclaim retries.
        }
      }
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
        consumerName,
        [{ key: STAGING_CANDIDATE_PROFILE_DRAFT_STREAM, id: ">" }],
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
      console.error("candidate-profile enricher cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("candidate-profile enricher cleanup warning (pool.end):", toReason(error));
    }
  }
}
