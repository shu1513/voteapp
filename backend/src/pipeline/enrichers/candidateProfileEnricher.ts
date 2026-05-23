import { Pool, type PoolClient } from "pg";
import { createClient } from "redis";

import {
  buildCandidateProfileConfigFromEnv,
  enrichCandidateProfile,
} from "../../ai/enrichCandidateProfile.js";
import { resolveIncludePartyForCandidateContest } from "../../ai/candidatePartisanship.js";
import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
  STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
  STAGING_CANDIDATE_PROFILE_REJECTED_STREAM,
  STAGING_ITEM_TYPE_CANDIDATE_PROFILE,
} from "../../config/electionsPipeline.js";
import type { CandidateProfilePayload } from "../../contracts/candidateProfilePayloadContract.js";
import {
  hasNormalizedIntersection,
  normalizeCandidateName,
  normalizeOptionalUrl,
  normalizeTwitterHandle,
  splitDisplayNameToFirstLast,
} from "../../utils/candidateIdentity.js";

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
  is_partisan: boolean | null;
  sources: unknown;
};

type ExistingCandidateRow = {
  id: string;
  first_name: string;
  last_name: string;
  date_of_birth: string | null;
  twitter_handle: string | null;
  linkedin_url: string | null;
  official_website_url: string | null;
  fec_ids: unknown;
  state_filing_ids: unknown;
  state: string;
};

const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;
const MAX_SEED_URLS = 8;
const MAX_DELIVERY_ATTEMPTS = 8;

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

function normalizeIdList(values: readonly string[] | undefined): string[] {
  return (values ?? [])
    .map((value) => value.trim().toLowerCase())
    .filter((value) => value.length > 0);
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
        e.is_partisan,
        e.sources
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
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

async function loadSameNameCandidates(
  pool: Pool,
  profile: CandidateProfilePayload,
  state: string
): Promise<ExistingCandidateRow[]> {
  const result = await pool.query<ExistingCandidateRow>(
    `
      SELECT
        id,
        first_name,
        last_name,
        date_of_birth::text AS date_of_birth,
        twitter_handle,
        linkedin_url,
        official_website_url,
        fec_ids,
        state_filing_ids,
        state
      FROM public.candidates
      WHERE deleted_at IS NULL
        AND lower(first_name) = lower($1)
        AND lower(last_name) = lower($2)
        AND state = $3
    `,
    [profile.first_name, profile.last_name, state]
  );

  return result.rows;
}

function hasAtLeastOneHardIdentifier(profile: CandidateProfilePayload): boolean {
  const hasFec = (profile.fec_ids?.length ?? 0) > 0;
  const hasStateFiling = (profile.state_filing_ids?.length ?? 0) > 0;
  const hasOfficialWebsite = Boolean(normalizeOptionalUrl(profile.official_website_url));
  return Boolean(
    profile.date_of_birth ||
      profile.twitter_handle ||
      profile.linkedin_url ||
      hasOfficialWebsite ||
      hasFec ||
      hasStateFiling
  );
}

function matchesByHardIdentifier(profile: CandidateProfilePayload, row: ExistingCandidateRow): boolean {
  if (profile.date_of_birth && row.date_of_birth && profile.date_of_birth === row.date_of_birth) {
    return true;
  }

  if (profile.twitter_handle && row.twitter_handle) {
    const normalizedProfileHandle = normalizeTwitterHandle(profile.twitter_handle);
    const normalizedRowHandle = normalizeTwitterHandle(row.twitter_handle);
    if (
      normalizedProfileHandle &&
      normalizedRowHandle &&
      normalizedProfileHandle === normalizedRowHandle
    ) {
      return true;
    }
  }

  if (profile.linkedin_url && row.linkedin_url) {
    if (normalizeOptionalUrl(profile.linkedin_url) === normalizeOptionalUrl(row.linkedin_url)) {
      return true;
    }
  }

  if (profile.official_website_url && row.official_website_url) {
    if (normalizeOptionalUrl(profile.official_website_url) === normalizeOptionalUrl(row.official_website_url)) {
      return true;
    }
  }

  const profileFecIds = normalizeIdList(profile.fec_ids);
  const rowFecIds = normalizeIdList(parseOptionalStringArray(row.fec_ids));
  if (profileFecIds.length > 0 && rowFecIds.length > 0 && hasNormalizedIntersection(profileFecIds, rowFecIds)) {
    return true;
  }

  const profileStateFilingIds = normalizeIdList(profile.state_filing_ids);
  const rowStateFilingIds = normalizeIdList(parseOptionalStringArray(row.state_filing_ids));
  if (
    profileStateFilingIds.length > 0 &&
    rowStateFilingIds.length > 0 &&
    hasNormalizedIntersection(profileStateFilingIds, rowStateFilingIds)
  ) {
    return true;
  }

  return false;
}

async function insertCandidate(
  client: PoolClient,
  profile: CandidateProfilePayload,
  state: string,
  rosterParty: string | undefined,
  includeParty: boolean
): Promise<string> {
  const storedParty = includeParty ? profile.party ?? rosterParty ?? "Unknown" : null;

  const insertResult = await client.query<{ id: string }>(
    `
      INSERT INTO public.candidates (
        display_name,
        first_name,
        last_name,
        date_of_birth,
        party,
        summary,
        twitter_handle,
        linkedin_url,
        fec_ids,
        state_filing_ids,
        state,
        official_website_url,
        last_researched
      )
      VALUES (
        $1,
        $2,
        $3,
        $4::date,
        $5,
        $6,
        $7,
        $8,
        $9::jsonb,
        $10::jsonb,
        $11,
        $12,
        now()
      )
      RETURNING id
    `,
    [
      profile.display_name,
      profile.first_name,
      profile.last_name,
      profile.date_of_birth ?? null,
      storedParty,
      profile.summary ?? null,
      profile.twitter_handle ?? null,
      profile.linkedin_url ?? null,
      profile.fec_ids ? JSON.stringify(profile.fec_ids) : null,
      profile.state_filing_ids ? JSON.stringify(profile.state_filing_ids) : null,
      state,
      profile.official_website_url ?? null,
    ]
  );

  const id = insertResult.rows[0]?.id;
  if (!id) {
    throw new Error("candidate insert returned no id");
  }

  return id;
}

async function upsertCandidateElection(
  client: PoolClient,
  candidateId: string,
  electionId: string,
  isIncumbent: boolean | undefined
): Promise<void> {
  await client.query(
    `
      INSERT INTO public.candidate_elections (
        candidate_id,
        election_id,
        is_incumbent,
        status
      )
      VALUES ($1, $2, $3, 'declared')
      ON CONFLICT (candidate_id, election_id) DO UPDATE
      SET is_incumbent = EXCLUDED.is_incumbent,
          status = EXCLUDED.status,
          updated_at = now()
    `,
    [candidateId, electionId, isIncumbent ?? false]
  );
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
        const electionId = entry.message.election_id;
        const itemType = entry.message.item_type;
        const candidateDisplayName = entry.message.candidate_display_name;
        const disambiguationHint = entry.message.disambiguation_hint?.trim() || undefined;
        const skipPerElectionNameDedupe = parseBooleanField(entry.message.skip_per_election_name_dedupe) === true;

        try {
          const deliveryCount = await getDeliveryCount(redis, entry.id);
          if (deliveryCount !== null && deliveryCount >= MAX_DELIVERY_ATTEMPTS) {
            await parkMessage(
              redis,
              entry,
              `max delivery attempts exceeded (${MAX_DELIVERY_ATTEMPTS})`,
              deliveryCount
            );
            console.warn(
              `candidate-profile enricher parked stream_id=${entry.id} election_id=${electionId ?? "unknown"} candidate=${candidateDisplayName ?? "unknown"} after ${deliveryCount} deliveries`
            );
            continue;
          }

          if (
            !electionId ||
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

          if (!skipPerElectionNameDedupe && (await electionAlreadyHasCandidateName(pool, electionId, candidateDisplayName))) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const election = await getElectionRow(pool, electionId);
          if (!election) {
            await redis.xAck(
              STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
              STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
              entry.id
            );
            continue;
          }

          const rosterParty = entry.message.roster_party?.trim() || undefined;
          const includeParty = resolveIncludePartyForCandidateContest({
            districtType: election.district_type,
            state: election.state,
            officialBallotTitle: election.official_ballot_title,
            electionIsPartisan: election.is_partisan,
          });
          const effectiveRosterParty = includeParty ? rosterParty : undefined;
          const rosterIncumbent = parseBooleanField(entry.message.roster_is_incumbent);
          const messageSeedUrls = parseSeedUrls(entry.message.seed_urls);
          const electionSeedUrls = parseSeedUrls(election.sources);

          const aiResult = await enrichCandidateProfile(
            {
              candidateDisplayName,
              districtName: election.district_name,
              districtType: election.district_type,
              state: election.state,
              electionDate: election.election_date,
              officialBallotTitle: election.official_ballot_title,
              electionIsPartisan: election.is_partisan,
              rosterParty: effectiveRosterParty,
              rosterIncumbent,
              disambiguationHint,
              seedUrls: mergeSeedUrls(messageSeedUrls, electionSeedUrls),
            },
            aiConfig
          );

          if (!aiResult.ok) {
            console.warn(
              `candidate-profile enricher retrying election_id=${electionId} candidate=${candidateDisplayName}: ${aiResult.errorCode} ${aiResult.reason}`
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

          const existingCandidates = await loadSameNameCandidates(pool, profile, election.state);

          let candidateId: string | null = null;
          if (hasAtLeastOneHardIdentifier(profile)) {
            const matched = existingCandidates.filter((row) => matchesByHardIdentifier(profile, row));
            if (matched.length === 1) {
              candidateId = matched[0]!.id;
            }
          }

          const client = await pool.connect();
          try {
            await client.query("BEGIN");

            if (!candidateId) {
              candidateId = await insertCandidate(
                client,
                profile,
                election.state,
                effectiveRosterParty,
                includeParty
              );
            }

            await upsertCandidateElection(client, candidateId, electionId, rosterIncumbent);
            await client.query("COMMIT");
          } catch (error) {
            await client.query("ROLLBACK");
            throw error;
          } finally {
            client.release();
          }

          await redis.xAck(
            STAGING_CANDIDATE_PROFILE_DRAFT_STREAM,
            STAGING_CANDIDATE_PROFILE_ENRICHER_GROUP,
            entry.id
          );
        } catch (error) {
          const reason = toReason(error);
          console.warn(
            `candidate-profile enricher retrying election_id=${electionId ?? "unknown"} candidate=${candidateDisplayName ?? "unknown"}: ${reason}`
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
