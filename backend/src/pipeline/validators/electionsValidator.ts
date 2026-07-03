import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import {
  STAGING_DRAFT_STREAM,
  STAGING_ELECTIONS_VALIDATOR_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_PENDING_STREAM,
  STAGING_REJECTED_STREAM,
  STAGING_VALIDATED_STREAM,
} from "../../config/electionsPipeline.js";
import {
  ELECTION_DRAFT_SCHEMA_VERSION,
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
} from "../../contracts/electionEnrichmentContract.js";
import { parseCanonicalElectionPayload } from "../../contracts/electionPayloadContract.js";
import type { ElectionDistrictType, ElectionEnrichedPayload, ElectionEntryPayload } from "../../types/election.js";
import { filterPresidentialElectionEntries } from "../../utils/presidentialOffice.js";
import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";

type ValidatorOptions = {
  once?: boolean;
  batchSize?: number;
  blockMs?: number;
};

type ValidationSeverity = "pass" | "soft_fail" | "hard_fail";

type ValidationResult = {
  severity: ValidationSeverity;
  reasons: string[];
};

type PresidentialEntryFilterResult = {
  payload: ElectionEnrichedPayload;
  removedTitles: string[];
};

type StagingRow = {
  ingest_key: string;
  payload: unknown;
  status: string;
  run_id: string | null;
  failure_debug: unknown;
  schema_version: string | null;
};

// Validation + DB writes can exceed tens of seconds; reclaim entries only after generous idle time.
const RECLAIM_MIN_IDLE_MS = 240_000;
const RECLAIM_MAX_BATCHES = 20;

function toReason(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 1000 ? `${message.slice(0, 997)}...` : message;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseSoftRetryCount(failureDebug: unknown): number {
  if (!isObjectRecord(failureDebug)) {
    return 0;
  }
  const raw = failureDebug.soft_retry_count;
  return typeof raw === "number" && Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : 0;
}

function normalize(text: string): string {
  return text.toLowerCase().replace(/\s+/g, " ").trim();
}

function hasAny(text: string, patterns: RegExp[]): boolean {
  return patterns.some((pattern) => pattern.test(text));
}

const STATE_UPPER_STRICT_MARKERS = [
  /\bstate senate\b/,
  /\bstate senator\b/,
  /\bsenate district\b/,
  /\bsenator district\b/,
  /\bsenator,\s*district\b/,
  /\bmember of the senate\b/,
  /\bmember of the state senate\b/,
  /\bsenator in the general assembly\b/,
  /\bupper chamber\b/,
];

const STATE_UPPER_MARKERS = [
  ...STATE_UPPER_STRICT_MARKERS,
  /\bmember of the legislature\b/,
  /\blegislature district\b/,
  /\blegislative district\b/,
  /\bstate legislature district\b/,
];

const STATE_LOWER_STRICT_MARKERS = [
  /\bstate house\b/,
  /\bstate representative\b/,
  /\bstate delegate\b/,
  /\bhouse delegate\b/,
  /\bhouse of representatives district\b/,
  /\brepresentative district\b/,
  /\bmember of the house of representatives\b/,
  /\bmember,\s*house of representatives\b/,
  /\bstate assembly\b/,
  /\bassembly district\b/,
  /\bassemblymember\b/,
  /\bassembly member\b/,
  /\bmember of the assembly\b/,
  /\bmember of the state assembly\b/,
  /\bhouse of delegates\b/,
  /\bdelegate district\b/,
  /\bmember of the house of delegates\b/,
  /\bmember,\s*house of delegates\b/,
  /\blower chamber\b/,
];

const STATE_LOWER_MARKERS = [
  ...STATE_LOWER_STRICT_MARKERS,
  /\brepresentative in the general assembly\b/,
  /\brepresentative in general assembly\b/,
  /\brepresentative in the general court\b/,
  /\brepresentative in general court\b/,
  /\bgeneral assembly district\b/,
  /\bmember of the general assembly\b/,
  /\bgeneral court district\b/,
  /\bmember of the general court\b/,
];

function currentUtcDateYmd(): string {
  return new Date().toISOString().slice(0, 10);
}

function utcDateYmdDaysAgo(daysAgo: number): string {
  const now = new Date();
  const utcMidnight = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const shifted = new Date(utcMidnight - daysAgo * 24 * 60 * 60 * 1000);
  return shifted.toISOString().slice(0, 10);
}

function isHardScopeMismatch(districtType: ElectionDistrictType, entry: ElectionEntryPayload): string | null {
  const titleText = normalize(entry.official_ballot_title);
  const scopeText = titleText;

  const usSenate = isUsSenateOfficeTitle(entry.official_ballot_title);
  const usHouse =
    /\bu\.?s\.?\s+house\b/.test(scopeText) ||
    /\bu\.?s\.?\s+house\s+of\s+representatives?\b/.test(scopeText) ||
    /\brepresentative in congress\b/.test(scopeText) ||
    /\brepresentative to congress\b/.test(scopeText) ||
    /\bcongressional district\b/.test(scopeText);
  const stateSenate = !usSenate && hasAny(scopeText, STATE_UPPER_STRICT_MARKERS);
  const stateHouse = !usHouse && hasAny(scopeText, STATE_LOWER_STRICT_MARKERS);
  const hasFederalMarker =
    usSenate ||
    usHouse ||
    /\bunited states\b/.test(scopeText) ||
    /\bcongress\b/.test(scopeText) ||
    /\bcongressional\b/.test(scopeText);
  const statewideStateLegislativeLike =
    !hasFederalMarker &&
    (hasAny(scopeText, STATE_UPPER_MARKERS) || hasAny(scopeText, STATE_LOWER_MARKERS));

  // Only use broad office-level mismatch signals for office races to avoid over-rejecting ballot-measure text.
  const governorLike =
    entry.race_type === "office" &&
    /\bgovernor\b|\blieutenant governor\b|\battorney general\b|\bsecretary of state\b/.test(scopeText);
  const countyLike =
    entry.race_type === "office" &&
    /\bcounty\b|\bsheriff\b|\bcounty commissioner\b|\bcounty clerk\b/.test(scopeText);
  const cityLike =
    entry.race_type === "office" &&
    /\bcity\b|\bmayor\b|\bcity council\b|\balderman\b/.test(scopeText);
  const schoolLike =
    entry.race_type === "office" &&
    /\bschool board\b|\bschool district\b|\bboard of education\b/.test(scopeText);

  if (districtType === "place" && (usSenate || usHouse || stateSenate || stateHouse || governorLike || countyLike || schoolLike)) {
    return "place scope contains clearly non-place race";
  }

  if (districtType === "county" && (usSenate || usHouse || stateSenate || stateHouse || governorLike || cityLike || schoolLike)) {
    return "county scope contains clearly non-county race";
  }

  if (districtType === "us_house" && (usSenate || stateSenate || stateHouse || governorLike || countyLike || cityLike || schoolLike)) {
    return "us_house scope contains clearly non-us_house race";
  }

  if (districtType === "state_upper" && (usSenate || usHouse || stateHouse || countyLike || cityLike || schoolLike)) {
    return "state_upper scope contains clearly non-state_upper race";
  }

  if (districtType === "state_lower" && (usSenate || usHouse || stateSenate || countyLike || cityLike || schoolLike)) {
    return "state_lower scope contains clearly non-state_lower race";
  }

  if (districtType.startsWith("school_") && (usSenate || usHouse || stateSenate || stateHouse || governorLike || countyLike || cityLike)) {
    return "school scope contains clearly non-school race";
  }

  if (
    districtType === "statewide" &&
    (countyLike ||
      cityLike ||
      schoolLike ||
      (entry.race_type === "office" && (usHouse || statewideStateLegislativeLike)))
  ) {
    return "statewide scope contains clearly non-statewide race";
  }

  return null;
}

function isSoftScopeAmbiguous(
  districtType: ElectionDistrictType,
  entry: ElectionEntryPayload,
  state: string
): string | null {
  const text = normalize(entry.official_ballot_title);

  if (
    districtType === "us_house" &&
    !hasAny(text, [
      /\bu\.?s\.?\s+house\b/,
      /\bu\.?s\.?\s+house\s+of\s+representatives?\b/,
      /\bu\.?s\.?\s+representative\b/,
      /\bunited states representative\b/,
      /\brepresentative in congress\b/,
      /\brepresentative to congress\b/,
      /\bcongressional district\b/,
    ])
  ) {
    return "us_house entry lacks clear us_house markers";
  }

  if (districtType === "state_upper" && !hasAny(text, STATE_UPPER_MARKERS)) {
    return "state_upper entry lacks clear state_upper markers";
  }

  if (districtType === "state_lower" && !hasAny(text, STATE_LOWER_MARKERS)) {
    return "state_lower entry lacks clear state_lower markers";
  }

  if (districtType === "county") {
    const countyMarkers = [/\bcounty\b/, /\bsheriff\b/, /\bcounty commissioner\b/, /\bcounty clerk\b/];
    // "Assessor" is a county office, but town/village/township assessors are municipal
    // contests that the city-oriented hard markers do not catch.
    if (!/\b(town|village|township|borough|municipal)\b/.test(text)) {
      countyMarkers.push(/\bassessor\b/);
    }
    // Superior courts are county trial courts in the states that elect their judges,
    // except Pennsylvania, where the Superior Court is a statewide appellate court.
    if (state.trim().toUpperCase() !== "PA") {
      countyMarkers.push(/\bsuperior court\b/);
    }
    if (!hasAny(text, countyMarkers)) {
      return "county entry lacks clear county markers";
    }
  }

  if (districtType === "place" && !hasAny(text, [/\bcity\b/, /\bmayor\b/, /\bcity council\b/, /\btown\b/, /\bvillage\b/])) {
    return "place entry lacks clear place markers";
  }

  if (districtType.startsWith("school_") && !hasAny(text, [/\bschool\b/, /\bboard of education\b/, /\bschool board\b/])) {
    return "school entry lacks clear school markers";
  }

  return null;
}

function ballotMeasureTitleQualityIssue(entry: ElectionEntryPayload): string | null {
  if (entry.race_type !== "ballot_measure") {
    return null;
  }

  const title = entry.official_ballot_title.trim();
  const isQuestionLike =
    title.includes("?") ||
    /\bbe adopted\b/i.test(title) ||
    /\bdo you\b/i.test(title);
  const isTooLong = title.length > 140;

  if (isQuestionLike || isTooLong) {
    return "ballot_measure title looks like question/description text instead of official measure label/title";
  }
  return null;
}

function filterPresidentialEntries(payload: ElectionEnrichedPayload): PresidentialEntryFilterResult {
  const result = filterPresidentialElectionEntries(payload.entries);

  return {
    payload:
      result.removedTitles.length === 0
        ? payload
        : {
            ...payload,
            entries: result.entries,
          },
    removedTitles: result.removedTitles,
  };
}

function validateScope(payload: ElectionEnrichedPayload): ValidationResult {
  const reasons: string[] = [];
  let severity: ValidationSeverity = "pass";
  const todayUtc = currentUtcDateYmd();
  const oldestAllowedDate = utcDateYmdDaysAgo(1);

  for (const entry of payload.entries) {
    if (entry.election_date < oldestAllowedDate) {
      return {
        severity: "hard_fail",
        reasons: [
          `election_date is too far in the past (${entry.election_date} < ${oldestAllowedDate}, UTC 1-day grace): ${entry.official_ballot_title}`,
        ],
      };
    }

    const hardReason = isHardScopeMismatch(payload.district_type, entry);
    if (hardReason) {
      return {
        severity: "hard_fail",
        reasons: [`${hardReason}: ${entry.official_ballot_title}`],
      };
    }

    const softReason = isSoftScopeAmbiguous(payload.district_type, entry, payload.state);
    if (softReason) {
      severity = "soft_fail";
      reasons.push(`${softReason}: ${entry.official_ballot_title}`);
    }

    const titleQualityReason = ballotMeasureTitleQualityIssue(entry);
    if (titleQualityReason) {
      severity = "soft_fail";
      reasons.push(`${titleQualityReason}: ${entry.official_ballot_title}`);
    }
  }

  return { severity, reasons };
}

async function ensureConsumerGroup(redis: ReturnType<typeof createClient>): Promise<void> {
  try {
    await redis.xGroupCreate(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, "0", { MKSTREAM: true });
  } catch (error) {
    const message = toReason(error);
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

async function getStagingRow(pool: Pool, ingestKey: string): Promise<StagingRow | null> {
  const result = await pool.query<StagingRow>(
    `
      SELECT ingest_key, payload, status, run_id, failure_debug, schema_version
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_ELECTION]
  );
  return result.rows[0] ?? null;
}

async function getStagingStatus(pool: Pool, ingestKey: string): Promise<string | null> {
  const result = await pool.query<{ status: string }>(
    `
      SELECT status
      FROM staging_items
      WHERE ingest_key = $1
        AND item_type = $2
    `,
    [ingestKey, STAGING_ITEM_TYPE_ELECTION]
  );
  return result.rows[0]?.status ?? null;
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
      STAGING_PENDING_STREAM,
      STAGING_ELECTIONS_VALIDATOR_GROUP,
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

export async function runElectionsValidator(options: ValidatorOptions = {}): Promise<void> {
  const { once = false, batchSize = 25, blockMs = 5000 } = options;
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });
  const consumerName = `elections_validator_${process.pid}_${Date.now()}`;

  try {
    await redis.connect();
    await ensureConsumerGroup(redis);

    const handleEntries = async (entries: Array<{ id: string; message: Record<string, string> }>): Promise<void> => {
      for (const entry of entries) {
        const ingestKey = entry.message.ingest_key;
        const itemType = entry.message.item_type;

        try {
          if (!ingestKey || itemType !== STAGING_ITEM_TYPE_ELECTION) {
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
            continue;
          }

          const row = await getStagingRow(pool, ingestKey);
          if (!row || row.status !== "pending" || row.schema_version !== ELECTION_ENRICHMENT_SCHEMA_VERSION) {
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
            continue;
          }

          const parsed = parseCanonicalElectionPayload(row.payload);
          if (!parsed.ok) {
            await pool.query(
              `
                UPDATE staging_items
                SET status = 'rejected',
                    reason = $2,
                    updated_at = now()
                WHERE ingest_key = $1
                  AND item_type = $3
              `,
              [ingestKey, `hard_fail: ${parsed.reason}`, STAGING_ITEM_TYPE_ELECTION]
            );
            await redis.xAdd(STAGING_REJECTED_STREAM, "*", {
              ingest_key: ingestKey,
              item_type: STAGING_ITEM_TYPE_ELECTION,
              run_id: row.run_id ?? "",
              reason: `hard_fail: ${parsed.reason}`,
            });
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
            continue;
          }

          const softRetryCount = parseSoftRetryCount(row.failure_debug);
          const presidentialFilter = filterPresidentialEntries(parsed.payload);
          const payloadForValidation = presidentialFilter.payload;
          if (presidentialFilter.removedTitles.length > 0 && payloadForValidation.entries.length === 0) {
            const reason = `presidential entries excluded from standard election discovery: ${presidentialFilter.removedTitles.join(", ")}`;
            if (softRetryCount === 0) {
              const failureDebug = {
                soft_retry_count: 1,
                validation_feedback: [reason],
                soft_retry_at: new Date().toISOString(),
              };

              await pool.query(
                `
                  UPDATE staging_items
                  SET status = 'pending',
                      reason = $2,
                      failure_debug = $3::jsonb,
                      schema_version = $4,
                      updated_at = now()
                  WHERE ingest_key = $1
                    AND item_type = $5
                `,
                [
                  ingestKey,
                  `soft_fail: ${reason}`,
                  JSON.stringify(failureDebug),
                  ELECTION_DRAFT_SCHEMA_VERSION,
                  STAGING_ITEM_TYPE_ELECTION,
                ]
              );

              await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
                ingest_key: ingestKey,
                item_type: STAGING_ITEM_TYPE_ELECTION,
                run_id: row.run_id ?? "",
              });

              await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
              continue;
            }

            const rejectReason = `soft_fail_final: ${reason}`;
            await pool.query(
              `
                UPDATE staging_items
                SET status = 'rejected',
                    reason = $2,
                    updated_at = now()
                WHERE ingest_key = $1
                  AND item_type = $3
              `,
              [ingestKey, rejectReason, STAGING_ITEM_TYPE_ELECTION]
            );
            await redis.xAdd(STAGING_REJECTED_STREAM, "*", {
              ingest_key: ingestKey,
              item_type: STAGING_ITEM_TYPE_ELECTION,
              run_id: row.run_id ?? "",
              reason: rejectReason,
            });
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
            continue;
          }

          const validation = validateScope(payloadForValidation);

          if (validation.severity === "hard_fail") {
            const reason = `hard_fail: ${validation.reasons.join("; ")}`;
            await pool.query(
              `
                UPDATE staging_items
                SET status = 'rejected',
                    reason = $2,
                    updated_at = now()
                WHERE ingest_key = $1
                  AND item_type = $3
              `,
              [ingestKey, reason, STAGING_ITEM_TYPE_ELECTION]
            );
            await redis.xAdd(STAGING_REJECTED_STREAM, "*", {
              ingest_key: ingestKey,
              item_type: STAGING_ITEM_TYPE_ELECTION,
              run_id: row.run_id ?? "",
              reason,
            });
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
            continue;
          }

          if (validation.severity === "soft_fail") {
            if (softRetryCount === 0) {
              const nextSoftRetryCount = softRetryCount + 1;
              const failureDebug = {
                soft_retry_count: nextSoftRetryCount,
                validation_feedback: validation.reasons,
                soft_retry_at: new Date().toISOString(),
              };

              await pool.query(
                `
                  UPDATE staging_items
                  SET status = 'pending',
                      reason = $2,
                      failure_debug = $3::jsonb,
                      schema_version = $4,
                      updated_at = now()
                  WHERE ingest_key = $1
                    AND item_type = $5
                `,
                [
                  ingestKey,
                  `soft_fail: ${validation.reasons.join("; ")}`,
                  JSON.stringify(failureDebug),
                  ELECTION_DRAFT_SCHEMA_VERSION,
                  STAGING_ITEM_TYPE_ELECTION,
                ]
              );

              await redis.xAdd(STAGING_DRAFT_STREAM, "*", {
                ingest_key: ingestKey,
                item_type: STAGING_ITEM_TYPE_ELECTION,
                run_id: row.run_id ?? "",
              });

              await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
              continue;
            }

            if (parsed.payload.review_decision === "approve") {
              await pool.query(
                `
                  UPDATE staging_items
                  SET status = 'validated',
                      payload = $3::jsonb,
                      reason = NULL,
                      validated_at = now(),
                      updated_at = now()
                  WHERE ingest_key = $1
                    AND item_type = $2
                `,
                [ingestKey, STAGING_ITEM_TYPE_ELECTION, JSON.stringify(payloadForValidation)]
              );
              await redis.xAdd(STAGING_VALIDATED_STREAM, "*", {
                ingest_key: ingestKey,
                item_type: STAGING_ITEM_TYPE_ELECTION,
                run_id: row.run_id ?? "",
                payload: JSON.stringify(payloadForValidation),
              });
              await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
              continue;
            }

            const rejectReason = `soft_fail_final: ${validation.reasons.join("; ")}`;
            await pool.query(
              `
                UPDATE staging_items
                SET status = 'rejected',
                    reason = $2,
                    updated_at = now()
                WHERE ingest_key = $1
                  AND item_type = $3
              `,
              [ingestKey, rejectReason, STAGING_ITEM_TYPE_ELECTION]
            );
            await redis.xAdd(STAGING_REJECTED_STREAM, "*", {
              ingest_key: ingestKey,
              item_type: STAGING_ITEM_TYPE_ELECTION,
              run_id: row.run_id ?? "",
              reason: rejectReason,
            });
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
            continue;
          }

          await pool.query(
            `
              UPDATE staging_items
              SET status = 'validated',
                  payload = $3::jsonb,
                  reason = NULL,
                  validated_at = now(),
                  updated_at = now()
              WHERE ingest_key = $1
                AND item_type = $2
            `,
            [ingestKey, STAGING_ITEM_TYPE_ELECTION, JSON.stringify(payloadForValidation)]
          );

          await redis.xAdd(STAGING_VALIDATED_STREAM, "*", {
            ingest_key: ingestKey,
            item_type: STAGING_ITEM_TYPE_ELECTION,
            run_id: row.run_id ?? "",
            payload: JSON.stringify(payloadForValidation),
          });
          await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
        } catch (error) {
          const reason = toReason(error);
          if (!ingestKey) {
            try {
              await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
            } catch {
              // keep unacked when ack fails; reclaim pass will retry later
            }
            console.error(`elections validator message error (missing ingest key): ${reason}`);
            continue;
          }

          const status = await getStagingStatus(pool, ingestKey);
          if (status === "pending") {
            // leave unacked so reclaim pass can retry
            console.warn(`elections validator retrying ingest_key=${ingestKey}: ${reason}`);
            continue;
          }

          try {
            await redis.xAck(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, entry.id);
          } catch {
            // keep unacked when ack fails; reclaim pass will retry later
          }
          console.error(`elections validator message error ingest_key=${ingestKey}: ${reason}`);
        }
      }
    };

    do {
      const reclaimed = await reclaimPendingEntries(redis, consumerName, batchSize);
      if (reclaimed.length > 0) {
        await handleEntries(reclaimed);
      }

      const batches = await redis.xReadGroup(
        STAGING_ELECTIONS_VALIDATOR_GROUP,
        consumerName,
        [{ key: STAGING_PENDING_STREAM, id: ">" }],
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
      console.error("elections validator cleanup warning (redis.quit):", toReason(error));
    }
    try {
      await pool.end();
    } catch (error) {
      console.error("elections validator cleanup warning (pool.end):", toReason(error));
    }
  }
}
