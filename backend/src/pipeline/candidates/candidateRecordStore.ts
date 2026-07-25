import { createHash } from "node:crypto";

import type { PoolClient } from "pg";

// Which writer produced the record content. NULL in the database means
// "written before provenance existed"; new writes always carry a value —
// the fields below are required so the compiler forces every writer
// (present and future) to stamp them.
export type CandidateRecordOrigin = "ai_enricher" | "repair" | "manual";

export type CandidateRecordUpsertInput = {
  candidateId: string;
  description: string;
  sourceUrl: string;
  eventDate: string | Date;
  origin: CandidateRecordOrigin;
  // Enricher staging-stream run_id or the manual writer's manual key; null
  // when the enricher message carried no run_id.
  originRunId: string | null;
};

export type CandidateRecordUpsertResult = {
  inserted: number;
  updated: number;
  processed: number;
  recordIdsByIdentityKey: Map<string, string>;
  insertedRecordIds: string[];
};

type ExistingRecordCandidate = {
  id: string;
  description: string;
  record_identity_key: string;
};

const DESCRIPTION_SIMILARITY_UPDATE_THRESHOLD = 0.86;

function normalizeTextForIdentity(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeUrlForIdentity(value: string): string {
  return value.trim().toLowerCase().replace(/\/+$/g, "");
}

function toEventDateKey(value: string | Date): string {
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    return trimmed;
  }
  return parsed.toISOString().slice(0, 10);
}

export async function deleteCandidateRecordsForReplacementRefresh(
  client: Pick<PoolClient, "query">,
  candidateId: string
): Promise<{ deletedCount: number }> {
  const trimmedCandidateId = candidateId.trim();
  if (trimmedCandidateId.length === 0) {
    return { deletedCount: 0 };
  }

  const result = await client.query(
    `
      DELETE FROM public.candidate_records
      WHERE candidate_id = $1
    `,
    [trimmedCandidateId]
  );

  return { deletedCount: result.rowCount ?? 0 };
}

export function buildCandidateRecordIdentityKey(input: {
  description: string;
  sourceUrl: string;
  eventDate: string | Date;
}): string {
  const payload = [
    "v3",
    normalizeUrlForIdentity(input.sourceUrl),
    toEventDateKey(input.eventDate),
    normalizeTextForIdentity(input.description),
  ].join("|");

  return `v3_${createHash("md5").update(payload).digest("hex")}`;
}

function toTokenSet(value: string): Set<string> {
  return new Set(
    normalizeTextForIdentity(value)
      .split(" ")
      .map((token) => token.trim())
      .filter((token) => token.length > 0)
  );
}

export function scoreCandidateRecordDescriptionSimilarity(left: string, right: string): number {
  const normalizedLeft = normalizeTextForIdentity(left);
  const normalizedRight = normalizeTextForIdentity(right);
  if (normalizedLeft.length === 0 || normalizedRight.length === 0) {
    return 0;
  }
  if (normalizedLeft === normalizedRight) {
    return 1;
  }

  const leftTokens = toTokenSet(normalizedLeft);
  const rightTokens = toTokenSet(normalizedRight);
  if (leftTokens.size === 0 || rightTokens.size === 0) {
    return 0;
  }

  let intersectionCount = 0;
  for (const token of leftTokens) {
    if (rightTokens.has(token)) {
      intersectionCount += 1;
    }
  }

  if (intersectionCount === 0) {
    return 0;
  }

  const precision = intersectionCount / leftTokens.size;
  const recall = intersectionCount / rightTokens.size;
  return (2 * precision * recall) / (precision + recall);
}

export type WithinPayloadRecordCollision = {
  firstIndex: number;
  secondIndex: number;
  eventDate: string;
  sourceUrl: string;
  similarity: number;
};

// Detects payload rows the upsert below would silently merge into ONE stored
// record: same event date, same normalized source URL, and descriptions at or
// above the similarity threshold (or outright identical identity). Live hits:
// adjacent bill numbers (HB 204/HB 205), same-day amendment votes, and an
// initial vote vs its same-day reconsideration all scored >= 0.86 and lost a
// genuinely distinct record. The manual writers refuse such payloads so the
// operator differentiates the descriptions (or splits the source URLs) before
// anything is written.
export function findWithinPayloadRecordCollisions(
  records: readonly { description: string; sourceUrl: string; eventDate: string | Date }[]
): WithinPayloadRecordCollision[] {
  const groups = new Map<string, number[]>();
  records.forEach((record, index) => {
    const key = `${toEventDateKey(record.eventDate)}|${normalizeUrlForIdentity(record.sourceUrl)}`;
    const group = groups.get(key);
    if (group) {
      group.push(index);
    } else {
      groups.set(key, [index]);
    }
  });

  const collisions: WithinPayloadRecordCollision[] = [];
  for (const indexes of groups.values()) {
    if (indexes.length < 2) {
      continue;
    }
    for (let left = 0; left < indexes.length; left += 1) {
      for (let right = left + 1; right < indexes.length; right += 1) {
        const firstIndex = indexes[left]!;
        const secondIndex = indexes[right]!;
        const first = records[firstIndex]!;
        const second = records[secondIndex]!;
        const similarity = scoreCandidateRecordDescriptionSimilarity(
          first.description,
          second.description
        );
        if (similarity >= DESCRIPTION_SIMILARITY_UPDATE_THRESHOLD) {
          collisions.push({
            firstIndex,
            secondIndex,
            eventDate: toEventDateKey(first.eventDate),
            // The normalized form is what actually collided; either row's raw
            // URL alone can read as a mismatch to the operator repairing the
            // other row.
            sourceUrl: normalizeUrlForIdentity(first.sourceUrl),
            similarity,
          });
        }
      }
    }
  }
  return collisions;
}

async function findSimilarExistingRecord(
  client: Pick<PoolClient, "query">,
  input: {
    candidateId: string;
    sourceUrl: string;
    eventDate: string;
    description: string;
  }
): Promise<{ id: string; recordIdentityKey: string; confidence: number } | null> {
  const normalizedSourceUrl = normalizeUrlForIdentity(input.sourceUrl);
  const result = await client.query<ExistingRecordCandidate>(
    `
      SELECT id, description, record_identity_key
      FROM public.candidate_records
      WHERE candidate_id = $1
        AND event_date = $2::date
        AND regexp_replace(lower(btrim(source_url)), '/+$', '') = $3
    `,
    [input.candidateId, input.eventDate, normalizedSourceUrl]
  );

  let best: { id: string; recordIdentityKey: string; confidence: number } | null = null;
  for (const row of result.rows) {
    const confidence = scoreCandidateRecordDescriptionSimilarity(input.description, row.description);
    if (!best || confidence > best.confidence) {
      best = {
        id: row.id,
        recordIdentityKey: row.record_identity_key,
        confidence,
      };
    }
  }

  if (!best || best.confidence < DESCRIPTION_SIMILARITY_UPDATE_THRESHOLD) {
    return null;
  }

  return best;
}

export async function upsertCandidateRecords(
  client: Pick<PoolClient, "query">,
  records: readonly CandidateRecordUpsertInput[]
): Promise<CandidateRecordUpsertResult> {
  let inserted = 0;
  let updated = 0;
  const recordIdsByIdentityKey = new Map<string, string>();
  const insertedRecordIds: string[] = [];

  for (const record of records) {
    const identityKey = buildCandidateRecordIdentityKey({
      description: record.description,
      sourceUrl: record.sourceUrl,
      eventDate: record.eventDate,
    });

    const eventDate = toEventDateKey(record.eventDate);
    const similar = await findSimilarExistingRecord(client, {
      candidateId: record.candidateId,
      sourceUrl: record.sourceUrl,
      eventDate,
      description: record.description,
    });

    if (similar) {
      await client.query(
        `
          UPDATE public.candidate_records
          SET description = $2,
              source_url = $3,
              event_date = $4::date,
              record_identity_key = $5,
              origin = $6,
              origin_run_id = $7,
              updated_at = now()
          WHERE id = $1
        `,
        [
          similar.id,
          record.description,
          record.sourceUrl,
          eventDate,
          identityKey,
          record.origin,
          record.originRunId,
        ]
      );
      recordIdsByIdentityKey.set(identityKey, similar.id);
      updated += 1;
      continue;
    }

    const result = await client.query<{ id: string; inserted: boolean }>(
      `
        INSERT INTO public.candidate_records (
          candidate_id,
          description,
          source_url,
          event_date,
          record_identity_key,
          origin,
          origin_run_id
        )
        VALUES ($1, $2, $3, $4::date, $5, $6, $7)
        ON CONFLICT (candidate_id, record_identity_key)
        DO UPDATE SET
          description = EXCLUDED.description,
          source_url = EXCLUDED.source_url,
          event_date = EXCLUDED.event_date,
          origin = EXCLUDED.origin,
          origin_run_id = EXCLUDED.origin_run_id,
          updated_at = now()
        RETURNING id, (xmax = 0) AS inserted
      `,
      [
        record.candidateId,
        record.description,
        record.sourceUrl,
        eventDate,
        identityKey,
        record.origin,
        record.originRunId,
      ]
    );

    const row = result.rows[0];
    if (row?.id) {
      recordIdsByIdentityKey.set(identityKey, row.id);
    }
    if (row?.inserted) {
      inserted += 1;
      if (row.id) {
        insertedRecordIds.push(row.id);
      }
    } else {
      updated += 1;
    }
  }

  return { inserted, updated, processed: records.length, recordIdsByIdentityKey, insertedRecordIds };
}
