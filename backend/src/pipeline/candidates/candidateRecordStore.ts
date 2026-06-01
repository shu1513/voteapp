import { createHash } from "node:crypto";

import type { PoolClient } from "pg";

export type CandidateRecordUpsertInput = {
  candidateId: string;
  title: string;
  description: string;
  sourceUrl: string;
  sourceName: string;
  eventDate: string | Date;
};

type UpsertResult = {
  inserted: number;
  updated: number;
  processed: number;
};

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

export function buildCandidateRecordIdentityKey(input: {
  title: string;
  sourceUrl: string;
  sourceName: string;
  eventDate: string | Date;
}): string {
  const payload = [
    "v1",
    normalizeUrlForIdentity(input.sourceUrl),
    toEventDateKey(input.eventDate),
    normalizeTextForIdentity(input.title),
    normalizeTextForIdentity(input.sourceName),
  ].join("|");

  return `v1_${createHash("md5").update(payload).digest("hex")}`;
}

export async function upsertCandidateRecords(
  client: Pick<PoolClient, "query">,
  records: readonly CandidateRecordUpsertInput[]
): Promise<UpsertResult> {
  let inserted = 0;
  let updated = 0;

  for (const record of records) {
    const identityKey = buildCandidateRecordIdentityKey({
      title: record.title,
      sourceUrl: record.sourceUrl,
      sourceName: record.sourceName,
      eventDate: record.eventDate,
    });

    const eventDate = toEventDateKey(record.eventDate);

    const result = await client.query<{ inserted: boolean }>(
      `
        INSERT INTO public.candidate_records (
          candidate_id,
          title,
          description,
          source_url,
          source_name,
          event_date,
          record_identity_key
        )
        VALUES ($1, $2, $3, $4, $5, $6::date, $7)
        ON CONFLICT (candidate_id, record_identity_key)
        DO UPDATE SET
          title = EXCLUDED.title,
          description = EXCLUDED.description,
          source_url = EXCLUDED.source_url,
          source_name = EXCLUDED.source_name,
          event_date = EXCLUDED.event_date
        RETURNING (xmax = 0) AS inserted
      `,
      [
        record.candidateId,
        record.title,
        record.description,
        record.sourceUrl,
        record.sourceName,
        eventDate,
        identityKey,
      ]
    );

    if (result.rows[0]?.inserted) {
      inserted += 1;
    } else {
      updated += 1;
    }
  }

  return { inserted, updated, processed: records.length };
}
