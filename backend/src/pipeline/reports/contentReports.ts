import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

export const CONTENT_REPORT_ENTITY_TYPES = ["candidate", "candidate_record", "election", "ballot_measure"] as const;
export type ContentReportEntityType = (typeof CONTENT_REPORT_ENTITY_TYPES)[number];

export type ContentReportInput = {
  entityType: ContentReportEntityType;
  entityId: string;
  message: string;
  suggestedSourceUrl?: string | null;
  reporterEmail?: string | null;
  userId?: string | null;
};

export type CreatedContentReport = {
  id: string;
};

export type ContentReportStats = {
  by_status: Array<{ status: string; count: number }>;
  open_entities: Array<{ entity_type: ContentReportEntityType; entity_id: string; open_report_count: number }>;
};

type Queryable = Pick<Pool | PoolClient, "query">;

type EntityLabelRow = { label: string | null };

type ContentReportErrorCode = "invalid_entity_id" | "entity_not_found" | "invalid_user_id";

export class ContentReportError extends Error {
  constructor(
    readonly code: ContentReportErrorCode,
    message: string
  ) {
    super(message);
    this.name = "ContentReportError";
  }
}

function assertUuid(value: string, fieldName: string, code: ContentReportErrorCode): string {
  const normalized = value.trim();
  if (!isUuid(normalized)) {
    throw new ContentReportError(code, `${fieldName} must be a valid UUID`);
  }
  return normalized;
}

function truncateLabel(value: string, maxLength = 500): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength - 3).trimEnd()}...`;
}

async function normalizeActiveUserId(db: Queryable, userId: string | null | undefined): Promise<string | null> {
  if (!userId) {
    return null;
  }
  const normalized = assertUuid(userId, "user_id", "invalid_user_id");
  const result = await db.query<{ id: string }>(
    `
      SELECT id::text AS id
      FROM public.users
      WHERE id = $1::uuid
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [normalized]
  );
  return result.rows[0]?.id ?? null;
}

async function loadEntityLabel(
  db: Queryable,
  entityType: ContentReportEntityType,
  entityId: string
): Promise<string> {
  const normalizedEntityId = assertUuid(entityId, "entity_id", "invalid_entity_id");
  let result: { rows: EntityLabelRow[] };

  if (entityType === "candidate") {
    result = await db.query<EntityLabelRow>(
      `
        SELECT COALESCE(NULLIF(trim(display_name), ''), trim(concat_ws(' ', first_name, last_name))) AS label
        FROM public.candidates
        WHERE id = $1::uuid
          AND deleted_at IS NULL
          AND merged_into_candidate_id IS NULL
        LIMIT 1
      `,
      [normalizedEntityId]
    );
  } else if (entityType === "candidate_record") {
    result = await db.query<EntityLabelRow>(
      `
        SELECT concat_ws(
          ': ',
          COALESCE(NULLIF(trim(candidate.display_name), ''), trim(concat_ws(' ', candidate.first_name, candidate.last_name))),
          left(regexp_replace(candidate_record.description, '\\s+', ' ', 'g'), 220)
        ) AS label
        FROM public.candidate_records AS candidate_record
        JOIN public.candidates AS candidate
          ON candidate.id = candidate_record.candidate_id
        WHERE candidate_record.id = $1::uuid
          AND candidate.deleted_at IS NULL
          AND candidate.merged_into_candidate_id IS NULL
        LIMIT 1
      `,
      [normalizedEntityId]
    );
  } else if (entityType === "election") {
    result = await db.query<EntityLabelRow>(
      `
        SELECT concat_ws(' • ', official_ballot_title, election_date::text) AS label
        FROM public.elections
        WHERE id = $1::uuid
        LIMIT 1
      `,
      [normalizedEntityId]
    );
  } else {
    result = await db.query<EntityLabelRow>(
      `
        SELECT concat_ws(' • ', ballot_measure.official_ballot_title, election.election_date::text) AS label
        FROM public.ballot_measures AS ballot_measure
        JOIN public.elections AS election
          ON election.id = ballot_measure.election_id
        WHERE ballot_measure.id = $1::uuid
        LIMIT 1
      `,
      [normalizedEntityId]
    );
  }

  const label = result.rows[0]?.label;
  if (!label?.trim()) {
    throw new ContentReportError("entity_not_found", "Reported content was not found");
  }
  return truncateLabel(label);
}

export async function createContentReport(db: Queryable, input: ContentReportInput): Promise<CreatedContentReport> {
  const entityId = assertUuid(input.entityId, "entity_id", "invalid_entity_id");
  const entityLabelSnapshot = await loadEntityLabel(db, input.entityType, entityId);
  const userId = await normalizeActiveUserId(db, input.userId);
  const result = await db.query<{ id: string }>(
    `
      INSERT INTO public.content_reports (
        entity_type,
        entity_id,
        entity_label_snapshot,
        message,
        suggested_source_url,
        reporter_email,
        user_id
      )
      VALUES ($1, $2::uuid, $3, $4, $5, $6, $7::uuid)
      RETURNING id::text AS id
    `,
    [
      input.entityType,
      entityId,
      entityLabelSnapshot,
      input.message,
      input.suggestedSourceUrl ?? null,
      input.reporterEmail ?? null,
      userId,
    ]
  );

  const id = result.rows[0]?.id;
  if (!id) {
    throw new Error("content report insert did not return an id");
  }
  return { id };
}

export async function getContentReportStats(db: Queryable): Promise<ContentReportStats> {
  const [statusResult, entityResult] = await Promise.all([
    db.query<{ status: string; count: string }>(
      `
        SELECT status, count(*)::text AS count
        FROM public.content_reports
        GROUP BY status
        ORDER BY status
      `
    ),
    db.query<{ entity_type: ContentReportEntityType; entity_id: string; open_report_count: string }>(
      `
        SELECT entity_type, entity_id::text AS entity_id, count(*)::text AS open_report_count
        FROM public.content_reports
        WHERE status IN ('new', 'investigating')
        GROUP BY entity_type, entity_id
        ORDER BY count(*) DESC, min(created_at) ASC
        LIMIT 20
      `
    ),
  ]);

  return {
    by_status: statusResult.rows.map((row) => ({ status: row.status, count: Number(row.count) })),
    open_entities: entityResult.rows.map((row) => ({
      entity_type: row.entity_type,
      entity_id: row.entity_id,
      open_report_count: Number(row.open_report_count),
    })),
  };
}
