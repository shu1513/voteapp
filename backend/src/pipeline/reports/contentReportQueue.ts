import type { Pool, PoolClient } from "pg";
import { isUuid } from "../../utils/uuid.js";
import type { ContentReportEntityType } from "./contentReports.js";

type Queryable = Pick<Pool | PoolClient, "query">;

/**
 * Operator-side reading and closing of content reports. Deliberately small:
 * one operator, low volume, so there is no claim/lease/ownership layer —
 * `list` shows what is open and `resolve` closes one row. The status and
 * resolution vocabularies mirror migration 155's CHECK constraints.
 */
export const CONTENT_REPORT_RESOLUTIONS = ["fixed", "no_change_needed", "unverifiable", "duplicate", "spam"] as const;
export type ContentReportResolution = (typeof CONTENT_REPORT_RESOLUTIONS)[number];

export const OPEN_CONTENT_REPORT_STATUSES = ["new", "investigating"] as const;
export type OpenContentReportStatus = (typeof OPEN_CONTENT_REPORT_STATUSES)[number];

/** Mirrors chk_content_reports_resolution_status: which terminal status a resolution lands in. */
export function terminalStatusForResolution(resolution: ContentReportResolution): "resolved" | "dismissed" {
  return resolution === "unverifiable" || resolution === "spam" ? "dismissed" : "resolved";
}

export function isContentReportResolution(value: string): value is ContentReportResolution {
  return (CONTENT_REPORT_RESOLUTIONS as readonly string[]).includes(value);
}

export function isOpenContentReportStatus(value: string): value is OpenContentReportStatus {
  return (OPEN_CONTENT_REPORT_STATUSES as readonly string[]).includes(value);
}

type ContentReportQueueErrorCode = "invalid_report_id" | "invalid_resolution" | "summary_required" | "report_not_open";

export class ContentReportQueueError extends Error {
  constructor(
    readonly code: ContentReportQueueErrorCode,
    message: string
  ) {
    super(message);
    this.name = "ContentReportQueueError";
  }
}

/**
 * One open report as the operator sees it. `message` and
 * `suggested_source_url` are reporter-written (attacker-controlled): the CLI
 * prints them for a human, and nothing here ever emails or follows them.
 */
export type OpenContentReport = {
  id: string;
  entity_type: ContentReportEntityType;
  entity_id: string;
  entity_label_snapshot: string;
  status: OpenContentReportStatus;
  message: string;
  suggested_source_url: string | null;
  created_at: string;
  age_days: number;
};

export type ListOpenContentReportsOptions = {
  status?: OpenContentReportStatus;
  limit?: number;
};

const DEFAULT_LIST_LIMIT = 50;

export async function listOpenContentReports(
  db: Queryable,
  options: ListOpenContentReportsOptions = {}
): Promise<OpenContentReport[]> {
  const statuses = options.status ? [options.status] : [...OPEN_CONTENT_REPORT_STATUSES];
  const limit = options.limit ?? DEFAULT_LIST_LIMIT;
  const result = await db.query<OpenContentReport>(
    `
      SELECT
        id::text AS id,
        entity_type,
        entity_id::text AS entity_id,
        entity_label_snapshot,
        status,
        message,
        suggested_source_url,
        created_at::text AS created_at,
        GREATEST(0, floor(extract(epoch FROM (now() - created_at)) / 86400))::int AS age_days
      FROM public.content_reports
      WHERE status = ANY($1::text[])
      ORDER BY created_at ASC, id ASC
      LIMIT $2
    `,
    [statuses, limit]
  );
  return result.rows;
}

export type ResolveContentReportInput = {
  id: string;
  resolution: ContentReportResolution;
  summary: string;
};

export type ResolvedContentReport = {
  id: string;
  status: "resolved" | "dismissed";
  resolution: ContentReportResolution;
};

/**
 * Closes one open report. Only `new`/`investigating` rows are touched: a row
 * that is already resolved or dismissed is left exactly as it was and the
 * caller gets `report_not_open`, so a repeated or mistyped command can never
 * overwrite an earlier decision.
 */
export async function resolveContentReport(db: Queryable, input: ResolveContentReportInput): Promise<ResolvedContentReport> {
  const id = input.id.trim();
  if (!isUuid(id)) {
    throw new ContentReportQueueError("invalid_report_id", "report id must be a valid UUID");
  }
  if (!isContentReportResolution(input.resolution)) {
    throw new ContentReportQueueError(
      "invalid_resolution",
      `resolution must be one of: ${CONTENT_REPORT_RESOLUTIONS.join(", ")}`
    );
  }
  const summary = input.summary.replace(/\s+/g, " ").trim();
  if (summary.length === 0) {
    throw new ContentReportQueueError("summary_required", "a non-empty --summary is required");
  }
  const status = terminalStatusForResolution(input.resolution);
  const updated = await db.query<ResolvedContentReport>(
    `
      UPDATE public.content_reports
      SET status = $2,
          resolution = $3,
          investigation_summary = $4,
          finished_at = now(),
          updated_at = now()
      WHERE id = $1::uuid
        AND status = ANY($5::text[])
      RETURNING id::text AS id, status, resolution
    `,
    [id, status, input.resolution, summary, [...OPEN_CONTENT_REPORT_STATUSES]]
  );
  const row = updated.rows[0];
  if (row) {
    return row;
  }
  const existing = await db.query<{ status: string; resolution: string | null }>(
    `
      SELECT status, resolution
      FROM public.content_reports
      WHERE id = $1::uuid
      LIMIT 1
    `,
    [id]
  );
  const current = existing.rows[0];
  throw new ContentReportQueueError(
    "report_not_open",
    current
      ? `report ${id} is already ${current.status} (resolution: ${current.resolution ?? "none"}); left unchanged`
      : `report ${id} was not found`
  );
}

export type OpenContentReportEntity = {
  entity_type: ContentReportEntityType;
  entity_id: string;
  report_count: number;
  oldest_age_days: number;
};

export type OpenContentReportSummary = {
  open_count: number;
  oldest_age_days: number;
  by_entity: OpenContentReportEntity[];
};

/**
 * Counts for the operator summary email. Only ids, counts, and ages leave
 * the database here — never the reporter's message or URL — so the email
 * body cannot carry attacker-written text.
 */
export async function summarizeOpenContentReports(db: Queryable): Promise<OpenContentReportSummary> {
  const result = await db.query<{
    entity_type: ContentReportEntityType;
    entity_id: string;
    report_count: string;
    oldest_age_days: number;
  }>(
    `
      SELECT
        entity_type,
        entity_id::text AS entity_id,
        count(*)::text AS report_count,
        GREATEST(0, floor(extract(epoch FROM (now() - min(created_at))) / 86400))::int AS oldest_age_days
      FROM public.content_reports
      WHERE status = ANY($1::text[])
      GROUP BY entity_type, entity_id
      ORDER BY min(created_at) ASC, entity_type ASC, entity_id ASC
    `,
    [[...OPEN_CONTENT_REPORT_STATUSES]]
  );
  const byEntity = result.rows.map((row) => ({
    entity_type: row.entity_type,
    entity_id: row.entity_id,
    report_count: Number(row.report_count),
    oldest_age_days: Number(row.oldest_age_days),
  }));
  return {
    open_count: byEntity.reduce((sum, entity) => sum + entity.report_count, 0),
    oldest_age_days: byEntity.reduce((max, entity) => Math.max(max, entity.oldest_age_days), 0),
    by_entity: byEntity,
  };
}
