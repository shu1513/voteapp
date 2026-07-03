import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";
import { parseRecordEventDate } from "./recordEventDate.js";

export type CandidateDiscoveredRecord = {
  description: string;
  source_url: string;
  event_date: string;
};

export type CandidateRecordDiscoveryPayload = {
  records: CandidateDiscoveredRecord[];
};

export type CandidateRecordDiscoveryInvalidRow = {
  index: number;
  reason: string;
  raw_record: {
    description: string;
    source_url: string;
    event_date: string;
  };
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeRawString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function parseEntry(
  value: unknown
): { ok: true; record: CandidateDiscoveredRecord } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "row must be object" };
  }

  const input = value as Record<string, unknown>;
  if (!isNonEmptyString(input.description)) {
    return { ok: false, reason: "description must be non-empty string" };
  }

  if (!isNonEmptyString(input.source_url)) {
    return { ok: false, reason: "source_url must be non-empty string" };
  }
  const sourceUrl = normalizeHttpUrl(input.source_url);
  if (!sourceUrl) {
    return { ok: false, reason: "source_url must be valid http(s) URL" };
  }

  const eventDate = parseRecordEventDate(input.event_date);
  if (!eventDate.ok) {
    return { ok: false, reason: eventDate.reason };
  }

  return {
    ok: true,
    record: {
      description: input.description.trim(),
      source_url: sourceUrl,
      event_date: eventDate.eventDate,
    },
  };
}

export function parseCandidateRecordDiscoveryPayloadPartial(payload: unknown):
  | { ok: true; payload: CandidateRecordDiscoveryPayload; invalid_rows: CandidateRecordDiscoveryInvalidRow[] }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.records)) {
    return { ok: false, reason: "payload.records must be array" };
  }

  const records: CandidateDiscoveredRecord[] = [];
  const invalidRows: CandidateRecordDiscoveryInvalidRow[] = [];
  const seen = new Set<string>();

  for (const [index, row] of input.records.entries()) {
    const parsed = parseEntry(row);
    if (!parsed.ok) {
      const rowObject = typeof row === "object" && row !== null && !Array.isArray(row)
        ? (row as Record<string, unknown>)
        : {};
      invalidRows.push({
        index,
        reason: parsed.reason,
        raw_record: {
          description: normalizeRawString(rowObject.description),
          source_url: normalizeRawString(rowObject.source_url),
          event_date: normalizeRawString(rowObject.event_date),
        },
      });
      continue;
    }
    const dedupeKey = `${parsed.record.source_url}|${parsed.record.event_date}|${parsed.record.description.toLowerCase()}`;
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);
    records.push(parsed.record);
  }

  return { ok: true, payload: { records }, invalid_rows: invalidRows };
}

export function parseCandidateRecordDiscoveryPayload(
  payload: unknown
):
  | { ok: true; payload: CandidateRecordDiscoveryPayload }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.records)) {
    return { ok: false, reason: "payload.records must be array" };
  }

  const parsed = parseCandidateRecordDiscoveryPayloadPartial(payload);
  if (!parsed.ok) {
    return parsed;
  }
  if (parsed.invalid_rows.length > 0) {
    // Keep the "payload.records contains invalid row" prefix stable for log
    // searches; append the per-row detail the partial parser already computed.
    const detail = parsed.invalid_rows
      .map((row) => `records[${row.index}]: ${row.reason}`)
      .join("; ");
    return { ok: false, reason: `payload.records contains invalid row: ${detail}` };
  }

  return { ok: true, payload: parsed.payload };
}
