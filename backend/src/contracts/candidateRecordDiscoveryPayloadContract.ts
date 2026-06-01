import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type CandidateDiscoveredRecord = {
  title: string;
  description: string;
  source_url: string;
  source_name: string;
  event_date: string;
};

export type CandidateRecordDiscoveryPayload = {
  records: CandidateDiscoveredRecord[];
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeEventDate(value: unknown): string | null {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString().slice(0, 10);
}

function parseEntry(value: unknown): CandidateDiscoveredRecord | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  const input = value as Record<string, unknown>;
  if (!isNonEmptyString(input.title) || !isNonEmptyString(input.description) || !isNonEmptyString(input.source_name)) {
    return null;
  }

  if (!isNonEmptyString(input.source_url)) {
    return null;
  }
  const sourceUrl = normalizeHttpUrl(input.source_url);
  if (!sourceUrl) {
    return null;
  }

  const eventDate = normalizeEventDate(input.event_date);
  if (!eventDate) {
    return null;
  }

  return {
    title: input.title.trim(),
    description: input.description.trim(),
    source_url: sourceUrl,
    source_name: input.source_name.trim(),
    event_date: eventDate,
  };
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

  const records: CandidateDiscoveredRecord[] = [];
  const seen = new Set<string>();
  for (const row of input.records) {
    const parsed = parseEntry(row);
    if (!parsed) {
      return { ok: false, reason: "payload.records contains invalid row" };
    }
    const dedupeKey = `${parsed.source_url}|${parsed.event_date}|${parsed.title.toLowerCase()}`;
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);
    records.push(parsed);
  }

  return { ok: true, payload: { records } };
}
