import { isNonStanceResearchAreaSlug } from "../pipeline/candidates/candidateRecordResearchAreaPolicy.js";

export type CandidateRecordAreaLabel = {
  record_index: number;
  research_area_slug: string;
  stance?: "for" | "against" | "neutral";
};

export type CandidateRecordAreaLabelPayload = {
  labels: CandidateRecordAreaLabel[];
};

type ParseOptions = {
  allowedResearchAreaSlugs?: ReadonlySet<string>;
  recordCount?: number;
  requireLabelForEveryRecord?: boolean;
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeStance(value: unknown): CandidateRecordAreaLabel["stance"] | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (value === "for" || value === "against" || value === "neutral") {
    return value;
  }
  return null;
}

function parseLabel(value: unknown, options: ParseOptions): CandidateRecordAreaLabel | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const input = value as Record<string, unknown>;

  if (!Number.isInteger(input.record_index) || Number(input.record_index) < 0) {
    return null;
  }
  const recordIndex = Number(input.record_index);
  if (options.recordCount !== undefined && recordIndex >= options.recordCount) {
    return null;
  }

  if (!isNonEmptyString(input.research_area_slug)) {
    return null;
  }
  const slug = input.research_area_slug.trim().toLowerCase();
  if (options.allowedResearchAreaSlugs && !options.allowedResearchAreaSlugs.has(slug)) {
    return null;
  }

  const stance = normalizeStance(input.stance);
  if (isNonStanceResearchAreaSlug(slug) && stance !== null) {
    return null;
  }
  if (!isNonStanceResearchAreaSlug(slug) && stance === null) {
    return null;
  }

  return {
    record_index: recordIndex,
    research_area_slug: slug,
    ...(stance ? { stance } : {}),
  };
}

export function parseCandidateRecordAreaLabelPayload(
  payload: unknown,
  options: ParseOptions = {}
):
  | { ok: true; payload: CandidateRecordAreaLabelPayload }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be an object" };
  }
  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.labels)) {
    return { ok: false, reason: "payload.labels must be array" };
  }

  const labels: CandidateRecordAreaLabel[] = [];
  const seenPairs = new Set<string>();
  const seenRecordIndexes = new Set<number>();

  for (const row of input.labels) {
    const parsed = parseLabel(row, options);
    if (!parsed) {
      return { ok: false, reason: "payload.labels contains invalid row" };
    }
    const pairKey = `${parsed.record_index}::${parsed.research_area_slug}`;
    if (seenPairs.has(pairKey)) {
      return { ok: false, reason: "payload.labels contains duplicate (record_index, research_area_slug) pair" };
    }
    seenPairs.add(pairKey);
    seenRecordIndexes.add(parsed.record_index);
    labels.push(parsed);
  }

  if (options.requireLabelForEveryRecord && options.recordCount !== undefined) {
    for (let i = 0; i < options.recordCount; i += 1) {
      if (!seenRecordIndexes.has(i)) {
        return { ok: false, reason: `payload.labels is missing at least one label for record_index=${i}` };
      }
    }
  }

  return { ok: true, payload: { labels } };
}
