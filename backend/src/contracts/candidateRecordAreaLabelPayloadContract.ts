import { isNonStanceResearchAreaSlug } from "../pipeline/candidates/candidateRecordResearchAreaPolicy.js";

export type CandidateRecordAreaLabel = {
  record_index: number;
  research_area_slug: string;
  stance?: "for" | "against";
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
  if (value === "for" || value === "against") {
    return value;
  }
  return null;
}

function parseLabel(
  value: unknown,
  options: ParseOptions
): { ok: true; label: CandidateRecordAreaLabel } | { ok: false; reason: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { ok: false, reason: "row must be an object" };
  }
  const input = value as Record<string, unknown>;

  if (!Number.isInteger(input.record_index) || Number(input.record_index) < 0) {
    return { ok: false, reason: "record_index must be a non-negative integer" };
  }
  const recordIndex = Number(input.record_index);
  if (options.recordCount !== undefined && recordIndex >= options.recordCount) {
    return {
      ok: false,
      reason: `record_index ${recordIndex} is out of range (record count ${options.recordCount})`,
    };
  }

  if (!isNonEmptyString(input.research_area_slug)) {
    return { ok: false, reason: "research_area_slug must be a non-empty string" };
  }
  const slug = input.research_area_slug.trim().toLowerCase();
  if (options.allowedResearchAreaSlugs && !options.allowedResearchAreaSlugs.has(slug)) {
    return {
      ok: false,
      reason: `research_area_slug '${slug}' is not in the allowed research areas for this office`,
    };
  }

  const rawStance = input.stance;
  const stance = normalizeStance(rawStance);
  const stanceWasProvided = rawStance !== undefined && rawStance !== null;
  if (stanceWasProvided && stance === null) {
    return { ok: false, reason: `stance must be 'for' or 'against', got ${JSON.stringify(rawStance)}` };
  }
  if (isNonStanceResearchAreaSlug(slug) && stanceWasProvided) {
    return { ok: false, reason: `stance is not allowed for non-stance area '${slug}'` };
  }
  if (!isNonStanceResearchAreaSlug(slug) && stance === null) {
    return { ok: false, reason: `stance is required for research_area_slug '${slug}'` };
  }

  return {
    ok: true,
    label: {
      record_index: recordIndex,
      research_area_slug: slug,
      ...(stance ? { stance } : {}),
    },
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

  for (const [index, row] of input.labels.entries()) {
    const parsed = parseLabel(row, options);
    if (!parsed.ok) {
      // Keep the "payload.labels contains invalid row" prefix stable: operator docs
      // and log searches match on it. The suffix names the row and cause so the AI
      // retry feedback loop (and manual-wrapper operators) can fix the right thing.
      return { ok: false, reason: `payload.labels contains invalid row: labels[${index}]: ${parsed.reason}` };
    }
    const pairKey = `${parsed.label.record_index}::${parsed.label.research_area_slug}`;
    if (seenPairs.has(pairKey)) {
      return { ok: false, reason: "payload.labels contains duplicate (record_index, research_area_slug) pair" };
    }
    seenPairs.add(pairKey);
    seenRecordIndexes.add(parsed.label.record_index);
    labels.push(parsed.label);
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
