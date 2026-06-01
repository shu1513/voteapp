import { normalizeHttpUrl } from "../utils/normalizeHttpUrl.js";

export type CandidateRecordSourceRepair = {
  bad_index: number;
  source_url: string;
  source_name: string;
};

export type CandidateRecordSourceRepairPayload = {
  repairs: CandidateRecordSourceRepair[];
  no_replacement_indexes: number[];
};

type ParseOptions = {
  badRecordCount: number;
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function parseBadIndex(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    return null;
  }
  return value;
}

export function parseCandidateRecordSourceRepairPayload(
  payload: unknown,
  options: ParseOptions
):
  | { ok: true; payload: CandidateRecordSourceRepairPayload }
  | { ok: false; reason: string } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return { ok: false, reason: "payload must be object" };
  }

  const input = payload as Record<string, unknown>;
  if (!Array.isArray(input.repairs)) {
    return { ok: false, reason: "payload.repairs must be array" };
  }

  const repairs: CandidateRecordSourceRepair[] = [];
  const noReplacementIndexes: number[] = [];
  const seenIndexes = new Set<number>();

  for (const item of input.repairs) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return { ok: false, reason: "payload.repairs contains invalid row" };
    }
    const row = item as Record<string, unknown>;

    const badIndex = parseBadIndex(row.bad_index);
    if (badIndex === null) {
      return { ok: false, reason: "payload.repairs[].bad_index must be non-negative integer" };
    }
    if (badIndex >= options.badRecordCount) {
      return {
        ok: false,
        reason: `payload.repairs[].bad_index out of range: ${badIndex} >= ${options.badRecordCount}`,
      };
    }
    if (seenIndexes.has(badIndex)) {
      return { ok: false, reason: `payload.repairs contains duplicate bad_index=${badIndex}` };
    }
    seenIndexes.add(badIndex);

    const noReplacement = row.no_replacement === true;
    if (noReplacement) {
      noReplacementIndexes.push(badIndex);
      continue;
    }

    if (!isNonEmptyString(row.source_url) || !isNonEmptyString(row.source_name)) {
      return {
        ok: false,
        reason: "payload.repairs rows require source_url and source_name unless no_replacement=true",
      };
    }

    const sourceUrl = normalizeHttpUrl(row.source_url);
    if (!sourceUrl) {
      return { ok: false, reason: "payload.repairs[].source_url must be valid http(s) URL" };
    }

    repairs.push({
      bad_index: badIndex,
      source_url: sourceUrl,
      source_name: row.source_name.trim(),
    });
  }

  return {
    ok: true,
    payload: {
      repairs,
      no_replacement_indexes: noReplacementIndexes,
    },
  };
}
