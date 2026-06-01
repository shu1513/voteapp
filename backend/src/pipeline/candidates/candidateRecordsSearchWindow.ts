export type CandidateRecordsSearchWindow = {
  mode: "full" | "incremental";
  sinceDate: string | null;
};

function parsePositiveInteger(raw: string, name: string): number {
  const normalized = raw.trim();
  if (!/^[1-9]\d*$/.test(normalized)) {
    throw new Error(`Invalid positive integer env ${name}: ${raw}`);
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid positive integer env ${name}: ${raw}`);
  }
  return parsed;
}

function parseIsoDate(value: string): Date {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Invalid ISO date: ${value}`);
  }
  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ISO date: ${value}`);
  }
  return parsed;
}

function toIsoDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function subtractDays(dateIso: string, days: number): string {
  const parsed = parseIsoDate(dateIso);
  parsed.setUTCDate(parsed.getUTCDate() - days);
  return toIsoDate(parsed);
}

export function readCandidateRecordsOverlapDaysFromEnv(fallback = 45): number {
  const raw = process.env.CANDIDATE_RECORDS_OVERLAP_DAYS;
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }
  return parsePositiveInteger(raw, "CANDIDATE_RECORDS_OVERLAP_DAYS");
}

export function computeCandidateRecordsSearchWindow(
  lastRecordsResearchedThrough: string | null | undefined,
  overlapDays: number
): CandidateRecordsSearchWindow {
  if (!Number.isFinite(overlapDays) || overlapDays <= 0) {
    throw new Error(`Invalid overlapDays: ${overlapDays}`);
  }

  if (!lastRecordsResearchedThrough) {
    return { mode: "full", sinceDate: null };
  }

  return {
    mode: "incremental",
    sinceDate: subtractDays(lastRecordsResearchedThrough, overlapDays),
  };
}
