function toIsoDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${raw}. Expected a positive integer.`);
  }
  return parsed;
}

export type ElectionsSearchPolicy = {
  asOfDate: string;
  cooldownDays: number;
  maxEnqueuePerRun: number;
  enabled: boolean;
};

export function readElectionsSearchPolicyFromEnv(): ElectionsSearchPolicy {
  return {
    asOfDate: toIsoDate(new Date()),
    cooldownDays: readPositiveIntegerEnv("ELECTIONS_SEARCH_COOLDOWN_DAYS", 180),
    maxEnqueuePerRun: readPositiveIntegerEnv("ELECTIONS_SEARCH_MAX_ENQUEUE_PER_RUN", 5000),
    enabled: process.env.ELECTIONS_SEARCH_ROLLOVER_ENABLED === "true",
  };
}
