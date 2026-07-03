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

export const DEFAULT_ELECTIONS_SEARCH_COOLDOWN_DAYS = 180;

// Cooldown-only reader for consumers (like auto district research) that share
// the cooldown window but must not fail on rollover-only settings such as
// ELECTIONS_SEARCH_MAX_ENQUEUE_PER_RUN.
export function readElectionsSearchCooldownDaysFromEnv(): number {
  return readPositiveIntegerEnv("ELECTIONS_SEARCH_COOLDOWN_DAYS", DEFAULT_ELECTIONS_SEARCH_COOLDOWN_DAYS);
}

export function readElectionsSearchPolicyFromEnv(): ElectionsSearchPolicy {
  return {
    asOfDate: toIsoDate(new Date()),
    cooldownDays: readElectionsSearchCooldownDaysFromEnv(),
    maxEnqueuePerRun: readPositiveIntegerEnv("ELECTIONS_SEARCH_MAX_ENQUEUE_PER_RUN", 5000),
    enabled: process.env.ELECTIONS_SEARCH_ROLLOVER_ENABLED === "true",
  };
}
