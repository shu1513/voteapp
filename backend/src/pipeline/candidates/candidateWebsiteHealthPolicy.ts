function readBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }
  const normalized = raw.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }
  throw new Error(`Invalid boolean env ${name}: ${raw}`);
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }
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

function toIsoTimestamp(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid CANDIDATE_WEBSITE_HEALTH_AS_OF value: ${value}`);
  }
  return parsed.toISOString();
}

export type CandidateWebsiteHealthPolicy = {
  enabled: boolean;
  retireEnabled: boolean;
  asOfTimestamp: string;
  staleAfterDays: number;
  maxUrlsPerRun: number;
  maxRetireUrlsPerRun: number;
  hardFailureThreshold: number;
  hardFailureWindowDays: number;
  timeoutMs: number;
  concurrency: number;
};

export function readCandidateWebsiteHealthPolicyFromEnv(
  now: Date = new Date()
): CandidateWebsiteHealthPolicy {
  const asOfRaw = process.env.CANDIDATE_WEBSITE_HEALTH_AS_OF?.trim();
  const asOfTimestamp = asOfRaw ? toIsoTimestamp(asOfRaw) : now.toISOString();

  return {
    enabled: readBooleanEnv("CANDIDATE_WEBSITE_HEALTH_ENABLED", false),
    // Retirement mutates candidates rows (archives the dead URL into
    // former_website_urls and nulls official_website_url), so it stays behind
    // its own flag: a plain sweep must never edit candidate data.
    retireEnabled: readBooleanEnv("CANDIDATE_WEBSITE_HEALTH_RETIRE_ENABLED", false),
    asOfTimestamp,
    staleAfterDays: readPositiveIntegerEnv("CANDIDATE_WEBSITE_HEALTH_STALE_AFTER_DAYS", 30),
    maxUrlsPerRun: readPositiveIntegerEnv("CANDIDATE_WEBSITE_HEALTH_MAX_URLS_PER_RUN", 1000),
    maxRetireUrlsPerRun: readPositiveIntegerEnv(
      "CANDIDATE_WEBSITE_HEALTH_MAX_RETIRE_URLS_PER_RUN",
      100
    ),
    hardFailureThreshold: readPositiveIntegerEnv(
      "CANDIDATE_WEBSITE_HEALTH_HARD_FAILURE_THRESHOLD",
      3
    ),
    hardFailureWindowDays: readPositiveIntegerEnv(
      "CANDIDATE_WEBSITE_HEALTH_HARD_FAILURE_WINDOW_DAYS",
      14
    ),
    timeoutMs: readPositiveIntegerEnv("CANDIDATE_WEBSITE_HEALTH_TIMEOUT_MS", 8_000),
    concurrency: readPositiveIntegerEnv("CANDIDATE_WEBSITE_HEALTH_CONCURRENCY", 6),
  };
}
