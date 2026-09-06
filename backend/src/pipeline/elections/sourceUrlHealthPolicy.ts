import { readBooleanEnv, readPositiveIntegerEnv } from "../../config/envReaders.js";

function toIsoTimestamp(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ELECTIONS_SOURCE_URL_HEALTH_AS_OF value: ${value}`);
  }
  return parsed.toISOString();
}

export type SourceUrlHealthPolicy = {
  enabled: boolean;
  cleanupEnabled: boolean;
  asOfTimestamp: string;
  staleAfterDays: number;
  maxUrlsPerRun: number;
  maxCleanupUrlsPerRun: number;
  hardFailureThreshold: number;
  hardFailureWindowDays: number;
  timeoutMs: number;
  concurrency: number;
};

export function readSourceUrlHealthPolicyFromEnv(now: Date = new Date()): SourceUrlHealthPolicy {
  const asOfRaw = process.env.ELECTIONS_SOURCE_URL_HEALTH_AS_OF?.trim();
  const asOfTimestamp = asOfRaw ? toIsoTimestamp(asOfRaw) : now.toISOString();

  return {
    enabled: readBooleanEnv("ELECTIONS_SOURCE_URL_HEALTH_ENABLED", false),
    cleanupEnabled: readBooleanEnv("ELECTIONS_SOURCE_URL_HEALTH_CLEANUP_ENABLED", false),
    asOfTimestamp,
    staleAfterDays: readPositiveIntegerEnv("ELECTIONS_SOURCE_URL_HEALTH_STALE_AFTER_DAYS", 30),
    maxUrlsPerRun: readPositiveIntegerEnv("ELECTIONS_SOURCE_URL_HEALTH_MAX_URLS_PER_RUN", 500),
    maxCleanupUrlsPerRun: readPositiveIntegerEnv("ELECTIONS_SOURCE_URL_HEALTH_MAX_CLEANUP_URLS_PER_RUN", 200),
    hardFailureThreshold: readPositiveIntegerEnv("ELECTIONS_SOURCE_URL_HEALTH_HARD_FAILURE_THRESHOLD", 3),
    hardFailureWindowDays: readPositiveIntegerEnv("ELECTIONS_SOURCE_URL_HEALTH_HARD_FAILURE_WINDOW_DAYS", 14),
    timeoutMs: readPositiveIntegerEnv("ELECTIONS_SOURCE_URL_HEALTH_TIMEOUT_MS", 8_000),
    concurrency: readPositiveIntegerEnv("ELECTIONS_SOURCE_URL_HEALTH_CONCURRENCY", 6),
  };
}
