import type { AddressApiRateLimitInput, AddressApiRateLimitResult } from "./addressApiTypes.js";

export const DEFAULT_CONTENT_REPORT_RATE_LIMIT_WINDOW_MS = 15 * 60_000;
export const DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_REQUESTS = 5;
export const DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_BUCKETS = 10_000;

type RateLimitBucket = {
  windowStartedAt: number;
  count: number;
};

export type InMemoryContentReportRateLimiterOptions = {
  windowMs: number;
  maxRequests: number;
  maxBuckets?: number;
  sweepIntervalMs?: number;
  now?: () => number;
};

export type InMemoryContentReportRateLimiter = ((input: AddressApiRateLimitInput) => AddressApiRateLimitResult) & {
  getBucketCount: () => number;
};

export function createInMemoryContentReportRateLimiter(
  options: InMemoryContentReportRateLimiterOptions
): InMemoryContentReportRateLimiter {
  const buckets = new Map<string, RateLimitBucket>();
  const maxBuckets = options.maxBuckets ?? DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_BUCKETS;
  const sweepIntervalMs = options.sweepIntervalMs ?? options.windowMs;
  const nowMs = options.now ?? (() => Date.now());
  let lastSweepAt = nowMs();

  function sweepExpiredBuckets(now: number): void {
    if (now - lastSweepAt < sweepIntervalMs && buckets.size <= maxBuckets) {
      return;
    }
    lastSweepAt = now;
    for (const [key, bucket] of buckets) {
      if (now - bucket.windowStartedAt >= options.windowMs) {
        buckets.delete(key);
      }
    }
  }

  function evictOldestUntilUnderCap(): void {
    while (buckets.size >= maxBuckets) {
      const oldestKey = buckets.keys().next().value as string | undefined;
      if (!oldestKey) {
        return;
      }
      buckets.delete(oldestKey);
    }
  }

  const rateLimit = ((input: AddressApiRateLimitInput): AddressApiRateLimitResult => {
    const now = nowMs();
    sweepExpiredBuckets(now);

    const key = input.clientIp || "unknown";
    const existing = buckets.get(key);
    if (!existing || now - existing.windowStartedAt >= options.windowMs) {
      if (!existing) {
        evictOldestUntilUnderCap();
      }
      buckets.set(key, { windowStartedAt: now, count: 1 });
      return { allowed: true };
    }

    existing.count += 1;
    if (existing.count <= options.maxRequests) {
      return { allowed: true };
    }

    return {
      allowed: false,
      retryAfterSeconds: Math.ceil((options.windowMs - (now - existing.windowStartedAt)) / 1000),
    };
  }) as InMemoryContentReportRateLimiter;

  rateLimit.getBucketCount = () => buckets.size;

  return rateLimit;
}
