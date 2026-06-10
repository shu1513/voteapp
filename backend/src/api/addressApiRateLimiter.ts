import type { AddressApiRateLimitInput, AddressApiRateLimitResult } from "./addressApiTypes.js";

export const DEFAULT_ADDRESS_API_RATE_LIMIT_WINDOW_MS = 60_000;
export const DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_REQUESTS = 60;
export const DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS = 10_000;

type RateLimitBucket = {
  windowStartedAt: number;
  count: number;
};

export type InMemoryAddressApiRateLimiterOptions = {
  windowMs: number;
  maxRequests: number;
  maxBuckets?: number;
  sweepIntervalMs?: number;
  now?: () => number;
};

export type InMemoryAddressApiRateLimiter = ((input: AddressApiRateLimitInput) => AddressApiRateLimitResult) & {
  getBucketCount: () => number;
};

export function createInMemoryAddressApiRateLimiter(
  options: InMemoryAddressApiRateLimiterOptions
): InMemoryAddressApiRateLimiter {
  const buckets = new Map<string, RateLimitBucket>();
  const maxBuckets = options.maxBuckets ?? DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS;
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
    // This is a memory bound, not a fairness guarantee. Insertion-order eviction
    // is sufficient here; use Redis/edge rate limiting if true distributed LRU matters.
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
  }) as InMemoryAddressApiRateLimiter;

  rateLimit.getBucketCount = () => buckets.size;

  return rateLimit;
}
