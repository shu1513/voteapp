export type InMemoryKeyedRateLimiterOptions = {
  windowMs: number;
  maxRequests: number;
  maxBuckets: number;
  sweepIntervalMs?: number;
  now?: () => number;
};

export type InMemoryKeyedRateLimitResult = {
  allowed: boolean;
  retryAfterSeconds?: number;
};

type RateLimitBucket = {
  windowStartedAt: number;
  count: number;
};

export type InMemoryKeyedRateLimiter<TInput> = ((input: TInput) => InMemoryKeyedRateLimitResult) & {
  getBucketCount: () => number;
};

export function createInMemoryKeyedRateLimiter<TInput>(
  options: InMemoryKeyedRateLimiterOptions,
  getKey: (input: TInput) => string
): InMemoryKeyedRateLimiter<TInput> {
  const buckets = new Map<string, RateLimitBucket>();
  const sweepIntervalMs = options.sweepIntervalMs ?? options.windowMs;
  const nowMs = options.now ?? (() => Date.now());
  let lastSweepAt = nowMs();

  function sweepExpiredBuckets(now: number): void {
    if (now - lastSweepAt < sweepIntervalMs && buckets.size <= options.maxBuckets) {
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
    // This is a memory bound, not a fairness guarantee. Insertion-order
    // eviction is sufficient here; use Redis/edge rate limiting if true
    // distributed LRU matters.
    while (buckets.size >= options.maxBuckets) {
      const oldestKey = buckets.keys().next().value as string | undefined;
      if (!oldestKey) {
        return;
      }
      buckets.delete(oldestKey);
    }
  }

  const rateLimit = ((input: TInput): InMemoryKeyedRateLimitResult => {
    const now = nowMs();
    sweepExpiredBuckets(now);

    const key = getKey(input);
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
  }) as InMemoryKeyedRateLimiter<TInput>;

  rateLimit.getBucketCount = () => buckets.size;

  return rateLimit;
}
