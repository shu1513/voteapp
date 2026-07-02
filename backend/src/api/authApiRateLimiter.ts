import type { AuthApiRateLimitInput, AuthApiRateLimitResult } from "./addressApiTypes.js";

export const DEFAULT_AUTH_API_RATE_LIMIT_WINDOW_MS = 15 * 60_000;
export const DEFAULT_AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_IP = 10;
export const DEFAULT_AUTH_API_RATE_LIMIT_MAX_REQUESTS_PER_EMAIL = 5;
export const DEFAULT_AUTH_API_RATE_LIMIT_MAX_BUCKETS = 10_000;

type RateLimitBucket = {
  windowStartedAt: number;
  count: number;
};

export type InMemoryAuthApiRateLimiterOptions = {
  windowMs: number;
  maxRequestsPerIp: number;
  maxRequestsPerEmail: number;
  maxBuckets?: number;
  sweepIntervalMs?: number;
  now?: () => number;
};

export type InMemoryAuthApiRateLimiter = ((input: AuthApiRateLimitInput) => AuthApiRateLimitResult) & {
  getBucketCount: () => number;
};

function normalizeEmail(email: string): string {
  const normalized = email.trim().toLowerCase();
  if (normalized.length === 0) {
    throw new TypeError("Email must be a non-empty string");
  }
  return normalized;
}

function buildBucketKey(kind: "ip" | "email", value: string, pathname: string): string {
  return `${kind}:${value}|${pathname}`;
}

function createBucket(now: number): RateLimitBucket {
  return { windowStartedAt: now, count: 1 };
}

export function createInMemoryAuthApiRateLimiter(
  options: InMemoryAuthApiRateLimiterOptions
): InMemoryAuthApiRateLimiter {
  const buckets = new Map<string, RateLimitBucket>();
  const maxBuckets = options.maxBuckets ?? DEFAULT_AUTH_API_RATE_LIMIT_MAX_BUCKETS;
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

  function evictOldestUntilUnderCap(missingSlotCount: number): void {
    while (buckets.size + missingSlotCount > maxBuckets) {
      const oldestKey = buckets.keys().next().value as string | undefined;
      if (!oldestKey) {
        return;
      }
      buckets.delete(oldestKey);
    }
  }

  function getRetryAfterSeconds(bucket: RateLimitBucket, now: number): number {
    return Math.max(1, Math.ceil((options.windowMs - (now - bucket.windowStartedAt)) / 1000));
  }

  const rateLimit = ((input: AuthApiRateLimitInput): AuthApiRateLimitResult => {
    const now = nowMs();
    sweepExpiredBuckets(now);

    const normalizedEmail = normalizeEmail(input.email);
    const ipKey = buildBucketKey("ip", input.clientIp || "unknown", input.pathname);
    const emailKey = buildBucketKey("email", normalizedEmail, input.pathname);
    const existingIpBucket = buckets.get(ipKey);
    const existingEmailBucket = buckets.get(emailKey);

    const ipIsActive = existingIpBucket !== undefined && now - existingIpBucket.windowStartedAt < options.windowMs;
    const emailIsActive =
      existingEmailBucket !== undefined && now - existingEmailBucket.windowStartedAt < options.windowMs;

    if (ipIsActive && existingIpBucket.count >= options.maxRequestsPerIp) {
      return {
        allowed: false,
        retryAfterSeconds: getRetryAfterSeconds(existingIpBucket, now),
      };
    }
    if (emailIsActive && existingEmailBucket.count >= options.maxRequestsPerEmail) {
      return {
        allowed: false,
        retryAfterSeconds: getRetryAfterSeconds(existingEmailBucket, now),
      };
    }

    const missingSlotCount = Number(!existingIpBucket) + Number(!existingEmailBucket);
    if (missingSlotCount > 0) {
      evictOldestUntilUnderCap(missingSlotCount);
    }

    const refreshedIpBucket =
      existingIpBucket && now - existingIpBucket.windowStartedAt < options.windowMs
        ? existingIpBucket
        : createBucket(now);
    const refreshedEmailBucket =
      existingEmailBucket && now - existingEmailBucket.windowStartedAt < options.windowMs
        ? existingEmailBucket
        : createBucket(now);

    refreshedIpBucket.count += existingIpBucket && now - existingIpBucket.windowStartedAt < options.windowMs ? 1 : 0;
    refreshedEmailBucket.count +=
      existingEmailBucket && now - existingEmailBucket.windowStartedAt < options.windowMs ? 1 : 0;

    buckets.set(ipKey, refreshedIpBucket);
    buckets.set(emailKey, refreshedEmailBucket);

    return { allowed: true };
  }) as InMemoryAuthApiRateLimiter;

  rateLimit.getBucketCount = () => buckets.size;

  return rateLimit;
}
