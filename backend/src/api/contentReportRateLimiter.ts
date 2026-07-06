import type { AddressApiRateLimitInput, AddressApiRateLimitResult } from "./addressApiTypes.js";
import { createInMemoryKeyedRateLimiter } from "./inMemoryRateLimiter.js";

export const DEFAULT_CONTENT_REPORT_RATE_LIMIT_WINDOW_MS = 15 * 60_000;
export const DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_REQUESTS = 5;
export const DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_BUCKETS = 10_000;

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
  return createInMemoryKeyedRateLimiter<AddressApiRateLimitInput>(
    {
      ...options,
      maxBuckets: options.maxBuckets ?? DEFAULT_CONTENT_REPORT_RATE_LIMIT_MAX_BUCKETS,
    },
    (input) => input.clientIp || "unknown"
  ) as InMemoryContentReportRateLimiter;
}
