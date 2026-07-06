import type { AddressApiRateLimitInput, AddressApiRateLimitResult } from "./addressApiTypes.js";
import { createInMemoryKeyedRateLimiter } from "./inMemoryRateLimiter.js";

export const DEFAULT_ADDRESS_API_RATE_LIMIT_WINDOW_MS = 60_000;
export const DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_REQUESTS = 60;
export const DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS = 10_000;

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
  return createInMemoryKeyedRateLimiter<AddressApiRateLimitInput>(
    {
      ...options,
      maxBuckets: options.maxBuckets ?? DEFAULT_ADDRESS_API_RATE_LIMIT_MAX_BUCKETS,
    },
    (input) => input.clientIp || "unknown"
  ) as InMemoryAddressApiRateLimiter;
}
