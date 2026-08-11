import type { AddressApiRateLimitInput, AddressApiRateLimitResult } from "./addressApiTypes.js";
import { ADDRESS_AUTOCOMPLETE_PATH, ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH } from "./apiValidation.js";
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
    // Autocomplete suggest fires roughly per keystroke (leading-edge +
    // 125ms debounce), so one long address entry can burn a whole per-IP
    // window. The suggest/retrieve pair gets its own bucket so keystroke
    // traffic can never starve the flow that must not 429 (resolve, ballot,
    // auth, page GETs). The cap is unchanged, so the per-IP ceiling on
    // billable Google calls is too; if the autocomplete bucket runs dry the
    // client swallows the 429s and the form still submits typed text.
    // The space separator cannot collide with an IP (v4 or v6).
    (input) => {
      const clientIp = input.clientIp || "unknown";
      const isAutocomplete =
        input.pathname === ADDRESS_AUTOCOMPLETE_PATH || input.pathname === ADDRESS_AUTOCOMPLETE_RETRIEVE_PATH;
      return isAutocomplete ? `${clientIp} autocomplete` : clientIp;
    }
  ) as InMemoryAddressApiRateLimiter;
}
