type ProviderRateLimitKey = `${string}:${string}`;

const blockedUntilByProviderModel = new Map<ProviderRateLimitKey, number>();

function keyFor(provider: string, model: string): ProviderRateLimitKey {
  return `${provider}:${model}`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterToMs(retryAfterHeader: string | null): number | null {
  if (!retryAfterHeader) {
    return null;
  }

  const trimmed = retryAfterHeader.trim();
  if (trimmed.length === 0) {
    return null;
  }

  const seconds = Number.parseFloat(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.ceil(seconds * 1000);
  }

  const dateMs = Date.parse(trimmed);
  if (!Number.isFinite(dateMs)) {
    return null;
  }

  const deltaMs = dateMs - Date.now();
  return deltaMs > 0 ? deltaMs : 0;
}

function parseRfc3339ResetToMs(resetHeader: string | null): number | null {
  if (!resetHeader) {
    return null;
  }
  const parsed = Date.parse(resetHeader);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  const deltaMs = parsed - Date.now();
  return deltaMs > 0 ? deltaMs : 0;
}

function parseRoundedInteger(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return parsed;
}

function bumpBlockedUntil(provider: string, model: string, msFromNow: number): void {
  const key = keyFor(provider, model);
  const now = Date.now();
  const candidate = now + Math.max(0, msFromNow);
  const existing = blockedUntilByProviderModel.get(key) ?? 0;
  blockedUntilByProviderModel.set(key, Math.max(existing, candidate));
}

export async function waitForProviderModelCooldown(provider: string, model: string): Promise<void> {
  const key = keyFor(provider, model);
  const blockedUntil = blockedUntilByProviderModel.get(key);
  if (!blockedUntil) {
    return;
  }

  const waitMs = blockedUntil - Date.now();
  if (waitMs <= 0) {
    blockedUntilByProviderModel.delete(key);
    return;
  }

  await sleep(waitMs);
}

/**
 * Updates per-provider/model cooldown from rate-limit response headers.
 * This is intentionally generic so it can be reused by different providers.
 */
export function updateProviderModelCooldownFromHeaders(
  provider: string,
  model: string,
  headers: Headers,
  options?: { onRateLimitedResponse?: boolean }
): void {
  const retryAfterMs = parseRetryAfterToMs(headers.get("retry-after"));
  if (retryAfterMs !== null) {
    bumpBlockedUntil(provider, model, retryAfterMs);
  }

  const inputRemaining = parseRoundedInteger(headers.get("anthropic-ratelimit-input-tokens-remaining"));
  const inputResetMs = parseRfc3339ResetToMs(headers.get("anthropic-ratelimit-input-tokens-reset"));
  if (inputRemaining !== null && inputRemaining <= 0 && inputResetMs !== null) {
    bumpBlockedUntil(provider, model, inputResetMs);
  }

  if (options?.onRateLimitedResponse && retryAfterMs === null && inputResetMs === null) {
    // Last-resort tiny guard if provider omitted expected headers on 429.
    bumpBlockedUntil(provider, model, 1000);
  }
}

/**
 * Extracts provider rate-limit and request trace headers for debugging/persistence.
 */
export function extractProviderRateLimitDebugHeaders(headers: Headers): Record<string, string> {
  const keys = [
    "retry-after",
    "request-id",
    "x-request-id",
    "anthropic-ratelimit-requests-limit",
    "anthropic-ratelimit-requests-remaining",
    "anthropic-ratelimit-requests-reset",
    "anthropic-ratelimit-input-tokens-limit",
    "anthropic-ratelimit-input-tokens-remaining",
    "anthropic-ratelimit-input-tokens-reset",
    "anthropic-ratelimit-output-tokens-limit",
    "anthropic-ratelimit-output-tokens-remaining",
    "anthropic-ratelimit-output-tokens-reset",
  ] as const;

  const output: Record<string, string> = {};
  for (const key of keys) {
    const value = headers.get(key);
    if (value && value.trim().length > 0) {
      output[key] = value.trim();
    }
  }

  return output;
}
