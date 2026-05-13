const CENSUS_HOSTNAME = "api.census.gov";

function truncate(text: string, max = 200): string {
  const trimmed = text.trim();
  if (trimmed.length <= max) {
    return trimmed;
  }
  return `${trimmed.slice(0, max)}...`;
}

function isLikelyMissingKeyResponse(status: number, bodyText: string): boolean {
  if (status === 302) {
    return true;
  }
  const normalized = bodyText.toLowerCase();
  return normalized.includes("missing key") || normalized.includes("must be included with each data api request");
}

function isLikelyHtml(bodyText: string): boolean {
  return bodyText.trimStart().startsWith("<");
}

/**
 * Reads configured Census API keys from environment, in explicit priority order.
 * Empty values are ignored and duplicates are removed.
 */
export function readCensusApiKeysFromEnv(env: NodeJS.ProcessEnv = process.env): string[] {
  const rawCandidates = [
    env.CENSUS_API_KEY_1,
    env.CENSUS_API_KEY_2,
    env.CENSUS_API_KEY_3,
    env.CENSUS_API_KEY,
  ];

  const normalized = rawCandidates
    .map((value) => value?.trim())
    .filter((value): value is string => Boolean(value && value.length > 0));

  return [...new Set(normalized)];
}

/**
 * Returns a Census URL with key query param applied (without mutating caller input).
 */
export function withCensusApiKey(baseUrl: string, apiKey: string): string {
  const parsed = new URL(baseUrl);
  if (parsed.hostname !== CENSUS_HOSTNAME) {
    throw new Error(`Expected Census hostname ${CENSUS_HOSTNAME}, got ${parsed.hostname}`);
  }
  parsed.searchParams.set("key", apiKey);
  return parsed.toString();
}

/**
 * Fetches JSON from Census using key rotation. If one key fails due key/rate/upstream issues,
 * it automatically tries the next key before failing.
 */
export async function fetchCensusJsonWithKeyRotation(
  baseUrl: string,
  apiKeys: readonly string[],
  signal?: AbortSignal
): Promise<unknown> {
  if (apiKeys.length === 0) {
    throw new Error("No Census API keys configured. Set CENSUS_API_KEY_1 (and optionally _2/_3).");
  }

  let lastError: Error | null = null;

  for (let index = 0; index < apiKeys.length; index += 1) {
    const apiKey = apiKeys[index];
    const attemptLabel = `key_${index + 1}`;

    try {
      const response = await fetch(withCensusApiKey(baseUrl, apiKey), {
        method: "GET",
        redirect: "follow",
        signal,
      });
      const bodyText = await response.text();

      if (!response.ok) {
        const retryable =
          response.status === 429 ||
          response.status >= 500 ||
          isLikelyMissingKeyResponse(response.status, bodyText);

        const error = new Error(
          `Census API request failed via ${attemptLabel}: status=${response.status} ${response.statusText}; body=${truncate(bodyText)}`
        );

        if (retryable && index < apiKeys.length - 1) {
          lastError = error;
          continue;
        }
        throw error;
      }

      if (isLikelyHtml(bodyText)) {
        const error = new Error(
          `Census API returned HTML via ${attemptLabel} instead of JSON; body=${truncate(bodyText)}`
        );
        if (index < apiKeys.length - 1) {
          lastError = error;
          continue;
        }
        throw error;
      }

      try {
        return JSON.parse(bodyText) as unknown;
      } catch {
        const error = new Error(
          `Census API returned non-JSON via ${attemptLabel}; body=${truncate(bodyText)}`
        );
        if (index < apiKeys.length - 1) {
          lastError = error;
          continue;
        }
        throw error;
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        throw error;
      }

      const normalized = error instanceof Error ? error : new Error(String(error));
      if (index < apiKeys.length - 1) {
        lastError = normalized;
        continue;
      }
      throw normalized;
    }
  }

  throw lastError ?? new Error("Census API request failed across all configured keys.");
}
