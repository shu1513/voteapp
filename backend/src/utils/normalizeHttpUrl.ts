type NormalizeHttpUrlOptions = {
  baseUrl?: string;
  stripHash?: boolean;
  stripTrailingSlash?: boolean;
};

/**
 * Normalizes an http(s) URL for stable comparisons.
 * Returns null for invalid or non-http(s) URLs.
 */
export function normalizeHttpUrl(raw: string, options: NormalizeHttpUrlOptions = {}): string | null {
  const {
    baseUrl,
    stripHash = true,
    stripTrailingSlash = true,
  } = options;

  try {
    const parsed = baseUrl ? new URL(raw, baseUrl) : new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return null;
    }

    if (stripHash) {
      parsed.hash = "";
    }

    const normalized = parsed.toString();
    if (stripTrailingSlash && normalized.endsWith("/")) {
      return normalized.slice(0, -1);
    }

    return normalized;
  } catch {
    return null;
  }
}
