export const CORS_ALLOW_METHODS = "GET, POST, PUT, OPTIONS";
export const CORS_ALLOW_HEADERS = "content-type";
export const CORS_MAX_AGE_SECONDS = "600";

export type HeaderRecord = Record<string, string | string[] | undefined>;

export type CorsResolution = {
  ok: boolean;
  headers: Record<string, string>;
};

function normalizeAllowedOrigins(origins: readonly string[] | undefined): Set<string> {
  return new Set((origins ?? []).map((origin) => origin.trim()).filter((origin) => origin.length > 0));
}

export function readHeader(headers: HeaderRecord | undefined, name: string): string | undefined {
  if (!headers) {
    return undefined;
  }
  const lowerName = name.toLowerCase();
  const value = headers[lowerName] ?? headers[name];
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

export function resolveCorsHeaders(
  headers: HeaderRecord | undefined,
  allowedOrigins: readonly string[] | undefined
): CorsResolution {
  const origin = readHeader(headers, "origin")?.trim();
  if (!origin) {
    return { ok: true, headers: {} };
  }

  const normalizedAllowedOrigins = normalizeAllowedOrigins(allowedOrigins);
  const allowAnyOrigin = normalizedAllowedOrigins.has("*");
  if (!allowAnyOrigin && !normalizedAllowedOrigins.has(origin)) {
    return { ok: false, headers: { vary: "Origin" } };
  }

  return {
    ok: true,
    headers: {
      "access-control-allow-origin": allowAnyOrigin ? "*" : origin,
      ...(allowAnyOrigin ? {} : { "access-control-allow-credentials": "true" }),
      "access-control-allow-methods": CORS_ALLOW_METHODS,
      "access-control-allow-headers": CORS_ALLOW_HEADERS,
      "access-control-max-age": CORS_MAX_AGE_SECONDS,
      vary: "Origin",
    },
  };
}
