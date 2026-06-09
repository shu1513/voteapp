export const CENSUS_ADDRESS_GEOCODER_URL =
  "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress";

export const DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK = "Public_AR_Current";
export const DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE = "ACS2024_Current";
export const DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS = "all";
export const DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS = 30_000;

export type CensusAddressGeocoderErrorCode =
  | "invalid_address"
  | "not_found"
  | "http_error"
  | "bad_response"
  | "timeout";

export class CensusAddressGeocoderError extends Error {
  readonly code: CensusAddressGeocoderErrorCode;

  constructor(code: CensusAddressGeocoderErrorCode, message: string) {
    super(message);
    this.name = "CensusAddressGeocoderError";
    this.code = code;
  }
}

export type CensusAddressGeocoderOptions = {
  benchmark?: string;
  vintage?: string;
  layers?: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
};

export type CensusAddressCoordinates = {
  lat: number;
  lng: number;
};

export type CensusAddressGeocodeResult = {
  matched_address: string;
  coordinates: CensusAddressCoordinates;
  geographies: unknown;
  address_match_count: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function truncate(text: string, max = 500): string {
  const trimmed = text.trim();
  if (trimmed.length <= max) {
    return trimmed;
  }
  return `${trimmed.slice(0, max)}...`;
}

function normalizeRequiredString(value: string | undefined, fallback: string, fieldName: string): string {
  const normalized = (value ?? fallback).trim();
  if (normalized.length === 0) {
    throw new CensusAddressGeocoderError("invalid_address", `${fieldName} must not be empty`);
  }
  return normalized;
}

function normalizeTimeoutMs(value: number | undefined): number {
  const timeoutMs = value ?? DEFAULT_CENSUS_ADDRESS_GEOCODER_TIMEOUT_MS;
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new CensusAddressGeocoderError("invalid_address", `timeoutMs must be a positive integer, got ${timeoutMs}`);
  }
  return timeoutMs;
}

function isAbortError(error: unknown): boolean {
  return (
    (error instanceof DOMException && error.name === "AbortError") ||
    (error instanceof Error && error.name === "AbortError")
  );
}

export function buildCensusAddressGeocoderUrl(address: string, options: CensusAddressGeocoderOptions = {}): string {
  const normalizedAddress = address.trim();
  if (normalizedAddress.length === 0) {
    throw new CensusAddressGeocoderError("invalid_address", "address must not be empty");
  }

  const benchmark = normalizeRequiredString(
    options.benchmark,
    DEFAULT_CENSUS_ADDRESS_GEOCODER_BENCHMARK,
    "benchmark"
  );
  const vintage = normalizeRequiredString(options.vintage, DEFAULT_CENSUS_ADDRESS_GEOCODER_VINTAGE, "vintage");
  const layers = normalizeRequiredString(options.layers, DEFAULT_CENSUS_ADDRESS_GEOCODER_LAYERS, "layers");

  const url = new URL(CENSUS_ADDRESS_GEOCODER_URL);
  url.searchParams.set("address", normalizedAddress);
  url.searchParams.set("benchmark", benchmark);
  url.searchParams.set("vintage", vintage);
  url.searchParams.set("layers", layers);
  url.searchParams.set("format", "json");
  return url.toString();
}

function parseCoordinates(value: unknown): CensusAddressCoordinates | null {
  if (!isRecord(value)) {
    return null;
  }
  const lng = typeof value.x === "number" ? value.x : Number.NaN;
  const lat = typeof value.y === "number" ? value.y : Number.NaN;
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return null;
  }
  return { lat, lng };
}

export function parseCensusAddressGeocoderPayload(payload: unknown): CensusAddressGeocodeResult {
  if (!isRecord(payload) || !isRecord(payload.result)) {
    throw new CensusAddressGeocoderError("bad_response", "Census geocoder response is missing result object");
  }

  const matches = payload.result.addressMatches;
  if (!Array.isArray(matches)) {
    throw new CensusAddressGeocoderError("bad_response", "Census geocoder response is missing addressMatches array");
  }
  if (matches.length === 0) {
    throw new CensusAddressGeocoderError("not_found", "Census geocoder could not locate the address");
  }

  const firstMatch = matches[0];
  if (!isRecord(firstMatch)) {
    throw new CensusAddressGeocoderError("bad_response", "Census geocoder first address match is malformed");
  }

  const matchedAddress = typeof firstMatch.matchedAddress === "string" ? firstMatch.matchedAddress.trim() : "";
  if (matchedAddress.length === 0) {
    throw new CensusAddressGeocoderError("bad_response", "Census geocoder first address match is missing matchedAddress");
  }

  const coordinates = parseCoordinates(firstMatch.coordinates);
  if (!coordinates) {
    throw new CensusAddressGeocoderError("bad_response", "Census geocoder first address match is missing coordinates");
  }

  if (!isRecord(firstMatch.geographies)) {
    throw new CensusAddressGeocoderError("bad_response", "Census geocoder first address match is missing geographies");
  }

  return {
    matched_address: matchedAddress,
    coordinates,
    geographies: firstMatch.geographies,
    address_match_count: matches.length,
  };
}

export async function geocodeAddressWithCensus(
  address: string,
  options: CensusAddressGeocoderOptions = {}
): Promise<CensusAddressGeocodeResult> {
  const timeoutMs = normalizeTimeoutMs(options.timeoutMs);
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetchImpl(buildCensusAddressGeocoderUrl(address, options), {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
    });
    const bodyText = await response.text();

    if (!response.ok) {
      throw new CensusAddressGeocoderError(
        "http_error",
        `Census geocoder request failed: status=${response.status} ${response.statusText}; body=${truncate(bodyText)}`
      );
    }

    let payload: unknown;
    try {
      payload = JSON.parse(bodyText) as unknown;
    } catch {
      throw new CensusAddressGeocoderError(
        "bad_response",
        `Census geocoder returned non-JSON response; body=${truncate(bodyText)}`
      );
    }

    return parseCensusAddressGeocoderPayload(payload);
  } catch (error) {
    if (isAbortError(error)) {
      throw new CensusAddressGeocoderError(
        "timeout",
        `Census geocoder request timed out after ${timeoutMs}ms`
      );
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}
