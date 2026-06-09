import { createServer, type IncomingMessage, type RequestListener, type Server, type ServerResponse } from "node:http";
import type { BallotLookupResult } from "../pipeline/address/ballotLookup.js";
import type { AddressResolutionResult } from "../pipeline/address/addressResolverService.js";
import type { SaveUserDistrictsResult } from "../pipeline/address/userDistricts.js";
import { CensusAddressGeocoderError } from "../pipeline/address/censusAddressGeocoder.js";
import type { AddressApiClientIpInput } from "./addressApiClientIp.js";

export const ADDRESS_RESOLVE_PATH = "/api/address/resolve";
export const BALLOT_LOOKUP_PATH = "/api/ballot";
const MAX_ADDRESS_REQUEST_BODY_BYTES = 16 * 1024;
const MAX_BALLOT_DISTRICT_IDS = 50;
const CORS_ALLOW_METHODS = "GET, POST, OPTIONS";
const CORS_ALLOW_HEADERS = "content-type";
const CORS_MAX_AGE_SECONDS = "600";
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export type AddressApiRateLimitInput = {
  clientIp: string;
  method: string;
  pathname: string;
};

export type AddressApiRateLimitResult = {
  allowed: boolean;
  retryAfterSeconds?: number;
};

export type AddressApiServerOptions = {
  resolveAddress: (address: string) => Promise<AddressResolutionResult>;
  lookupBallot?: (districtIds: readonly string[]) => Promise<BallotLookupResult>;
  resolveUserId?: (headers: Record<string, string | string[] | undefined> | undefined) => string | null | undefined;
  saveUserDistricts?: (
    userId: string,
    districts: AddressResolutionResult["districts"]
  ) => Promise<SaveUserDistrictsResult>;
  allowedOrigins?: readonly string[];
  logDiagnostics?: (diagnostics: AddressResolutionDiagnostics) => void;
  rateLimit?: (input: AddressApiRateLimitInput) => AddressApiRateLimitResult;
  resolveClientIp?: (input: AddressApiClientIpInput) => string;
};

export type AddressResolvePayload = {
  address: string;
  include_ballot: boolean;
  save_districts: boolean;
};

export type AddressApiRequestInput = {
  method: string;
  path: string;
  rawBody: string;
  headers?: Record<string, string | string[] | undefined>;
  clientIp?: string;
};

export type AddressApiResponse = {
  statusCode: number;
  headers: Record<string, string>;
  body?: unknown;
};

export type PublicAddressResolutionResult = {
  matched_address: string;
  coordinates: {
    lat: number;
    lng: number;
  };
  districts: AddressResolutionResult["districts"];
  ballot?: BallotLookupResult;
  saved_user_districts?: {
    district_count: number;
  };
};

export type AddressResolutionDiagnostics = {
  address_match_count: number;
  district_keys: AddressResolutionResult["district_keys"];
  missing_district_keys: AddressResolutionResult["missing_district_keys"];
  warnings: AddressResolutionResult["warnings"];
};

type ApiErrorCode =
  | "not_found"
  | "method_not_allowed"
  | "invalid_json"
  | "invalid_request"
  | "auth_required"
  | "rate_limited"
  | "address_not_found"
  | "upstream_unavailable"
  | "bad_upstream_response"
  | "internal_error";

type ApiErrorBody = {
  error: {
    code: ApiErrorCode;
    message: string;
  };
};

function writeJson(response: ServerResponse, statusCode: number, body: unknown, extraHeaders: Record<string, string> = {}): void {
  const serialized = JSON.stringify(body);
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(serialized).toString(),
    ...extraHeaders,
  });
  response.end(serialized);
}

function toJsonResponse(statusCode: number, body: unknown, extraHeaders: Record<string, string> = {}): AddressApiResponse {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...extraHeaders,
    },
    body,
  };
}

function toEmptyResponse(statusCode: number, extraHeaders: Record<string, string> = {}): AddressApiResponse {
  return {
    statusCode,
    headers: extraHeaders,
  };
}

function toErrorResponse(
  statusCode: number,
  code: ApiErrorCode,
  message: string,
  extraHeaders: Record<string, string> = {}
): AddressApiResponse {
  return toJsonResponse(
    statusCode,
    {
      error: {
        code,
        message,
      },
    } satisfies ApiErrorBody,
    extraHeaders
  );
}

function writeError(
  response: ServerResponse,
  statusCode: number,
  code: ApiErrorCode,
  message: string,
  extraHeaders: Record<string, string> = {}
): void {
  writeJson(response, statusCode, {
    error: {
      code,
      message,
    },
  } satisfies ApiErrorBody, extraHeaders);
}

function writeApiResponse(response: ServerResponse, apiResponse: AddressApiResponse): void {
  if (apiResponse.body === undefined) {
    response.writeHead(apiResponse.statusCode, apiResponse.headers);
    response.end();
    return;
  }
  writeJson(response, apiResponse.statusCode, apiResponse.body, apiResponse.headers);
}

async function readRequestBody(request: IncomingMessage, maxBytes: number): Promise<string> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;

  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    totalBytes += buffer.byteLength;
    if (totalBytes > maxBytes) {
      throw new Error(`request body exceeds ${maxBytes} bytes`);
    }
    chunks.push(buffer);
  }

  return Buffer.concat(chunks).toString("utf8");
}

function parseBooleanField(value: unknown, fieldName: string): boolean {
  if (value === undefined) {
    return false;
  }
  if (typeof value !== "boolean") {
    throw new TypeError(`Request field ${fieldName} must be boolean when provided`);
  }
  return value;
}

function parseAddressPayload(rawBody: string): AddressResolvePayload {
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawBody);
  } catch {
    throw new SyntaxError("Request body must be valid JSON");
  }

  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new TypeError("Request body must be a JSON object");
  }

  const address = (parsed as { address?: unknown }).address;
  if (typeof address !== "string" || address.trim().length === 0) {
    throw new TypeError("Request body must include non-empty string field: address");
  }

  return {
    address: address.trim(),
    include_ballot: parseBooleanField((parsed as { include_ballot?: unknown }).include_ballot, "include_ballot"),
    save_districts: parseBooleanField((parsed as { save_districts?: unknown }).save_districts, "save_districts"),
  };
}

function parseDistrictIds(url: URL): string[] {
  const rawValues = url.searchParams
    .getAll("district_ids")
    .flatMap((value) => value.split(","))
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  const districtIds = [...new Set(rawValues)];
  if (districtIds.length === 0) {
    throw new TypeError("Query parameter district_ids must include at least one district UUID");
  }
  if (districtIds.length > MAX_BALLOT_DISTRICT_IDS) {
    throw new TypeError(`Query parameter district_ids supports at most ${MAX_BALLOT_DISTRICT_IDS} UUIDs`);
  }
  const invalidId = districtIds.find((id) => !UUID_PATTERN.test(id));
  if (invalidId) {
    throw new TypeError(`Query parameter district_ids contains invalid UUID: ${invalidId}`);
  }
  return districtIds;
}

function normalizeAllowedOrigins(origins: readonly string[] | undefined): Set<string> {
  return new Set((origins ?? []).map((origin) => origin.trim()).filter((origin) => origin.length > 0));
}

function readHeader(
  headers: Record<string, string | string[] | undefined> | undefined,
  name: string
): string | undefined {
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

function resolveCorsHeaders(
  headers: Record<string, string | string[] | undefined> | undefined,
  allowedOrigins: readonly string[] | undefined
): { ok: boolean; headers: Record<string, string> } {
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
      "access-control-allow-methods": CORS_ALLOW_METHODS,
      "access-control-allow-headers": CORS_ALLOW_HEADERS,
      "access-control-max-age": CORS_MAX_AGE_SECONDS,
      vary: "Origin",
    },
  };
}

function toPublicAddressResolution(
  result: AddressResolutionResult,
  extras: Pick<PublicAddressResolutionResult, "ballot" | "saved_user_districts"> = {}
): PublicAddressResolutionResult {
  return {
    matched_address: result.matched_address,
    coordinates: result.coordinates,
    districts: result.districts,
    ...extras,
  };
}

function toAddressResolutionDiagnostics(result: AddressResolutionResult): AddressResolutionDiagnostics {
  return {
    address_match_count: result.address_match_count,
    district_keys: result.district_keys,
    missing_district_keys: result.missing_district_keys,
    warnings: result.warnings,
  };
}

function mapErrorToResponse(error: unknown): { statusCode: number; code: ApiErrorCode; message: string } {
  if (error instanceof SyntaxError) {
    return { statusCode: 400, code: "invalid_json", message: error.message };
  }
  if (error instanceof TypeError) {
    return { statusCode: 400, code: "invalid_request", message: error.message };
  }
  if (error instanceof CensusAddressGeocoderError) {
    if (error.code === "invalid_address") {
      return { statusCode: 400, code: "invalid_request", message: error.message };
    }
    if (error.code === "not_found") {
      return { statusCode: 422, code: "address_not_found", message: error.message };
    }
    if (error.code === "bad_response") {
      return { statusCode: 502, code: "bad_upstream_response", message: error.message };
    }
    if (error.code === "timeout" || error.code === "http_error") {
      return { statusCode: 503, code: "upstream_unavailable", message: error.message };
    }
  }
  if (error instanceof Error && error.message.startsWith("request body exceeds")) {
    return { statusCode: 413, code: "invalid_request", message: error.message };
  }
  const message = error instanceof Error ? error.message : String(error);
  return { statusCode: 500, code: "internal_error", message };
}

export async function handleAddressApiRequest(
  input: AddressApiRequestInput,
  options: AddressApiServerOptions
): Promise<AddressApiResponse> {
  const url = new URL(input.path, "http://localhost");
  const cors = resolveCorsHeaders(input.headers, options.allowedOrigins);
  if (url.pathname !== ADDRESS_RESOLVE_PATH && url.pathname !== BALLOT_LOOKUP_PATH) {
    return toErrorResponse(404, "not_found", "Not found", cors.headers);
  }

  if (!cors.ok) {
    return toErrorResponse(403, "invalid_request", "Origin is not allowed", cors.headers);
  }

  if (input.method === "OPTIONS") {
    return toEmptyResponse(204, cors.headers);
  }

  if (options.rateLimit) {
    const rateLimit = options.rateLimit({
      clientIp: input.clientIp ?? "unknown",
      method: input.method,
      pathname: url.pathname,
    });
    if (!rateLimit.allowed) {
      const retryAfterSeconds = Math.max(1, Math.ceil(rateLimit.retryAfterSeconds ?? 1));
      return toErrorResponse(429, "rate_limited", "Too many requests. Try again later.", {
        ...cors.headers,
        "retry-after": String(retryAfterSeconds),
      });
    }
  }

  if (url.pathname === BALLOT_LOOKUP_PATH) {
    if (input.method !== "GET") {
      return toErrorResponse(405, "method_not_allowed", "Use GET /api/ballot?district_ids=...", {
        ...cors.headers,
        allow: "GET",
      });
    }
    if (!options.lookupBallot) {
      return toErrorResponse(500, "internal_error", "Ballot lookup is not configured", cors.headers);
    }

    try {
      const districtIds = parseDistrictIds(url);
      const result = await options.lookupBallot(districtIds);
      return toJsonResponse(200, result, cors.headers);
    } catch (error) {
      const mapped = mapErrorToResponse(error);
      return toErrorResponse(mapped.statusCode, mapped.code, mapped.message, cors.headers);
    }
  }

  if (input.method !== "POST") {
    return toErrorResponse(405, "method_not_allowed", "Use POST /api/address/resolve", {
      ...cors.headers,
      allow: "POST",
    });
  }

  try {
    const payload = parseAddressPayload(input.rawBody);
    const result = await options.resolveAddress(payload.address);
    options.logDiagnostics?.(toAddressResolutionDiagnostics(result));

    const districtIds = result.districts.map((district) => district.id);
    const extras: Pick<PublicAddressResolutionResult, "ballot" | "saved_user_districts"> = {};

    if (payload.include_ballot) {
      if (!options.lookupBallot) {
        return toErrorResponse(500, "internal_error", "Ballot lookup is not configured", cors.headers);
      }
      extras.ballot = await options.lookupBallot(districtIds);
    }

    if (payload.save_districts) {
      const userId = options.resolveUserId?.(input.headers)?.trim() || null;
      if (!userId) {
        return toErrorResponse(401, "auth_required", "Login is required to save user districts", cors.headers);
      }
      if (!options.saveUserDistricts) {
        return toErrorResponse(500, "internal_error", "User district saving is not configured", cors.headers);
      }
      const saved = await options.saveUserDistricts(userId, result.districts);
      extras.saved_user_districts = { district_count: saved.district_count };
    }

    return toJsonResponse(200, toPublicAddressResolution(result, extras), cors.headers);
  } catch (error) {
    const mapped = mapErrorToResponse(error);
    return toErrorResponse(mapped.statusCode, mapped.code, mapped.message, cors.headers);
  }
}

export function createAddressApiRequestHandler(options: AddressApiServerOptions): RequestListener {
  return (request, response) => {
    void (async (): Promise<void> => {
      try {
        const method = request.method ?? "GET";
        const path = request.url ?? "/";
        const remoteAddress = request.socket.remoteAddress ?? "unknown";
        const clientIp =
          options.resolveClientIp?.({ headers: request.headers, remoteAddress })?.trim() || remoteAddress;
        if (new URL(path, "http://localhost").pathname !== ADDRESS_RESOLVE_PATH || method !== "POST") {
          writeApiResponse(
            response,
            await handleAddressApiRequest({ method, path, rawBody: "", headers: request.headers, clientIp }, options)
          );
          return;
        }

        const rawBody = await readRequestBody(request, MAX_ADDRESS_REQUEST_BODY_BYTES);
        writeApiResponse(
          response,
          await handleAddressApiRequest({ method, path, rawBody, headers: request.headers, clientIp }, options)
        );
      } catch (error) {
        const mapped = mapErrorToResponse(error);
        writeError(response, mapped.statusCode, mapped.code, mapped.message);
      }
    })();
  };
}

export function createAddressApiServer(options: AddressApiServerOptions): Server {
  return createServer(createAddressApiRequestHandler(options));
}
