import { createServer, type IncomingMessage, type RequestListener, type Server, type ServerResponse } from "node:http";
import type { AddressResolutionResult } from "../pipeline/address/addressResolverService.js";
import { CensusAddressGeocoderError } from "../pipeline/address/censusAddressGeocoder.js";

export const ADDRESS_RESOLVE_PATH = "/api/address/resolve";
const MAX_ADDRESS_REQUEST_BODY_BYTES = 16 * 1024;
const CORS_ALLOW_METHODS = "POST, OPTIONS";
const CORS_ALLOW_HEADERS = "content-type";
const CORS_MAX_AGE_SECONDS = "600";

export type AddressApiServerOptions = {
  resolveAddress: (address: string) => Promise<AddressResolutionResult>;
  allowedOrigins?: readonly string[];
  logDiagnostics?: (diagnostics: AddressResolutionDiagnostics) => void;
};

export type AddressApiRequestInput = {
  method: string;
  path: string;
  rawBody: string;
  headers?: Record<string, string | string[] | undefined>;
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

function parseAddressPayload(rawBody: string): string {
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

  return address.trim();
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

function toPublicAddressResolution(result: AddressResolutionResult): PublicAddressResolutionResult {
  return {
    matched_address: result.matched_address,
    coordinates: result.coordinates,
    districts: result.districts,
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
  if (url.pathname !== ADDRESS_RESOLVE_PATH) {
    return toErrorResponse(404, "not_found", "Not found", cors.headers);
  }

  if (!cors.ok) {
    return toErrorResponse(403, "invalid_request", "Origin is not allowed", cors.headers);
  }

  if (input.method === "OPTIONS") {
    return toEmptyResponse(204, cors.headers);
  }

  if (input.method !== "POST") {
    return toErrorResponse(405, "method_not_allowed", "Use POST /api/address/resolve", {
      ...cors.headers,
      allow: "POST",
    });
  }

  try {
    const address = parseAddressPayload(input.rawBody);
    const result = await options.resolveAddress(address);
    options.logDiagnostics?.(toAddressResolutionDiagnostics(result));
    return toJsonResponse(200, toPublicAddressResolution(result), cors.headers);
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
        if (new URL(path, "http://localhost").pathname !== ADDRESS_RESOLVE_PATH || method !== "POST") {
          writeApiResponse(response, await handleAddressApiRequest({ method, path, rawBody: "", headers: request.headers }, options));
          return;
        }

        const rawBody = await readRequestBody(request, MAX_ADDRESS_REQUEST_BODY_BYTES);
        writeApiResponse(response, await handleAddressApiRequest({ method, path, rawBody, headers: request.headers }, options));
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
