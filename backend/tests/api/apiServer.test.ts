import { type IncomingMessage, ServerResponse } from "node:http";
import { Readable, Writable } from "node:stream";
import express, { type Express } from "express";
import { describe, expect, it, vi } from "vitest";

import { createApiApp } from "../../src/api/apiServer.js";
import { MAX_INITIALIZE_DISTRICT_IDS } from "../../src/api/apiValidation.js";
import { CensusAddressGeocoderError } from "../../src/pipeline/address/censusAddressGeocoder.js";
import type { AddressResolutionResult } from "../../src/pipeline/address/addressResolverService.js";
import { UserCandidateFollowsError } from "../../src/pipeline/users/userCandidateFollows.js";
import { InitializeUserDistrictsError } from "../../src/pipeline/users/userDistrictInitializer.js";
import { UserDistrictReaderError } from "../../src/pipeline/users/userDistrictReader.js";
import { ReplaceUserDistrictsError } from "../../src/pipeline/users/userDistrictReplacer.js";
import { UserResearchAreaPreferencesError } from "../../src/pipeline/users/userResearchAreaPreferences.js";

const resolvedAddress: AddressResolutionResult = {
  matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
  coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
  address_match_count: 1,
  district_keys: [
    {
      district_type: "county",
      geoid_compact: "06037",
      source: "mtfcc",
      layer_name: "Counties",
      mtfcc: "G4020",
      name: "Los Angeles County",
    },
  ],
  districts: [
    {
      id: "district-la",
      district_type: "county",
      geoid_compact: "06037",
      name: "Los Angeles County",
      state: "CA",
      state_fips: "06",
      population: 9876482,
      representation_power_score: 12.3,
    },
  ],
  missing_district_keys: [],
  warnings: [],
};

const districtId = "11111111-1111-4111-8111-111111111111";
const electionId = "33333333-3333-4333-8333-333333333333";

function makeDistrictId(index: number): string {
  return `11111111-1111-4111-8111-${index.toString().padStart(12, "0")}`;
}

async function invokeExpressApp(
  app: Express,
  input: {
    method: string;
    path: string;
    body?: string;
    headers?: Record<string, string>;
    remoteAddress?: string;
  }
): Promise<{ statusCode: number; headers: Record<string, string>; body: unknown; rawBody: string }> {
  const requestBody = input.body ?? "";
  const headers = {
    ...(input.headers ?? {}),
    ...(requestBody.length > 0 && !input.headers?.["content-length"]
      ? { "content-length": Buffer.byteLength(requestBody).toString() }
      : {}),
  };
  const request = Readable.from(requestBody.length > 0 ? [requestBody] : []) as IncomingMessage;
  Object.assign(request, {
    method: input.method,
    url: input.path,
    headers,
    socket: {
      remoteAddress: input.remoteAddress ?? "127.0.0.1",
    },
  });

  const response = new ServerResponse(request);
  const responseChunks: Buffer[] = [];
  const socket = new Writable({
    write(chunk, _encoding, callback) {
      responseChunks.push(Buffer.from(chunk));
      callback();
    },
  });
  response.assignSocket(socket as never);

  return await new Promise((resolve, reject) => {
    response.on("finish", () => {
      const rawResponse = Buffer.concat(responseChunks).toString("utf8");
      const [, rawBody = ""] = rawResponse.split("\r\n\r\n");
      const body =
        rawBody.length > 0 && String(response.getHeader("content-type") ?? "").includes("application/json")
          ? JSON.parse(rawBody)
          : rawBody;
      const headers = Object.fromEntries(
        Object.entries(response.getHeaders()).map(([key, value]) => [key, String(value)])
      );
      resolve({
        statusCode: response.statusCode,
        headers,
        body,
        rawBody,
      });
    });
    response.on("error", reject);
    app(request, response);
  });
}

describe("createApiApp", () => {
  it("serves POST /api/address/resolve without leaking coordinates or ballot data", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const logDiagnostics = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, logDiagnostics }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      matched_address: resolvedAddress.matched_address,
      districts: resolvedAddress.districts,
    });
    expect(response.body).not.toHaveProperty("coordinates");
    expect(response.body).not.toHaveProperty("ballot");
    expect(resolveAddress).toHaveBeenCalledWith("3921 Harlan Ave Baldwin Park CA 91706");
    expect(logDiagnostics).toHaveBeenCalledWith({
      address_match_count: 1,
      district_keys: resolvedAddress.district_keys,
      missing_district_keys: [],
      warnings: [],
    });
  });

  it("keeps address resolve read-only even when user district initialization is configured", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/address/resolve",
        body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
        headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(initializeUserDistricts).not.toHaveBeenCalled();
    expect(resolveAuthenticatedUserId).not.toHaveBeenCalled();
  });

  it("does not fail successful address responses when diagnostics logging throws", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const logDiagnostics = vi.fn(() => {
      throw new Error("diagnostics sink failed");
    });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, logDiagnostics }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      matched_address: resolvedAddress.matched_address,
      districts: resolvedAddress.districts,
    });
    expect(logDiagnostics).toHaveBeenCalledOnce();
  });

  it("handles allowed CORS preflight before rate limiting", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, allowedOrigins: ["http://localhost:3000"], rateLimit }),
      {
        method: "OPTIONS",
        path: "/api/address/resolve",
        headers: { origin: "http://localhost:3000" },
      }
    );

    expect(response.statusCode).toBe(204);
    expect(response.headers).toMatchObject({
      "access-control-allow-origin": "http://localhost:3000",
      "access-control-allow-credentials": "true",
      "access-control-allow-methods": "GET, POST, PUT, OPTIONS",
      "access-control-allow-headers": "content-type",
      "access-control-max-age": "600",
      vary: "Origin",
    });
    expect(rateLimit).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects disallowed origins before rate limiting", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, allowedOrigins: ["http://localhost:3000"], rateLimit }),
      {
        method: "POST",
        path: "/api/address/resolve",
        body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
        headers: { origin: "https://evil.example", "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(403);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Origin is not allowed",
      },
    });
    expect(response.headers).toMatchObject({ vary: "Origin" });
    expect(rateLimit).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves same-origin or server-side requests without CORS headers", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);

    const response = await invokeExpressApp(createApiApp({ resolveAddress, allowedOrigins: ["http://localhost:3000"] }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers).not.toHaveProperty("access-control-allow-origin");
    expect(resolveAddress).toHaveBeenCalledWith("3921 Harlan Ave Baldwin Park CA 91706");
  });

  it("supports wildcard CORS origins", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);

    const response = await invokeExpressApp(createApiApp({ resolveAddress, allowedOrigins: ["*"] }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { origin: "https://frontend.example", "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers).toMatchObject({
      "access-control-allow-origin": "*",
      vary: "Origin",
    });
    expect(response.headers).not.toHaveProperty("access-control-allow-credentials");
  });

  it("preserves CORS headers on route validation errors", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({
        resolveAddress,
        lookupBallotSummaries,
        allowedOrigins: ["https://frontend.example"],
      }),
      {
        method: "GET",
        path: "/api/ballot?district_ids=not-a-uuid",
        headers: { origin: "https://frontend.example" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.headers).toMatchObject({
      "access-control-allow-origin": "https://frontend.example",
      vary: "Origin",
    });
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Query parameter district_ids contains invalid UUID: not-a-uuid",
      },
    });
  });

  it("returns unknown-path 404 before origin rejection and rate limiting", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, allowedOrigins: ["http://localhost:3000"], rateLimit }),
      {
        method: "POST",
        path: "/api/other",
        body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
        headers: { origin: "https://evil.example", "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(404);
    expect(response.body).toEqual({
      error: {
        code: "not_found",
        message: "Not found",
      },
    });
    expect(response.headers).toMatchObject({ vary: "Origin" });
    expect(rateLimit).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rate limits address POST requests before parsing oversized bodies", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn().mockReturnValue({ allowed: false, retryAfterSeconds: 7 });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, rateLimit }), {
      method: "POST",
      path: "/api/address/resolve",
      body: "x".repeat(16 * 1024 + 1),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(429);
    expect(response.body).toEqual({
      error: {
        code: "rate_limited",
        message: "Too many requests. Try again later.",
      },
    });
    expect(response.headers).toMatchObject({ "retry-after": "7" });
    expect(rateLimit).toHaveBeenCalledWith({
      clientIp: expect.any(String),
      method: "POST",
      pathname: "/api/address/resolve",
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps oversized address bodies to 413 after rate limiting allows the request", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn().mockReturnValue({ allowed: true });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, rateLimit }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "x".repeat(16 * 1024) }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(413);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "request body exceeds 16384 bytes",
      },
    });
    expect(rateLimit).toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps invalid JSON to invalid_json", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      body: "{not-json",
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_json",
        message: "Request body must be valid JSON",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects non-JSON content types before parsing address resolve bodies", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "text/plain" },
    });

    expect(response.statusCode).toBe(415);
    expect(response.body).toEqual({
      error: {
        code: "unsupported_media_type",
        message: "Content-Type must be application/json",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects non-application +json content types before parsing bodies", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "text/plain+json" },
    });

    expect(response.statusCode).toBe(415);
    expect(response.body).toEqual({
      error: {
        code: "unsupported_media_type",
        message: "Content-Type must be application/json",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps empty JSON bodies to invalid_request", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Request body must be a JSON object",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps strict JSON primitive bodies to invalid_json", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify("not an object"),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_json",
        message: "Request body must be valid JSON",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps validation errors to invalid_request", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Request body must include non-empty string field: address",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it.each([
    {
      upstreamError: new CensusAddressGeocoderError("invalid_address", "Address is invalid"),
      statusCode: 400,
      code: "invalid_request",
    },
    {
      upstreamError: new CensusAddressGeocoderError("not_found", "Census geocoder could not locate the address"),
      statusCode: 422,
      code: "address_not_found",
    },
    {
      upstreamError: new CensusAddressGeocoderError("bad_response", "Census geocoder returned malformed JSON"),
      statusCode: 502,
      code: "bad_upstream_response",
    },
    {
      upstreamError: new CensusAddressGeocoderError("timeout", "Census geocoder timed out"),
      statusCode: 503,
      code: "upstream_unavailable",
    },
    {
      upstreamError: new CensusAddressGeocoderError("network_error", "Census geocoder request failed: fetch failed"),
      statusCode: 503,
      code: "upstream_unavailable",
    },
  ])("maps Census geocoder $code errors", async ({ upstreamError, statusCode, code }) => {
    const resolveAddress = vi.fn().mockRejectedValue(upstreamError);

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "missing address" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(statusCode);
    expect(response.body).toEqual({
      error: {
        code,
        message: upstreamError.message,
      },
    });
  });

  it("maps unexpected route errors to internal_error", async () => {
    const resolveAddress = vi.fn().mockRejectedValue(new Error("database went sideways"));

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Internal error",
      },
    });
  });

  it("keeps known-path wrong methods as 405 responses", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();
    const lookupAuthenticatedBallotSummaries = vi.fn();
    const lookupElectionDetail = vi.fn();
    const updateAuthenticatedAddressDistricts = vi.fn();
    const initializeUserDistricts = vi.fn();
    const listResearchAreas = vi.fn();
    const listAuthenticatedResearchAreaPreferences = vi.fn();
    const replaceAuthenticatedResearchAreaPreferences = vi.fn();
    const app = createApiApp({
      resolveAddress,
      lookupBallotSummaries,
      lookupAuthenticatedBallotSummaries,
      lookupElectionDetail,
      updateAuthenticatedAddressDistricts,
      initializeUserDistricts,
      listResearchAreas,
      listAuthenticatedResearchAreaPreferences,
      replaceAuthenticatedResearchAreaPreferences,
    });

    const ballotResponse = await invokeExpressApp(app, {
      method: "POST",
      path: `/api/ballot?district_ids=${districtId}`,
    });
    expect(ballotResponse.statusCode).toBe(405);
    expect(ballotResponse.headers).toMatchObject({ allow: "GET" });
    expect(ballotResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use GET /api/ballot?district_ids=...",
      },
    });

    const electionResponse = await invokeExpressApp(app, {
      method: "POST",
      path: `/api/elections/${electionId}`,
    });
    expect(electionResponse.statusCode).toBe(405);
    expect(electionResponse.headers).toMatchObject({ allow: "GET" });
    expect(electionResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use GET /api/elections/:election_id",
      },
    });

    const authenticatedBallotResponse = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/ballot",
    });
    expect(authenticatedBallotResponse.statusCode).toBe(405);
    expect(authenticatedBallotResponse.headers).toMatchObject({ allow: "GET" });
    expect(authenticatedBallotResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use GET /api/me/ballot",
      },
    });

    const authenticatedAddressResponse = await invokeExpressApp(app, {
      method: "GET",
      path: "/api/me/address",
    });
    expect(authenticatedAddressResponse.statusCode).toBe(405);
    expect(authenticatedAddressResponse.headers).toMatchObject({ allow: "PUT" });
    expect(authenticatedAddressResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use PUT /api/me/address",
      },
    });

    const researchAreasResponse = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/research-areas",
    });
    expect(researchAreasResponse.statusCode).toBe(405);
    expect(researchAreasResponse.headers).toMatchObject({ allow: "GET" });
    expect(researchAreasResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use GET /api/research-areas",
      },
    });

    const researchAreaPreferencesResponse = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/research-area-preferences",
    });
    expect(researchAreaPreferencesResponse.statusCode).toBe(405);
    expect(researchAreaPreferencesResponse.headers).toMatchObject({ allow: "GET, PUT" });
    expect(researchAreaPreferencesResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use GET or PUT /api/me/research-area-preferences",
      },
    });

    const addressResponse = await invokeExpressApp(app, {
      method: "GET",
      path: "/api/address/resolve",
    });
    expect(addressResponse.statusCode).toBe(405);
    expect(addressResponse.headers).toMatchObject({ allow: "POST" });
    expect(addressResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use POST /api/address/resolve",
      },
    });

    const initializeResponse = await invokeExpressApp(app, {
      method: "GET",
      path: "/api/me/districts/initialize",
    });
    expect(initializeResponse.statusCode).toBe(405);
    expect(initializeResponse.headers).toMatchObject({ allow: "POST" });
    expect(initializeResponse.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use POST /api/me/districts/initialize",
      },
    });

    expect(resolveAddress).not.toHaveBeenCalled();
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
    expect(lookupAuthenticatedBallotSummaries).not.toHaveBeenCalled();
    expect(lookupElectionDetail).not.toHaveBeenCalled();
    expect(updateAuthenticatedAddressDistricts).not.toHaveBeenCalled();
    expect(initializeUserDistricts).not.toHaveBeenCalled();
    expect(listResearchAreas).not.toHaveBeenCalled();
    expect(listAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
    expect(replaceAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
  });

  it("rate limits known-path wrong methods before returning 405", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn().mockReturnValue({ allowed: false, retryAfterSeconds: 11 });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, rateLimit }), {
      method: "GET",
      path: "/api/address/resolve",
    });

    expect(response.statusCode).toBe(429);
    expect(response.headers).toMatchObject({ "retry-after": "11" });
    expect(response.body).toEqual({
      error: {
        code: "rate_limited",
        message: "Too many requests. Try again later.",
      },
    });
    expect(rateLimit).toHaveBeenCalledWith({
      clientIp: expect.any(String),
      method: "GET",
      pathname: "/api/address/resolve",
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("uses configured client IP resolution for rate limiting", async () => {
    const resolveAddress = vi.fn();
    const resolveClientIp = vi.fn().mockReturnValue("198.51.100.44");
    const rateLimit = vi.fn().mockReturnValue({ allowed: false, retryAfterSeconds: 3 });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveClientIp, rateLimit }), {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "application/json", "x-forwarded-for": "198.51.100.44" },
      remoteAddress: "10.0.0.5",
    });

    expect(response.statusCode).toBe(429);
    expect(resolveClientIp).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-forwarded-for": "198.51.100.44" }),
      remoteAddress: "10.0.0.5",
    });
    expect(rateLimit).toHaveBeenCalledWith({
      clientIp: "198.51.100.44",
      method: "POST",
      pathname: "/api/address/resolve",
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves ballot summaries and election detail routes", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const listAuthenticatedCandidateFollows = vi.fn();
    const lookupBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });
    const lookupElectionDetail = vi.fn().mockResolvedValue({
      id: electionId,
      district_id: districtId,
      district: {
        id: districtId,
        district_type: "county",
        geoid_compact: "06037",
        name: "Los Angeles County",
        state: "CA",
        state_fips: "06",
      },
      race_type: "office",
      official_ballot_title: "Sheriff",
      election_date: "2026-06-02",
      election_stage: "primary",
      is_partisan: false,
      discovery_contest_family: "non_judicial_office",
      sources: [],
      candidates: [],
      ballot_measure: null,
      results: [],
    });
    const app = createApiApp({
      resolveAddress,
      resolveAuthenticatedUserId,
      listAuthenticatedCandidateFollows,
      lookupBallotSummaries,
      lookupElectionDetail,
    });

    const ballotResponse = await invokeExpressApp(app, {
      method: "GET",
      path: `/api/ballot?district_ids=${districtId}`,
    });
    expect(ballotResponse.statusCode).toBe(200);
    expect(ballotResponse.body).toEqual({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });

    const electionResponse = await invokeExpressApp(app, {
      method: "GET",
      path: `/api/elections/${electionId}`,
      headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
    });
    expect(electionResponse.statusCode).toBe(200);
    expect(electionResponse.body).toMatchObject({
      id: electionId,
      race_type: "office",
      official_ballot_title: "Sheriff",
    });

    expect(lookupBallotSummaries).toHaveBeenCalledWith([districtId], {});
    expect(lookupElectionDetail).toHaveBeenCalledWith(electionId);
    expect(resolveAuthenticatedUserId).not.toHaveBeenCalled();
    expect(listAuthenticatedCandidateFollows).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves public candidate detail anonymously", async () => {
    const resolveAddress = vi.fn();
    const lookupCandidateDetail = vi.fn().mockResolvedValue({
      candidate: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        display_name: "Jane Smith",
        first_name: "Jane",
        last_name: "Smith",
        party: "Democratic",
        state: "CA",
        current_office: "Mayor",
        summary: "Incumbent mayor.",
        fec_ids: [],
        state_filing_ids: [],
        records: [],
        elections: [],
        is_following: false,
        follow: null,
      },
    });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupCandidateDetail }), {
      method: "GET",
      path: "/api/candidates/22222222-2222-4222-8222-222222222222",
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toMatchObject({
      candidate: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        display_name: "Jane Smith",
        is_following: false,
      },
    });
    expect(lookupCandidateDetail).toHaveBeenCalledWith("22222222-2222-4222-8222-222222222222", null);
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("passes the trusted user ID to candidate detail when authentication is present", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupCandidateDetail = vi.fn().mockResolvedValue({
      candidate: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        display_name: "Jane Smith",
        first_name: "Jane",
        last_name: "Smith",
        party: "Democratic",
        state: "CA",
        current_office: "Mayor",
        summary: null,
        fec_ids: [],
        state_filing_ids: [],
        records: [],
        elections: [],
        is_following: true,
        follow: {
          notify_elections: true,
          notify_updates: false,
          created_at: "2026-01-02T03:04:05.000Z",
        },
      },
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, lookupCandidateDetail }),
      {
        method: "GET",
        path: "/api/candidates/22222222-2222-4222-8222-222222222222",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toMatchObject({
      candidate: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        is_following: true,
      },
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(lookupCandidateDetail).toHaveBeenCalledWith(
      "22222222-2222-4222-8222-222222222222",
      "99999999-9999-4999-8999-999999999999"
    );
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 404 when candidate detail is missing", async () => {
    const resolveAddress = vi.fn();
    const lookupCandidateDetail = vi.fn().mockResolvedValue(null);

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupCandidateDetail }), {
      method: "GET",
      path: "/api/candidates/22222222-2222-4222-8222-222222222222",
    });

    expect(response.statusCode).toBe(404);
    expect(response.body).toEqual({
      error: {
        code: "not_found",
        message: "Candidate not found",
      },
    });
    expect(lookupCandidateDetail).toHaveBeenCalledWith("22222222-2222-4222-8222-222222222222", null);
  });

  it("rejects invalid candidate detail IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupCandidateDetail = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupCandidateDetail }), {
      method: "GET",
      path: "/api/candidates/not-a-uuid",
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Candidate detail path contains invalid UUID: not-a-uuid",
      },
    });
    expect(lookupCandidateDetail).not.toHaveBeenCalled();
  });

  it("rejects unsupported candidate detail methods", async () => {
    const resolveAddress = vi.fn();
    const lookupCandidateDetail = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupCandidateDetail }), {
      method: "POST",
      path: "/api/candidates/22222222-2222-4222-8222-222222222222",
      body: JSON.stringify({}),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(405);
    expect(response.headers.allow).toBe("GET");
    expect(response.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use GET /api/candidates/:candidate_id",
      },
    });
    expect(lookupCandidateDetail).not.toHaveBeenCalled();
  });

  it("returns 500 when candidate detail lookup is not configured", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "GET",
      path: "/api/candidates/22222222-2222-4222-8222-222222222222",
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Candidate detail lookup is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves the selectable research area catalog", async () => {
    const resolveAddress = vi.fn();
    const listResearchAreas = vi.fn().mockResolvedValue({
      research_areas: [
        {
          id: "22222222-2222-4222-8222-222222222222",
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: "Housing policy",
        },
      ],
    });

    const response = await invokeExpressApp(createApiApp({ resolveAddress, listResearchAreas }), {
      method: "GET",
      path: "/api/research-areas",
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      research_areas: [
        {
          id: "22222222-2222-4222-8222-222222222222",
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: "Housing policy",
        },
      ],
    });
    expect(listResearchAreas).toHaveBeenCalledOnce();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when the research area catalog lookup is not configured", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "GET",
      path: "/api/research-areas",
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Research area catalog lookup is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves authenticated ballot summaries for the current user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupAuthenticatedBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, lookupAuthenticatedBallotSummaries }),
      {
        method: "GET",
        path: "/api/me/ballot",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(lookupAuthenticatedBallotSummaries).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999", {});
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("passes sort and followed_first query params through to authenticated ballot summaries", async () => {
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupAuthenticatedBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [],
      districts: [],
      elections: [],
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAuthenticatedUserId, lookupAuthenticatedBallotSummaries }),
      {
        method: "GET",
        path: "/api/me/ballot?sort=soonest&followed_first=true",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(lookupAuthenticatedBallotSummaries).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999", {
      sort: "soonest",
      followedFirst: true,
    });
  });

  it("rejects an invalid sort query param with 400", async () => {
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupAuthenticatedBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [],
      districts: [],
      elections: [],
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAuthenticatedUserId, lookupAuthenticatedBallotSummaries }),
      {
        method: "GET",
        path: "/api/me/ballot?sort=alphabetical",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toMatchObject({ error: { code: "invalid_request" } });
    expect(lookupAuthenticatedBallotSummaries).not.toHaveBeenCalled();
  });

  it("passes the sort query param through to anonymous ballot summaries", async () => {
    const lookupBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });

    const response = await invokeExpressApp(createApiApp({ lookupBallotSummaries }), {
      method: "GET",
      path: `/api/ballot?district_ids=${districtId}&sort=vote_power`,
    });

    expect(response.statusCode).toBe(200);
    expect(lookupBallotSummaries).toHaveBeenCalledWith([districtId], { sort: "vote_power" });
  });

  it("accepts sort=district_size as a valid ballot sort", async () => {
    const lookupBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });

    const response = await invokeExpressApp(createApiApp({ lookupBallotSummaries }), {
      method: "GET",
      path: `/api/ballot?district_ids=${districtId}&sort=district_size`,
    });

    expect(response.statusCode).toBe(200);
    expect(lookupBallotSummaries).toHaveBeenCalledWith([districtId], { sort: "district_size" });
  });

  it("serves empty authenticated ballot summaries when the user has no saved districts", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupAuthenticatedBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [],
      districts: [],
      elections: [],
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, lookupAuthenticatedBallotSummaries }),
      {
        method: "GET",
        path: "/api/me/ballot",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      district_ids: [],
      districts: [],
      elections: [],
    });
    expect(lookupAuthenticatedBallotSummaries).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999", {});
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated ballot lookup when authentication is not configured", async () => {
    const resolveAddress = vi.fn();
    const lookupAuthenticatedBallotSummaries = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupAuthenticatedBallotSummaries }), {
      method: "GET",
      path: "/api/me/ballot",
    });

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(lookupAuthenticatedBallotSummaries).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when authenticated ballot lookup is not configured", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveAuthenticatedUserId }), {
      method: "GET",
      path: "/api/me/ballot",
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Authenticated ballot lookup is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated ballot lookup without an authenticated user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue(null);
    const lookupAuthenticatedBallotSummaries = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, lookupAuthenticatedBallotSummaries }),
      {
        method: "GET",
        path: "/api/me/ballot",
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(lookupAuthenticatedBallotSummaries).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps authenticated ballot user-district reader errors to unauthorized", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupAuthenticatedBallotSummaries = vi
      .fn()
      .mockRejectedValue(new UserDistrictReaderError("user_not_found", "User not found"));

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, lookupAuthenticatedBallotSummaries }),
      {
        method: "GET",
        path: "/api/me/ballot",
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(lookupAuthenticatedBallotSummaries).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999", {});
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("updates authenticated address districts for the current user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupAuthenticatedUserEmailVerified = vi.fn().mockResolvedValue(true);
    const updateAuthenticatedAddressDistricts = vi.fn().mockResolvedValue({
      matched_address: "123 MAIN ST, DENVER, CO, 80203",
      district_ids: [districtId],
      districts: resolvedAddress.districts,
      elections: [],
    });

    const response = await invokeExpressApp(
      createApiApp({
        resolveAddress,
        resolveAuthenticatedUserId,
        lookupAuthenticatedUserEmailVerified,
        updateAuthenticatedAddressDistricts,
      }),
      {
        method: "PUT",
        path: "/api/me/address",
        body: JSON.stringify({ address: "  123 Main St Denver CO 80203  " }),
        headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      matched_address: "123 MAIN ST, DENVER, CO, 80203",
      district_ids: [districtId],
      districts: resolvedAddress.districts,
      elections: [],
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(lookupAuthenticatedUserEmailVerified).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999");
    expect(updateAuthenticatedAddressDistricts).toHaveBeenCalledWith(
      "99999999-9999-4999-8999-999999999999",
      "123 Main St Denver CO 80203"
    );
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated address updates when authentication is not configured", async () => {
    const resolveAddress = vi.fn();
    const updateAuthenticatedAddressDistricts = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, updateAuthenticatedAddressDistricts }), {
      method: "PUT",
      path: "/api/me/address",
      body: JSON.stringify({ address: "123 Main St Denver CO 80203" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(updateAuthenticatedAddressDistricts).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it.each([
    {
      name: "authenticated address updates",
      method: "PUT",
      path: "/api/me/address",
      body: JSON.stringify({ address: "123 Main St Denver CO 80203" }),
      handlerKey: "updateAuthenticatedAddressDistricts" as const,
    },
    {
      name: "authenticated candidate follow updates",
      method: "PUT",
      path: "/api/me/candidate-follows",
      body: JSON.stringify({
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
      }),
      handlerKey: "setAuthenticatedCandidateFollow" as const,
    },
    {
      name: "authenticated research area preference updates",
      method: "PUT",
      path: "/api/me/research-area-preferences",
      body: JSON.stringify({ preferences: [] }),
      handlerKey: "replaceAuthenticatedResearchAreaPreferences" as const,
    },
    {
      name: "authenticated district initialization",
      method: "POST",
      path: "/api/me/districts/initialize",
      body: JSON.stringify({ district_ids: [districtId] }),
      handlerKey: "initializeUserDistricts" as const,
    },
  ])("rejects unverified users from $name", async ({ method, path, body, handlerKey }) => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const lookupAuthenticatedUserEmailVerified = vi.fn().mockResolvedValue(false);
    const handler = vi.fn();
    const app = createApiApp({
      resolveAddress,
      resolveAuthenticatedUserId,
      lookupAuthenticatedUserEmailVerified,
      [handlerKey]: handler,
    });

    const response = await invokeExpressApp(app, {
      method,
      path,
      body,
      headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
    });

    expect(response.statusCode).toBe(403);
    expect(response.body).toEqual({
      error: {
        code: "forbidden",
        message: "Email verification is required",
      },
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(lookupAuthenticatedUserEmailVerified).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999");
    expect(handler).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when authenticated address update storage is not configured", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveAuthenticatedUserId }), {
      method: "PUT",
      path: "/api/me/address",
      body: JSON.stringify({ address: "123 Main St Denver CO 80203" }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Authenticated address update is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated address updates without an authenticated user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue(null);
    const updateAuthenticatedAddressDistricts = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, updateAuthenticatedAddressDistricts }),
      {
        method: "PUT",
        path: "/api/me/address",
        body: JSON.stringify({ address: "123 Main St Denver CO 80203" }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(updateAuthenticatedAddressDistricts).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects invalid authenticated address update payloads before calling the handler", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const updateAuthenticatedAddressDistricts = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, updateAuthenticatedAddressDistricts }),
      {
        method: "PUT",
        path: "/api/me/address",
        body: JSON.stringify({ address: "   " }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Request body must include non-empty string field: address",
      },
    });
    expect(updateAuthenticatedAddressDistricts).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects non-JSON content types before parsing authenticated address update bodies", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const updateAuthenticatedAddressDistricts = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, updateAuthenticatedAddressDistricts }),
      {
        method: "PUT",
        path: "/api/me/address",
        body: JSON.stringify({ address: "123 Main St Denver CO 80203" }),
        headers: { "content-type": "text/plain" },
      }
    );

    expect(response.statusCode).toBe(415);
    expect(response.body).toEqual({
      error: {
        code: "unsupported_media_type",
        message: "Content-Type must be application/json",
      },
    });
    expect(resolveAuthenticatedUserId).not.toHaveBeenCalled();
    expect(updateAuthenticatedAddressDistricts).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps authenticated address replacement errors to API errors", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const updateAuthenticatedAddressDistricts = vi
      .fn()
      .mockRejectedValue(new ReplaceUserDistrictsError("unknown_district_ids", `Unknown district IDs: ${districtId}`));

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, updateAuthenticatedAddressDistricts }),
      {
        method: "PUT",
        path: "/api/me/address",
        body: JSON.stringify({ address: "123 Main St Denver CO 80203" }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Address could not be matched to saved districts",
      },
    });
    expect(updateAuthenticatedAddressDistricts).toHaveBeenCalledWith(
      "99999999-9999-4999-8999-999999999999",
      "123 Main St Denver CO 80203"
    );
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves authenticated candidate follows for the current user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const listAuthenticatedCandidateFollows = vi.fn().mockResolvedValue({
      follows: [
        {
          candidate_id: "22222222-2222-4222-8222-222222222222",
          display_name: "Jane Smith",
          party: "Democratic",
          state: "CA",
          current_office: null,
          latest_record: {
            description: "Sponsored a housing affordability bill.",
            event_date: "2026-01-15",
          },
          active_election: {
            election_id: "33333333-3333-4333-8333-333333333333",
            official_ballot_title: "Mayor",
            election_date: "2026-11-03",
          },
          notify_elections: true,
          notify_updates: false,
          created_at: "2026-01-02T03:04:05.000Z",
        },
      ],
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, listAuthenticatedCandidateFollows }),
      {
        method: "GET",
        path: "/api/me/candidate-follows",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      follows: [
        {
          candidate_id: "22222222-2222-4222-8222-222222222222",
          display_name: "Jane Smith",
          party: "Democratic",
          state: "CA",
          current_office: null,
          latest_record: {
            description: "Sponsored a housing affordability bill.",
            event_date: "2026-01-15",
          },
          active_election: {
            election_id: "33333333-3333-4333-8333-333333333333",
            official_ballot_title: "Mayor",
            election_date: "2026-11-03",
          },
          notify_elections: true,
          notify_updates: false,
          created_at: "2026-01-02T03:04:05.000Z",
        },
      ],
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(response.body.follows[0]).not.toHaveProperty("records");
    expect(listAuthenticatedCandidateFollows).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999");
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("updates authenticated candidate follows for the current user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const setAuthenticatedCandidateFollow = vi.fn().mockResolvedValue({
      follow: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
        notify_elections: false,
        notify_updates: true,
        created_at: "2026-01-02T03:04:05.000Z",
      },
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, setAuthenticatedCandidateFollow }),
      {
        method: "PUT",
        path: "/api/me/candidate-follows",
        body: JSON.stringify({
          candidate_id: "22222222-2222-4222-8222-222222222222",
          following: true,
          notify_elections: false,
          notify_updates: true,
        }),
        headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      follow: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
        notify_elections: false,
        notify_updates: true,
        created_at: "2026-01-02T03:04:05.000Z",
      },
    });
    expect(setAuthenticatedCandidateFollow).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999", {
      candidateId: "22222222-2222-4222-8222-222222222222",
      following: true,
      notifyElections: false,
      notifyUpdates: true,
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("unfollows authenticated candidates for the current user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const setAuthenticatedCandidateFollow = vi.fn().mockResolvedValue({
      follow: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: false,
        notify_elections: false,
        notify_updates: false,
        created_at: null,
      },
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, setAuthenticatedCandidateFollow }),
      {
        method: "PUT",
        path: "/api/me/candidate-follows",
        body: JSON.stringify({
          candidate_id: "22222222-2222-4222-8222-222222222222",
          following: false,
          notify_elections: false,
          notify_updates: false,
        }),
        headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      follow: {
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: false,
        notify_elections: false,
        notify_updates: false,
        created_at: null,
      },
    });
    expect(setAuthenticatedCandidateFollow).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999", {
      candidateId: "22222222-2222-4222-8222-222222222222",
      following: false,
      notifyElections: false,
      notifyUpdates: false,
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated candidate follows when authentication is not configured", async () => {
    const resolveAddress = vi.fn();
    const listAuthenticatedCandidateFollows = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, listAuthenticatedCandidateFollows }), {
      method: "GET",
      path: "/api/me/candidate-follows",
    });

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(listAuthenticatedCandidateFollows).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated candidate follow updates when authentication is not configured", async () => {
    const resolveAddress = vi.fn();
    const setAuthenticatedCandidateFollow = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, setAuthenticatedCandidateFollow }), {
      method: "PUT",
      path: "/api/me/candidate-follows",
      body: JSON.stringify({
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
      }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(setAuthenticatedCandidateFollow).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when authenticated candidate follow lookup is not configured", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveAuthenticatedUserId }), {
      method: "GET",
      path: "/api/me/candidate-follows",
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Authenticated candidate follow lookup is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when authenticated candidate follow storage is not configured", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveAuthenticatedUserId }), {
      method: "PUT",
      path: "/api/me/candidate-follows",
      body: JSON.stringify({
        candidate_id: "22222222-2222-4222-8222-222222222222",
        following: true,
      }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Authenticated candidate follow storage is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated candidate follows without an authenticated user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue(null);
    const listAuthenticatedCandidateFollows = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, listAuthenticatedCandidateFollows }),
      {
        method: "GET",
        path: "/api/me/candidate-follows",
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(listAuthenticatedCandidateFollows).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated candidate follow updates without an authenticated user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue(null);
    const setAuthenticatedCandidateFollow = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, setAuthenticatedCandidateFollow }),
      {
        method: "PUT",
        path: "/api/me/candidate-follows",
        body: JSON.stringify({
          candidate_id: "22222222-2222-4222-8222-222222222222",
          following: true,
        }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(setAuthenticatedCandidateFollow).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects invalid authenticated candidate follow payloads before calling the handler", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const setAuthenticatedCandidateFollow = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, setAuthenticatedCandidateFollow }),
      {
        method: "PUT",
        path: "/api/me/candidate-follows",
        body: JSON.stringify({ candidate_id: "not-a-uuid", following: true }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "candidate_id must be a valid UUID: not-a-uuid",
      },
    });
    expect(setAuthenticatedCandidateFollow).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects non-JSON content types before parsing authenticated candidate follow bodies", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const setAuthenticatedCandidateFollow = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, setAuthenticatedCandidateFollow }),
      {
        method: "PUT",
        path: "/api/me/candidate-follows",
        body: JSON.stringify({
          candidate_id: "22222222-2222-4222-8222-222222222222",
          following: true,
        }),
        headers: { "content-type": "text/plain" },
      }
    );

    expect(response.statusCode).toBe(415);
    expect(response.body).toEqual({
      error: {
        code: "unsupported_media_type",
        message: "Content-Type must be application/json",
      },
    });
    expect(resolveAuthenticatedUserId).not.toHaveBeenCalled();
    expect(setAuthenticatedCandidateFollow).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects unsupported authenticated candidate follow methods", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const listAuthenticatedCandidateFollows = vi.fn();
    const setAuthenticatedCandidateFollow = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({
        resolveAddress,
        resolveAuthenticatedUserId,
        listAuthenticatedCandidateFollows,
        setAuthenticatedCandidateFollow,
      }),
      {
        method: "POST",
        path: "/api/me/candidate-follows",
        body: JSON.stringify({}),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(405);
    expect(response.headers.allow).toBe("GET, PUT");
    expect(response.body).toEqual({
      error: {
        code: "method_not_allowed",
        message: "Use GET or PUT /api/me/candidate-follows",
      },
    });
    expect(resolveAuthenticatedUserId).not.toHaveBeenCalled();
    expect(listAuthenticatedCandidateFollows).not.toHaveBeenCalled();
    expect(setAuthenticatedCandidateFollow).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps authenticated candidate follow user errors to unauthorized", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const listAuthenticatedCandidateFollows = vi
      .fn()
      .mockRejectedValue(new UserCandidateFollowsError("user_not_found", "User not found"));

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, listAuthenticatedCandidateFollows }),
      {
        method: "GET",
        path: "/api/me/candidate-follows",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(listAuthenticatedCandidateFollows).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999");
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps missing/deleted/merged candidate follow targets to not_found", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const setAuthenticatedCandidateFollow = vi
      .fn()
      .mockRejectedValue(new UserCandidateFollowsError("candidate_not_found", "Candidate not found"));

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, setAuthenticatedCandidateFollow }),
      {
        method: "PUT",
        path: "/api/me/candidate-follows",
        body: JSON.stringify({
          candidate_id: "22222222-2222-4222-8222-222222222222",
          following: true,
        }),
        headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(404);
    expect(response.body).toEqual({
      error: {
        code: "not_found",
        message: "Candidate not found",
      },
    });
    expect(setAuthenticatedCandidateFollow).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999", {
      candidateId: "22222222-2222-4222-8222-222222222222",
      following: true,
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves authenticated research area preferences for the current user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const listAuthenticatedResearchAreaPreferences = vi.fn().mockResolvedValue({
      preferences: [
        {
          research_area_id: "22222222-2222-4222-8222-222222222222",
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: null,
          rank: 1,
        },
      ],
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, listAuthenticatedResearchAreaPreferences }),
      {
        method: "GET",
        path: "/api/me/research-area-preferences",
        headers: { "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      preferences: [
        {
          research_area_id: "22222222-2222-4222-8222-222222222222",
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: null,
          rank: 1,
        },
      ],
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(listAuthenticatedResearchAreaPreferences).toHaveBeenCalledWith("99999999-9999-4999-8999-999999999999");
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated research area preferences lookup when authentication is not configured", async () => {
    const resolveAddress = vi.fn();
    const listAuthenticatedResearchAreaPreferences = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, listAuthenticatedResearchAreaPreferences }),
      {
        method: "GET",
        path: "/api/me/research-area-preferences",
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(listAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when authenticated research area preferences lookup is not configured", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveAuthenticatedUserId }), {
      method: "GET",
      path: "/api/me/research-area-preferences",
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Authenticated research area preferences lookup is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated research area preferences lookup without an authenticated user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue(null);
    const listAuthenticatedResearchAreaPreferences = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, listAuthenticatedResearchAreaPreferences }),
      {
        method: "GET",
        path: "/api/me/research-area-preferences",
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(listAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("maps authenticated research area preference reader errors to unauthorized", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const listAuthenticatedResearchAreaPreferences = vi
      .fn()
      .mockRejectedValue(new UserResearchAreaPreferencesError("user_not_found", "User not found"));

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, listAuthenticatedResearchAreaPreferences }),
      {
        method: "GET",
        path: "/api/me/research-area-preferences",
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(listAuthenticatedResearchAreaPreferences).toHaveBeenCalledWith(
      "99999999-9999-4999-8999-999999999999"
    );
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("replaces authenticated research area preferences for the current user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const replaceAuthenticatedResearchAreaPreferences = vi.fn().mockResolvedValue({
      preferences: [
        {
          research_area_id: "22222222-2222-4222-8222-222222222222",
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: null,
          rank: 1,
        },
        {
          research_area_id: "33333333-3333-4333-8333-333333333333",
          slug: "healthcare_affordability",
          name: "Healthcare Affordability",
          description: null,
          rank: null,
        },
      ],
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, replaceAuthenticatedResearchAreaPreferences }),
      {
        method: "PUT",
        path: "/api/me/research-area-preferences",
        body: JSON.stringify({
          preferences: [
            { research_area_id: "22222222-2222-4222-8222-222222222222", rank: 1 },
            { research_area_id: "33333333-3333-4333-8333-333333333333", rank: null },
          ],
        }),
        headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      preferences: [
        {
          research_area_id: "22222222-2222-4222-8222-222222222222",
          slug: "housing_affordability",
          name: "Housing Affordability",
          description: null,
          rank: 1,
        },
        {
          research_area_id: "33333333-3333-4333-8333-333333333333",
          slug: "healthcare_affordability",
          name: "Healthcare Affordability",
          description: null,
          rank: null,
        },
      ],
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(replaceAuthenticatedResearchAreaPreferences).toHaveBeenCalledWith(
      "99999999-9999-4999-8999-999999999999",
      [
        { researchAreaId: "22222222-2222-4222-8222-222222222222", rank: 1 },
        { researchAreaId: "33333333-3333-4333-8333-333333333333", rank: null },
      ]
    );
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("clears authenticated research area preferences", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const replaceAuthenticatedResearchAreaPreferences = vi.fn().mockResolvedValue({ preferences: [] });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, replaceAuthenticatedResearchAreaPreferences }),
      {
        method: "PUT",
        path: "/api/me/research-area-preferences",
        body: JSON.stringify({ preferences: [] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({ preferences: [] });
    expect(replaceAuthenticatedResearchAreaPreferences).toHaveBeenCalledWith(
      "99999999-9999-4999-8999-999999999999",
      []
    );
  });

  it("rejects authenticated research area preference replacement when authentication is not configured", async () => {
    const resolveAddress = vi.fn();
    const replaceAuthenticatedResearchAreaPreferences = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, replaceAuthenticatedResearchAreaPreferences }),
      {
        method: "PUT",
        path: "/api/me/research-area-preferences",
        body: JSON.stringify({ preferences: [] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(replaceAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when authenticated research area preference storage is not configured", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveAuthenticatedUserId }), {
      method: "PUT",
      path: "/api/me/research-area-preferences",
      body: JSON.stringify({ preferences: [] }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Authenticated research area preference storage is not configured",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects authenticated research area preference replacement without an authenticated user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue(null);
    const replaceAuthenticatedResearchAreaPreferences = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, replaceAuthenticatedResearchAreaPreferences }),
      {
        method: "PUT",
        path: "/api/me/research-area-preferences",
        body: JSON.stringify({ preferences: [] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(replaceAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
  });

  it("rejects invalid research area preference payloads before calling storage", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const replaceAuthenticatedResearchAreaPreferences = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, replaceAuthenticatedResearchAreaPreferences }),
      {
        method: "PUT",
        path: "/api/me/research-area-preferences",
        body: JSON.stringify({ preferences: [{ research_area_id: "not-a-uuid" }] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "preferences contains invalid research_area_id: not-a-uuid",
      },
    });
    expect(replaceAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
  });

  it("rejects non-JSON content types before parsing research area preference replacement bodies", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const replaceAuthenticatedResearchAreaPreferences = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, replaceAuthenticatedResearchAreaPreferences }),
      {
        method: "PUT",
        path: "/api/me/research-area-preferences",
        body: JSON.stringify({ preferences: [] }),
        headers: { "content-type": "text/plain" },
      }
    );

    expect(response.statusCode).toBe(415);
    expect(response.body).toEqual({
      error: {
        code: "unsupported_media_type",
        message: "Content-Type must be application/json",
      },
    });
    expect(resolveAuthenticatedUserId).not.toHaveBeenCalled();
    expect(replaceAuthenticatedResearchAreaPreferences).not.toHaveBeenCalled();
  });

  it("maps authenticated research area preference replacement errors to API errors", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const replaceAuthenticatedResearchAreaPreferences = vi
      .fn()
      .mockRejectedValue(
        new UserResearchAreaPreferencesError("unselectable_research_area_ids", "Research area cannot be selected")
      );

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, replaceAuthenticatedResearchAreaPreferences }),
      {
        method: "PUT",
        path: "/api/me/research-area-preferences",
        body: JSON.stringify({ preferences: [{ research_area_id: "22222222-2222-4222-8222-222222222222" }] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Research area cannot be selected",
      },
    });
    expect(replaceAuthenticatedResearchAreaPreferences).toHaveBeenCalledOnce();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("initializes authenticated user districts", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi.fn().mockResolvedValue({
      status: "initialized",
      districtCount: 1,
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/me/districts/initialize",
        body: JSON.stringify({ district_ids: [districtId, districtId.toUpperCase()] }),
        headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      status: "initialized",
      district_count: 1,
    });
    expect(resolveAuthenticatedUserId).toHaveBeenCalledWith({
      headers: expect.objectContaining({ "x-user-id": "99999999-9999-4999-8999-999999999999" }),
    });
    expect(initializeUserDistricts).toHaveBeenCalledWith({
      userId: "99999999-9999-4999-8999-999999999999",
      districtIds: [districtId],
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("supports anonymous resolve followed by post-signup district initialization", async () => {
    const resolvedAddressWithUuidDistrict = {
      ...resolvedAddress,
      districts: resolvedAddress.districts.map((district) => ({ ...district, id: districtId })),
    };
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddressWithUuidDistrict);
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi.fn().mockResolvedValue({
      status: "initialized",
      districtCount: 1,
    });
    const app = createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts });

    const resolveResponse = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/address/resolve",
      body: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
      headers: { "content-type": "application/json" },
    });
    expect(resolveResponse.statusCode).toBe(200);
    expect(resolveResponse.body).toEqual({
      matched_address: resolvedAddressWithUuidDistrict.matched_address,
      districts: resolvedAddressWithUuidDistrict.districts,
    });
    expect(initializeUserDistricts).not.toHaveBeenCalled();

    const returnedDistrictIds = (resolveResponse.body as { districts: Array<{ id: string }> }).districts.map(
      (district) => district.id
    );
    const initializeResponse = await invokeExpressApp(app, {
      method: "POST",
      path: "/api/me/districts/initialize",
      body: JSON.stringify({ district_ids: returnedDistrictIds }),
      headers: { "content-type": "application/json", "x-user-id": "99999999-9999-4999-8999-999999999999" },
    });

    expect(initializeResponse.statusCode).toBe(200);
    expect(initializeResponse.body).toEqual({
      status: "initialized",
      district_count: 1,
    });
    expect(initializeUserDistricts).toHaveBeenCalledWith({
      userId: "99999999-9999-4999-8999-999999999999",
      districtIds: [districtId],
    });
  });

  it("returns already_initialized for users with existing saved districts", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi.fn().mockResolvedValue({
      status: "already_initialized",
      districtCount: 7,
    });

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/me/districts/initialize",
        body: JSON.stringify({ district_ids: [districtId] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      status: "already_initialized",
      district_count: 7,
    });
    expect(initializeUserDistricts).toHaveBeenCalledOnce();
  });

  it("rejects district initialization when authentication is not configured", async () => {
    const resolveAddress = vi.fn();
    const initializeUserDistricts = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, initializeUserDistricts }), {
      method: "POST",
      path: "/api/me/districts/initialize",
      body: JSON.stringify({ district_ids: [districtId] }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(initializeUserDistricts).not.toHaveBeenCalled();
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 500 when district initialization storage is not configured", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, resolveAuthenticatedUserId }), {
      method: "POST",
      path: "/api/me/districts/initialize",
      body: JSON.stringify({ district_ids: [districtId] }),
      headers: { "content-type": "application/json" },
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "User district initialization is not configured",
      },
    });
  });

  it("rejects district initialization without an authenticated user", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue(null);
    const initializeUserDistricts = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/me/districts/initialize",
        body: JSON.stringify({ district_ids: [districtId] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(401);
    expect(response.body).toEqual({
      error: {
        code: "unauthorized",
        message: "Authentication is required",
      },
    });
    expect(initializeUserDistricts).not.toHaveBeenCalled();
  });

  it("rejects invalid district initialization payloads before calling the store", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/me/districts/initialize",
        body: JSON.stringify({ district_ids: ["not-a-uuid"] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "district_ids contains invalid UUID: not-a-uuid",
      },
    });
    expect(initializeUserDistricts).not.toHaveBeenCalled();
  });

  it("rejects non-JSON content types before parsing district initialization bodies", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi.fn();

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/me/districts/initialize",
        body: JSON.stringify({ district_ids: [districtId] }),
        headers: { "content-type": "text/plain" },
      }
    );

    expect(response.statusCode).toBe(415);
    expect(response.body).toEqual({
      error: {
        code: "unsupported_media_type",
        message: "Content-Type must be application/json",
      },
    });
    expect(resolveAuthenticatedUserId).not.toHaveBeenCalled();
    expect(initializeUserDistricts).not.toHaveBeenCalled();
  });

  it("rejects too many district IDs before calling the initialize store", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi.fn();
    const districtIds = Array.from({ length: MAX_INITIALIZE_DISTRICT_IDS + 1 }, (_value, index) =>
      makeDistrictId(index + 1)
    );

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/me/districts/initialize",
        body: JSON.stringify({ district_ids: districtIds }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: `district_ids supports at most ${MAX_INITIALIZE_DISTRICT_IDS} UUIDs`,
      },
    });
    expect(initializeUserDistricts).not.toHaveBeenCalled();
  });

  it("maps initialize store errors to API errors", async () => {
    const resolveAddress = vi.fn();
    const resolveAuthenticatedUserId = vi.fn().mockReturnValue("99999999-9999-4999-8999-999999999999");
    const initializeUserDistricts = vi
      .fn()
      .mockRejectedValue(new InitializeUserDistrictsError("unknown_district_ids", `Unknown district IDs: ${districtId}`));

    const response = await invokeExpressApp(
      createApiApp({ resolveAddress, resolveAuthenticatedUserId, initializeUserDistricts }),
      {
        method: "POST",
        path: "/api/me/districts/initialize",
        body: JSON.stringify({ district_ids: [districtId] }),
        headers: { "content-type": "application/json" },
      }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: `Unknown district IDs: ${districtId}`,
      },
    });
  });

  it("dispatches routes correctly when mounted under a path prefix", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });
    const mountedApp = express();
    mountedApp.use("/v1", createApiApp({ resolveAddress, lookupBallotSummaries }));

    const response = await invokeExpressApp(mountedApp, {
      method: "GET",
      path: `/v1/api/ballot?district_ids=${districtId}`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toEqual({
      district_ids: [districtId],
      districts: [],
      elections: [],
    });
    expect(lookupBallotSummaries).toHaveBeenCalledWith([districtId], {});
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects invalid ballot district IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupBallotSummaries }), {
      method: "GET",
      path: "/api/ballot?district_ids=not-a-uuid",
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Query parameter district_ids contains invalid UUID: not-a-uuid",
      },
    });
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
  });

  it("rejects missing ballot district IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupBallotSummaries }), {
      method: "GET",
      path: "/api/ballot",
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Query parameter district_ids must include at least one district UUID",
      },
    });
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
  });

  it("rejects too many ballot district IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();
    const tooManyDistrictIds = Array.from(
      { length: 51 },
      (_, index) => `11111111-1111-4111-8111-${String(index + 1).padStart(12, "0")}`
    ).join(",");

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupBallotSummaries }), {
      method: "GET",
      path: `/api/ballot?district_ids=${tooManyDistrictIds}`,
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Query parameter district_ids supports at most 50 UUIDs",
      },
    });
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
  });

  it("returns 500 when ballot summary lookup is not configured", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "GET",
      path: `/api/ballot?district_ids=${districtId}`,
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Ballot lookup is not configured",
      },
    });
  });

  it("returns 404 when election detail is missing", async () => {
    const resolveAddress = vi.fn();
    const lookupElectionDetail = vi.fn().mockResolvedValue(null);

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupElectionDetail }), {
      method: "GET",
      path: `/api/elections/${electionId}`,
    });

    expect(response.statusCode).toBe(404);
    expect(response.body).toEqual({
      error: {
        code: "not_found",
        message: "Election not found",
      },
    });
    expect(lookupElectionDetail).toHaveBeenCalledWith(electionId);
  });

  it("rejects invalid election detail IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupElectionDetail = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupElectionDetail }), {
      method: "GET",
      path: "/api/elections/not-a-uuid",
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Election detail path contains invalid UUID: not-a-uuid",
      },
    });
    expect(lookupElectionDetail).not.toHaveBeenCalled();
  });

  it("rejects malformed election detail paths before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupElectionDetail = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress, lookupElectionDetail }), {
      method: "GET",
      path: `/api/elections/${electionId}/extra`,
    });

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Election detail path must be /api/elections/:election_id",
      },
    });
    expect(lookupElectionDetail).not.toHaveBeenCalled();
  });

  it("returns 500 when election detail lookup is not configured", async () => {
    const resolveAddress = vi.fn();

    const response = await invokeExpressApp(createApiApp({ resolveAddress }), {
      method: "GET",
      path: `/api/elections/${electionId}`,
    });

    expect(response.statusCode).toBe(500);
    expect(response.body).toEqual({
      error: {
        code: "internal_error",
        message: "Election detail lookup is not configured",
      },
    });
  });
});
