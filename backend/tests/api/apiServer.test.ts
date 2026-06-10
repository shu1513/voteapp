import { type IncomingMessage, ServerResponse } from "node:http";
import { Readable, Writable } from "node:stream";
import express, { type Express } from "express";
import { describe, expect, it, vi } from "vitest";

import { createApiApp } from "../../src/api/apiServer.js";
import { CensusAddressGeocoderError } from "../../src/pipeline/address/censusAddressGeocoder.js";
import type { AddressResolutionResult } from "../../src/pipeline/address/addressResolverService.js";

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
      vote_power_score: 12.3,
    },
  ],
  missing_district_keys: [],
  warnings: [],
};

const districtId = "11111111-1111-4111-8111-111111111111";
const electionId = "33333333-3333-4333-8333-333333333333";

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
      "access-control-allow-methods": "GET, POST, OPTIONS",
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
        message: "database went sideways",
      },
    });
  });

  it("keeps known-path wrong methods as 405 responses", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();
    const lookupElectionDetail = vi.fn();
    const app = createApiApp({ resolveAddress, lookupBallotSummaries, lookupElectionDetail });

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

    expect(resolveAddress).not.toHaveBeenCalled();
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
    expect(lookupElectionDetail).not.toHaveBeenCalled();
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
    const app = createApiApp({ resolveAddress, lookupBallotSummaries, lookupElectionDetail });

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
    });
    expect(electionResponse.statusCode).toBe(200);
    expect(electionResponse.body).toMatchObject({
      id: electionId,
      race_type: "office",
      official_ballot_title: "Sheriff",
    });

    expect(lookupBallotSummaries).toHaveBeenCalledWith([districtId]);
    expect(lookupElectionDetail).toHaveBeenCalledWith(electionId);
    expect(resolveAddress).not.toHaveBeenCalled();
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
    expect(lookupBallotSummaries).toHaveBeenCalledWith([districtId]);
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
