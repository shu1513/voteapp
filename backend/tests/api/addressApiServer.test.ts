import { describe, expect, it, vi } from "vitest";
import { Readable } from "node:stream";
import type { IncomingMessage, RequestListener, ServerResponse } from "node:http";

import { createAddressApiRequestHandler, handleAddressApiRequest } from "../../src/api/addressApiServer.js";
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
const secondDistrictId = "22222222-2222-4222-8222-222222222222";
const electionId = "33333333-3333-4333-8333-333333333333";

async function invokeRequestHandler(
  handler: RequestListener,
  input: {
    method: string;
    path: string;
    body: string;
    remoteAddress?: string;
    headers?: Record<string, string>;
  }
): Promise<{ statusCode: number; headers: Record<string, string>; body: string }> {
  const request = Readable.from([input.body]) as IncomingMessage;
  Object.assign(request, {
    method: input.method,
    url: input.path,
    headers: input.headers ?? {},
    socket: {
      remoteAddress: input.remoteAddress ?? "127.0.0.1",
    },
  });

  return await new Promise((resolve) => {
    const response = {
      writeHead: vi.fn((statusCode: number, headers: Record<string, string>) => {
        response.statusCode = statusCode;
        response.headers = headers;
        return response;
      }),
      end: vi.fn((body?: string) => {
        resolve({
          statusCode: response.statusCode,
          headers: response.headers,
          body: body ?? "",
        });
        return response;
      }),
      statusCode: 200,
      headers: {} as Record<string, string>,
    } as unknown as ServerResponse & {
      statusCode: number;
      headers: Record<string, string>;
    };

    handler(request, response);
  });
}

describe("handleAddressApiRequest", () => {
  it("serves POST /api/address/resolve", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const logDiagnostics = vi.fn();

    await expect(
      handleAddressApiRequest(
        {
          method: "POST",
          path: "/api/address/resolve",
          rawBody: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
        },
        { resolveAddress, logDiagnostics }
      )
    ).resolves.toEqual({
      statusCode: 200,
      headers: { "content-type": "application/json; charset=utf-8" },
      body: {
        matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
        districts: resolvedAddress.districts,
      },
    });
    expect(resolveAddress).toHaveBeenCalledWith("3921 Harlan Ave Baldwin Park CA 91706");
    expect(logDiagnostics).toHaveBeenCalledWith({
      address_match_count: 1,
      district_keys: resolvedAddress.district_keys,
      missing_district_keys: [],
      warnings: [],
    });
  });

  it("does not include ballot data in an address response", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const lookupBallotSummaries = vi.fn();
    const saveUserDistricts = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({
          address: "3921 Harlan Ave Baldwin Park CA 91706",
        }),
      },
      { resolveAddress, lookupBallotSummaries, saveUserDistricts }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toMatchObject({
      matched_address: resolvedAddress.matched_address,
      districts: resolvedAddress.districts,
    });
    expect(response.body).not.toHaveProperty("ballot");
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
    expect(saveUserDistricts).not.toHaveBeenCalled();
  });

  it("saves resolved districts only when requested by an authenticated user", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const saveUserDistricts = vi.fn().mockResolvedValue({ user_id: districtId, district_count: 1 });

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({
          address: "3921 Harlan Ave Baldwin Park CA 91706",
          save_districts: true,
        }),
        headers: { "x-user-id": districtId },
      },
      {
        resolveAddress,
        saveUserDistricts,
        resolveUserId: (headers) => String(headers?.["x-user-id"] ?? ""),
      }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toMatchObject({
      matched_address: resolvedAddress.matched_address,
      saved_user_districts: { district_count: 1 },
    });
    expect(saveUserDistricts).toHaveBeenCalledWith(districtId, resolvedAddress.districts);
  });

  it("rejects save_districts for anonymous users", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const saveUserDistricts = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({
          address: "3921 Harlan Ave Baldwin Park CA 91706",
          save_districts: true,
        }),
      },
      { resolveAddress, saveUserDistricts }
    );

    expect(response).toEqual({
      statusCode: 401,
      headers: { "content-type": "application/json; charset=utf-8" },
      body: {
        error: {
          code: "auth_required",
          message: "Login is required to save user districts",
        },
      },
    });
    expect(resolveAddress).toHaveBeenCalledOnce();
    expect(saveUserDistricts).not.toHaveBeenCalled();
  });

  it("rejects non-boolean save_districts field", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({ address: "3921 Harlan Ave", save_districts: "yes" }),
      },
      { resolveAddress }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Request field save_districts must be boolean when provided",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("adds CORS headers for allowed origins", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
        headers: { origin: "http://localhost:3000" },
      },
      { resolveAddress, allowedOrigins: ["http://localhost:3000"] }
    );

    expect(response.statusCode).toBe(200);
    expect(response.headers).toMatchObject({
      "access-control-allow-origin": "http://localhost:3000",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "content-type",
      "access-control-max-age": "600",
      vary: "Origin",
    });
  });

  it("handles CORS preflight for allowed origins", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "OPTIONS",
        path: "/api/address/resolve",
        rawBody: "",
        headers: { origin: "http://localhost:3000" },
      },
      { resolveAddress, allowedOrigins: ["http://localhost:3000"], rateLimit }
    );

    expect(response).toEqual({
      statusCode: 204,
      headers: {
        "access-control-allow-origin": "http://localhost:3000",
        "access-control-allow-methods": "GET, POST, OPTIONS",
        "access-control-allow-headers": "content-type",
        "access-control-max-age": "600",
        vary: "Origin",
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
    expect(rateLimit).not.toHaveBeenCalled();
  });

  it("rate limits non-preflight address API requests", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn().mockReturnValue({ allowed: false, retryAfterSeconds: 42 });

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
        clientIp: "203.0.113.10",
      },
      { resolveAddress, rateLimit }
    );

    expect(response).toEqual({
      statusCode: 429,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "retry-after": "42",
      },
      body: {
        error: {
          code: "rate_limited",
          message: "Too many requests. Try again later.",
        },
      },
    });
    expect(rateLimit).toHaveBeenCalledWith({
      clientIp: "203.0.113.10",
      method: "POST",
      pathname: "/api/address/resolve",
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rate limits POST address requests before reading oversized bodies", async () => {
    const resolveAddress = vi.fn();
    const rateLimit = vi.fn().mockReturnValue({ allowed: false, retryAfterSeconds: 7 });
    const handler = createAddressApiRequestHandler({ resolveAddress, rateLimit });

    const response = await invokeRequestHandler(handler, {
      method: "POST",
      path: "/api/address/resolve",
      body: "x".repeat(16 * 1024 + 1),
      remoteAddress: "203.0.113.10",
    });

    expect(response.statusCode).toBe(429);
    expect(JSON.parse(response.body)).toEqual({
      error: {
        code: "rate_limited",
        message: "Too many requests. Try again later.",
      },
    });
    expect(response.headers).toMatchObject({ "retry-after": "7" });
    expect(rateLimit).toHaveBeenCalledWith({
      clientIp: "203.0.113.10",
      method: "POST",
      pathname: "/api/address/resolve",
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects disallowed cross-origin requests before resolving address", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({ address: "3921 Harlan Ave Baldwin Park CA 91706" }),
        headers: { origin: "https://evil.example" },
      },
      { resolveAddress, allowedOrigins: ["http://localhost:3000"] }
    );

    expect(response).toEqual({
      statusCode: 403,
      headers: {
        "content-type": "application/json; charset=utf-8",
        vary: "Origin",
      },
      body: {
        error: {
          code: "invalid_request",
          message: "Origin is not allowed",
        },
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("serves GET /api/ballot with comma-separated district IDs", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn().mockResolvedValue({
      district_ids: [districtId, secondDistrictId],
      districts: [],
      elections: [],
    });

    await expect(
      handleAddressApiRequest(
        {
          method: "GET",
          path: `/api/ballot?district_ids=${districtId},${secondDistrictId},${districtId}`,
          rawBody: "",
        },
        { resolveAddress, lookupBallotSummaries }
      )
    ).resolves.toEqual({
      statusCode: 200,
      headers: { "content-type": "application/json; charset=utf-8" },
      body: {
        district_ids: [districtId, secondDistrictId],
        districts: [],
        elections: [],
      },
    });
    expect(lookupBallotSummaries).toHaveBeenCalledWith([districtId, secondDistrictId]);
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects invalid ballot district IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "GET", path: "/api/ballot?district_ids=not-a-uuid", rawBody: "" },
      { resolveAddress, lookupBallotSummaries }
    );

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

    const response = await handleAddressApiRequest(
      { method: "GET", path: "/api/ballot", rawBody: "" },
      { resolveAddress, lookupBallotSummaries }
    );

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

    const response = await handleAddressApiRequest(
      { method: "GET", path: `/api/ballot?district_ids=${tooManyDistrictIds}`, rawBody: "" },
      { resolveAddress, lookupBallotSummaries }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Query parameter district_ids supports at most 50 UUIDs",
      },
    });
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
  });

  it("rejects non-GET ballot requests", async () => {
    const resolveAddress = vi.fn();
    const lookupBallotSummaries = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "POST", path: `/api/ballot?district_ids=${districtId}`, rawBody: "" },
      { resolveAddress, lookupBallotSummaries }
    );

    expect(response).toEqual({
      statusCode: 405,
      headers: {
        "content-type": "application/json; charset=utf-8",
        allow: "GET",
      },
      body: {
        error: {
          code: "method_not_allowed",
          message: "Use GET /api/ballot?district_ids=...",
        },
      },
    });
    expect(lookupBallotSummaries).not.toHaveBeenCalled();
  });

  it("serves GET /api/elections/:election_id", async () => {
    const resolveAddress = vi.fn();
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

    await expect(
      handleAddressApiRequest(
        {
          method: "GET",
          path: `/api/elections/${electionId}`,
          rawBody: "",
        },
        { resolveAddress, lookupElectionDetail }
      )
    ).resolves.toMatchObject({
      statusCode: 200,
      headers: { "content-type": "application/json; charset=utf-8" },
      body: {
        id: electionId,
        race_type: "office",
        official_ballot_title: "Sheriff",
      },
    });
    expect(lookupElectionDetail).toHaveBeenCalledWith(electionId);
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("returns 404 when election detail is missing", async () => {
    const resolveAddress = vi.fn();
    const lookupElectionDetail = vi.fn().mockResolvedValue(null);

    const response = await handleAddressApiRequest(
      { method: "GET", path: `/api/elections/${electionId}`, rawBody: "" },
      { resolveAddress, lookupElectionDetail }
    );

    expect(response).toEqual({
      statusCode: 404,
      headers: { "content-type": "application/json; charset=utf-8" },
      body: {
        error: {
          code: "not_found",
          message: "Election not found",
        },
      },
    });
    expect(lookupElectionDetail).toHaveBeenCalledWith(electionId);
  });

  it("rejects invalid election detail IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupElectionDetail = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "GET", path: "/api/elections/not-a-uuid", rawBody: "" },
      { resolveAddress, lookupElectionDetail }
    );

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

    const response = await handleAddressApiRequest(
      { method: "GET", path: `/api/elections/${electionId}/extra`, rawBody: "" },
      { resolveAddress, lookupElectionDetail }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Election detail path must be /api/elections/:election_id",
      },
    });
    expect(lookupElectionDetail).not.toHaveBeenCalled();
  });

  it("rejects non-GET election detail requests", async () => {
    const resolveAddress = vi.fn();
    const lookupElectionDetail = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "POST", path: `/api/elections/${electionId}`, rawBody: "" },
      { resolveAddress, lookupElectionDetail }
    );

    expect(response).toEqual({
      statusCode: 405,
      headers: {
        "content-type": "application/json; charset=utf-8",
        allow: "GET",
      },
      body: {
        error: {
          code: "method_not_allowed",
          message: "Use GET /api/elections/:election_id",
        },
      },
    });
    expect(lookupElectionDetail).not.toHaveBeenCalled();
  });

  it("rejects non-POST methods", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "GET", path: "/api/address/resolve", rawBody: "" },
      { resolveAddress }
    );

    expect(response).toEqual({
      statusCode: 405,
      headers: {
        "content-type": "application/json; charset=utf-8",
        allow: "POST",
      },
      body: {
        error: {
          code: "method_not_allowed",
          message: "Use POST /api/address/resolve",
        },
      },
    });
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects invalid JSON body", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "POST", path: "/api/address/resolve", rawBody: "{not-json" },
      { resolveAddress }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_json",
        message: "Request body must be valid JSON",
      },
    });
  });

  it("rejects missing address field", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "POST", path: "/api/address/resolve", rawBody: JSON.stringify({ address: "" }) },
      { resolveAddress }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Request body must include non-empty string field: address",
      },
    });
  });

  it("maps Census no-match errors to 422", async () => {
    const resolveAddress = vi
      .fn()
      .mockRejectedValue(new CensusAddressGeocoderError("not_found", "Census geocoder could not locate the address"));

    const response = await handleAddressApiRequest(
      { method: "POST", path: "/api/address/resolve", rawBody: JSON.stringify({ address: "missing address" }) },
      { resolveAddress }
    );

    expect(response.statusCode).toBe(422);
    expect(response.body).toEqual({
      error: {
        code: "address_not_found",
        message: "Census geocoder could not locate the address",
      },
    });
  });

  it("returns 404 for other paths", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "POST", path: "/api/other", rawBody: "" },
      { resolveAddress }
    );

    expect(response).toEqual({
      statusCode: 404,
      headers: { "content-type": "application/json; charset=utf-8" },
      body: {
        error: {
          code: "not_found",
          message: "Not found",
        },
      },
    });
  });
});
