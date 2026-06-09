import { describe, expect, it, vi } from "vitest";

import { handleAddressApiRequest } from "../../src/api/addressApiServer.js";
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
        coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
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

  it("can include ballot data in an address response without saving user districts", async () => {
    const resolveAddress = vi.fn().mockResolvedValue(resolvedAddress);
    const lookupBallot = vi.fn().mockResolvedValue({
      district_ids: ["district-la"],
      districts: resolvedAddress.districts,
      elections: [],
    });
    const saveUserDistricts = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({
          address: "3921 Harlan Ave Baldwin Park CA 91706",
          include_ballot: true,
        }),
      },
      { resolveAddress, lookupBallot, saveUserDistricts }
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toMatchObject({
      matched_address: resolvedAddress.matched_address,
      districts: resolvedAddress.districts,
      ballot: {
        district_ids: ["district-la"],
        elections: [],
      },
    });
    expect(lookupBallot).toHaveBeenCalledWith(["district-la"]);
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

  it("rejects non-boolean address option fields", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "POST",
        path: "/api/address/resolve",
        rawBody: JSON.stringify({ address: "3921 Harlan Ave", include_ballot: "yes" }),
      },
      { resolveAddress }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Request field include_ballot must be boolean when provided",
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
    const lookupBallot = vi.fn().mockResolvedValue({
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
        { resolveAddress, lookupBallot }
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
    expect(lookupBallot).toHaveBeenCalledWith([districtId, secondDistrictId]);
    expect(resolveAddress).not.toHaveBeenCalled();
  });

  it("rejects invalid ballot district IDs before lookup", async () => {
    const resolveAddress = vi.fn();
    const lookupBallot = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "GET", path: "/api/ballot?district_ids=not-a-uuid", rawBody: "" },
      { resolveAddress, lookupBallot }
    );

    expect(response.statusCode).toBe(400);
    expect(response.body).toEqual({
      error: {
        code: "invalid_request",
        message: "Query parameter district_ids contains invalid UUID: not-a-uuid",
      },
    });
    expect(lookupBallot).not.toHaveBeenCalled();
  });

  it("rejects non-GET ballot requests", async () => {
    const resolveAddress = vi.fn();
    const lookupBallot = vi.fn();

    const response = await handleAddressApiRequest(
      { method: "POST", path: `/api/ballot?district_ids=${districtId}`, rawBody: "" },
      { resolveAddress, lookupBallot }
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
    expect(lookupBallot).not.toHaveBeenCalled();
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
