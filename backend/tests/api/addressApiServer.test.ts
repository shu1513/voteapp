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
      "access-control-allow-methods": "POST, OPTIONS",
      "access-control-allow-headers": "content-type",
      "access-control-max-age": "600",
      vary: "Origin",
    });
  });

  it("handles CORS preflight for allowed origins", async () => {
    const resolveAddress = vi.fn();

    const response = await handleAddressApiRequest(
      {
        method: "OPTIONS",
        path: "/api/address/resolve",
        rawBody: "",
        headers: { origin: "http://localhost:3000" },
      },
      { resolveAddress, allowedOrigins: ["http://localhost:3000"] }
    );

    expect(response).toEqual({
      statusCode: 204,
      headers: {
        "access-control-allow-origin": "http://localhost:3000",
        "access-control-allow-methods": "POST, OPTIONS",
        "access-control-allow-headers": "content-type",
        "access-control-max-age": "600",
        vary: "Origin",
      },
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
