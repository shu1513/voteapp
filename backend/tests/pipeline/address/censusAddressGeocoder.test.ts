import { describe, expect, it, vi } from "vitest";

import {
  CensusAddressGeocoderError,
  buildCensusAddressCandidates,
  buildCensusAddressGeocoderUrl,
  buildCensusCoordinatesGeocoderUrl,
  geocodeAddressWithCensus,
  geocodeAddressWithCensusFallbacks,
  geocodeCoordinatesWithCensus,
  parseCensusAddressGeocoderPayload,
  parseCensusCoordinatesGeocoderPayload,
} from "../../../src/pipeline/address/censusAddressGeocoder.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

function fetchMockReturning(response: Response): typeof fetch {
  return vi.fn().mockResolvedValue(response) as unknown as typeof fetch;
}

describe("censusAddressGeocoder", () => {
  it("builds a Census geocoder URL with defaults and encoded address", () => {
    const url = new URL(buildCensusAddressGeocoderUrl("3921 Harlan Ave Baldwin Park CA 91706"));

    expect(url.origin + url.pathname).toBe("https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress");
    expect(url.searchParams.get("address")).toBe("3921 Harlan Ave Baldwin Park CA 91706");
    expect(url.searchParams.get("benchmark")).toBe("Public_AR_Current");
    expect(url.searchParams.get("vintage")).toBe("ACS2024_Current");
    expect(url.searchParams.get("layers")).toBe("all");
    expect(url.searchParams.get("format")).toBe("json");
  });

  it("allows benchmark, vintage, and layers to be pinned by caller", () => {
    const url = new URL(
      buildCensusAddressGeocoderUrl("3921 Harlan Ave Baldwin Park CA 91706", {
        benchmark: "Public_AR_Census2020",
        vintage: "Census2020_Census2020",
        layers: "States,Counties",
      })
    );

    expect(url.searchParams.get("benchmark")).toBe("Public_AR_Census2020");
    expect(url.searchParams.get("vintage")).toBe("Census2020_Census2020");
    expect(url.searchParams.get("layers")).toBe("States,Counties");
  });

  it("rejects empty addresses before calling Census", async () => {
    const fetchImpl = vi.fn() as unknown as typeof fetch;

    await expect(geocodeAddressWithCensus("   ", { fetchImpl })).rejects.toMatchObject({
      code: "invalid_address",
    });
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("parses the first address match and keeps raw geographies", () => {
    const geographies = {
      Counties: [{ GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G4020" }],
    };

    expect(
      parseCensusAddressGeocoderPayload({
        result: {
          addressMatches: [
            {
              matchedAddress: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
              coordinates: { x: -117.981072355887, y: 34.082500135664 },
              geographies,
            },
            {
              matchedAddress: "OTHER MATCH",
              coordinates: { x: -1, y: 1 },
              geographies: {},
            },
          ],
        },
      })
    ).toEqual({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
      geographies,
      address_match_count: 2,
    });
  });

  it("throws a typed not_found error when Census returns zero matches", () => {
    expect(() =>
      parseCensusAddressGeocoderPayload({
        result: { addressMatches: [] },
      })
    ).toThrow(CensusAddressGeocoderError);

    try {
      parseCensusAddressGeocoderPayload({ result: { addressMatches: [] } });
      throw new Error("expected parse to throw");
    } catch (error) {
      expect(error).toMatchObject({ code: "not_found" });
    }
  });

  it("fetches and parses a successful Census response", async () => {
    const fetchImpl = fetchMockReturning(
      jsonResponse({
        result: {
          addressMatches: [
            {
              matchedAddress: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
              coordinates: { x: -117.981072355887, y: 34.082500135664 },
              geographies: { States: [{ GEOID: "06", MTFCC: "G4000" }] },
            },
          ],
        },
      })
    );

    const result = await geocodeAddressWithCensus("3921 Harlan Ave Baldwin Park CA 91706", { fetchImpl });

    expect(result).toMatchObject({
      matched_address: "3921 HARLAN AVE, BALDWIN PARK, CA, 91706",
      coordinates: { lat: 34.082500135664, lng: -117.981072355887 },
      address_match_count: 1,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [url, init] = vi.mocked(fetchImpl).mock.calls[0]!;
    expect(String(url)).toContain("geocoding.geo.census.gov/geocoder/geographies/onelineaddress");
    expect(init).toMatchObject({ method: "GET", redirect: "follow" });
    expect(init?.signal).toBeInstanceOf(AbortSignal);
  });

  it("throws typed http_error for non-2xx responses", async () => {
    const fetchImpl = fetchMockReturning(new Response("upstream unavailable", { status: 503, statusText: "Service Unavailable" }));

    await expect(geocodeAddressWithCensus("3921 Harlan Ave Baldwin Park CA 91706", { fetchImpl })).rejects.toMatchObject({
      code: "http_error",
      message: expect.stringContaining("status=503 Service Unavailable"),
    });
  });

  it("throws typed bad_response for non-JSON responses", async () => {
    const fetchImpl = fetchMockReturning(new Response("<html>nope</html>", { status: 200, statusText: "OK" }));

    await expect(geocodeAddressWithCensus("3921 Harlan Ave Baldwin Park CA 91706", { fetchImpl })).rejects.toMatchObject({
      code: "bad_response",
      message: expect.stringContaining("non-JSON"),
    });
  });

  it("maps AbortError to a timeout error", async () => {
    const abortError = new DOMException("aborted", "AbortError");
    const fetchImpl = vi.fn().mockRejectedValue(abortError) as unknown as typeof fetch;

    await expect(
      geocodeAddressWithCensus("3921 Harlan Ave Baldwin Park CA 91706", { fetchImpl, timeoutMs: 1234 })
    ).rejects.toMatchObject({
      code: "timeout",
      message: "Census geocoder request timed out after 1234ms",
    });
  });

  it("wraps non-abort fetch failures in a typed network_error", async () => {
    const fetchImpl = vi.fn().mockRejectedValue(new TypeError("fetch failed")) as unknown as typeof fetch;

    await expect(
      geocodeAddressWithCensus("3921 Harlan Ave Baldwin Park CA 91706", { fetchImpl })
    ).rejects.toMatchObject({
      name: "CensusAddressGeocoderError",
      code: "network_error",
      message: "Census geocoder request failed: fetch failed",
    });
  });
});

describe("buildCensusAddressCandidates", () => {
  it("derives country-stripped, street+zip, and no-zip variants from a Google formattedAddress", () => {
    expect(buildCensusAddressCandidates("1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA")).toEqual([
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA",
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073",
      "1 MetLife Stadium Dr, 07073",
      "1 MetLife Stadium Dr, East Rutherford, NJ",
    ]);
  });

  it("collapses ZIP+4 to ZIP5", () => {
    expect(buildCensusAddressCandidates("3921 Harlan Ave, Baldwin Park, CA 91706-1234")).toEqual([
      "3921 Harlan Ave, Baldwin Park, CA 91706-1234",
      "3921 Harlan Ave, Baldwin Park, CA 91706",
      "3921 Harlan Ave, 91706",
      "3921 Harlan Ave, Baldwin Park, CA",
    ]);
  });

  it("returns a single candidate when there is nothing to vary", () => {
    expect(buildCensusAddressCandidates("3921 Harlan Ave Baldwin Park CA 91706")).toEqual([
      "3921 Harlan Ave Baldwin Park CA 91706",
    ]);
  });

  it("only strips a country that follows a comma", () => {
    expect(buildCensusAddressCandidates("10 Route US")).toEqual(["10 Route US"]);
  });

  it("returns no candidates for blank input", () => {
    expect(buildCensusAddressCandidates("   ")).toEqual([]);
  });
});

describe("geocodeAddressWithCensusFallbacks", () => {
  const MATCH_PAYLOAD = {
    result: {
      addressMatches: [
        {
          matchedAddress: "1 METLIFE STADIUM DR, EAST RUTHERFORD, NJ, 07073",
          coordinates: { x: -74.0741, y: 40.8135 },
          geographies: { Counties: [] },
        },
      ],
    },
  };
  const NO_MATCH_PAYLOAD = { result: { addressMatches: [] } };

  it("returns the first candidate's match without trying later candidates", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse(MATCH_PAYLOAD)) as unknown as typeof fetch;

    const result = await geocodeAddressWithCensusFallbacks(
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA",
      { fetchImpl }
    );

    expect(result.matched_address).toBe("1 METLIFE STADIUM DR, EAST RUTHERFORD, NJ, 07073");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("falls through not_found to the next candidate", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(NO_MATCH_PAYLOAD))
      .mockResolvedValueOnce(jsonResponse(MATCH_PAYLOAD)) as unknown as typeof fetch;

    const result = await geocodeAddressWithCensusFallbacks(
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA",
      { fetchImpl }
    );

    expect(result.matched_address).toBe("1 METLIFE STADIUM DR, EAST RUTHERFORD, NJ, 07073");
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    const calls = (fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls as [string][];
    expect(new URL(calls[0][0]).searchParams.get("address")).toBe(
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA"
    );
    expect(new URL(calls[1][0]).searchParams.get("address")).toBe(
      "1 MetLife Stadium Dr, East Rutherford, NJ 07073"
    );
  });

  it("throws not_found when every candidate misses", async () => {
    // Fresh Response per call: a shared body errors on the second read.
    const fetchImpl = vi.fn().mockImplementation(() => Promise.resolve(jsonResponse(NO_MATCH_PAYLOAD))) as unknown as typeof fetch;

    await expect(
      geocodeAddressWithCensusFallbacks("1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA", { fetchImpl })
    ).rejects.toMatchObject({ code: "not_found" });
    expect(fetchImpl).toHaveBeenCalledTimes(4);
  });

  it("propagates upstream failures immediately instead of retrying", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValue(jsonResponse({ error: "boom" }, { status: 500, statusText: "Internal Server Error" })) as unknown as typeof fetch;

    await expect(
      geocodeAddressWithCensusFallbacks("1 MetLife Stadium Dr, East Rutherford, NJ 07073, USA", { fetchImpl })
    ).rejects.toMatchObject({ code: "http_error" });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("rejects blank addresses before calling Census", async () => {
    const fetchImpl = vi.fn() as unknown as typeof fetch;

    await expect(geocodeAddressWithCensusFallbacks("   ", { fetchImpl })).rejects.toMatchObject({
      code: "invalid_address",
    });
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});

describe("censusCoordinatesGeocoder", () => {
  it("builds a coordinates URL mapping lng to x and lat to y with defaults", () => {
    const url = new URL(buildCensusCoordinatesGeocoderUrl({ lat: 40.8135, lng: -74.0741 }));

    expect(url.origin + url.pathname).toBe("https://geocoding.geo.census.gov/geocoder/geographies/coordinates");
    expect(url.searchParams.get("x")).toBe("-74.0741");
    expect(url.searchParams.get("y")).toBe("40.8135");
    expect(url.searchParams.get("benchmark")).toBe("Public_AR_Current");
    expect(url.searchParams.get("vintage")).toBe("ACS2024_Current");
    expect(url.searchParams.get("layers")).toBe("all");
    expect(url.searchParams.get("format")).toBe("json");
  });

  it("rejects out-of-range coordinates", () => {
    expect(() => buildCensusCoordinatesGeocoderUrl({ lat: 91, lng: 0 })).toThrowError(CensusAddressGeocoderError);
    expect(() => buildCensusCoordinatesGeocoderUrl({ lat: 0, lng: 181 })).toThrowError(CensusAddressGeocoderError);
    expect(() => buildCensusCoordinatesGeocoderUrl({ lat: Number.NaN, lng: 0 })).toThrowError(
      CensusAddressGeocoderError
    );
  });

  it("parses geographies from a coordinates response", () => {
    const geographies = { Counties: [{ GEOID: "34003" }] };

    expect(parseCensusCoordinatesGeocoderPayload({ result: { geographies } })).toEqual({ geographies });
  });

  it("rejects a coordinates response without geographies as bad_response", () => {
    expect(() => parseCensusCoordinatesGeocoderPayload({ result: {} })).toThrowError(/geographies/);
    expect(() => parseCensusCoordinatesGeocoderPayload({})).toThrowError(CensusAddressGeocoderError);
  });

  it("geocodes coordinates end to end via fetch", async () => {
    const geographies = { Counties: [{ GEOID: "34003" }] };
    const fetchImpl = fetchMockReturning(jsonResponse({ result: { geographies } }));

    const result = await geocodeCoordinatesWithCensus({ lat: 40.8135, lng: -74.0741 }, { fetchImpl });

    expect(result).toEqual({ geographies });
  });
});
