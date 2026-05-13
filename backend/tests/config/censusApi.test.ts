import { afterEach, describe, expect, it, vi } from "vitest";

import { fetchCensusJsonWithKeyRotation } from "../../src/config/censusApi.ts";

describe("fetchCensusJsonWithKeyRotation", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("rethrows plain Error AbortError without rotating keys", async () => {
    const abortError = new Error("aborted");
    abortError.name = "AbortError";
    const fetchMock = vi.spyOn(globalThis, "fetch").mockRejectedValue(abortError);

    await expect(
      fetchCensusJsonWithKeyRotation(
        "https://api.census.gov/data/2024/acs/acs5?get=NAME&for=state:*",
        ["k1", "k2"]
      )
    ).rejects.toBe(abortError);

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("rethrows DOMException AbortError without rotating keys", async () => {
    const abortError = new DOMException("aborted", "AbortError");
    const fetchMock = vi.spyOn(globalThis, "fetch").mockRejectedValue(abortError);

    await expect(
      fetchCensusJsonWithKeyRotation(
        "https://api.census.gov/data/2024/acs/acs5?get=NAME&for=state:*",
        ["k1", "k2"]
      )
    ).rejects.toBe(abortError);

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
