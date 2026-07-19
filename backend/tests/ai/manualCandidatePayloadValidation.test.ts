import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../src/ai/urlReachability.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../src/ai/urlReachability.js")>()),
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

import { validateCandidateProfileAiPayload } from "../../src/ai/enrichCandidateProfile.js";
import { validateCandidateRecordDiscoveryPayload } from "../../src/ai/enrichCandidateRecords.js";

describe("manual candidate payload validation helpers", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => ({
      ok: true,
      finalUrl: url,
      status: 200,
    }));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("validates candidate profile payload shape and source reachability", async () => {
    const result = await validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        party: "Democratic",
        official_website_url: "https://jane.example",
        current_office: "Governor",
        summary: "Former city council member.",
        sources: ["https://jane.example/about"],
      },
      1000
    );

    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledWith(
      "https://jane.example/about",
      expect.objectContaining({ allowStatusCodes: [403] })
    );
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sourceCount).toBe(1);
      expect(result.profile).toMatchObject({
        display_name: "Jane Candidate",
        party: "Democratic",
        official_website_url: "https://jane.example",
        current_office: "Governor",
      });
    }
  });

  it("rejects candidate profile payloads with unreachable source URLs", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: false,
      reason: "citation fetch returned status 404",
    });

    const result = await validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        official_website_url: "https://jane.example",
        sources: ["https://jane.example/missing"],
      },
      1000
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.retryable).toBe(false);
      expect(result.reason).toContain("citation URL(s) could not be verified");
      expect(result.failedCitationUrls).toEqual(["https://jane.example/missing"]);
    }
  });

  it("passes candidate profile parse options through to the contract parser", async () => {
    const federalResult = await validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        sources: ["https://jane.example/about"],
      },
      1000,
      { requireFecIds: true }
    );

    expect(federalResult.ok).toBe(false);
    if (!federalResult.ok) {
      expect(federalResult.reason).toBe("payload.fec_ids must contain at least one FEC ID for federal contests");
    }

    const disallowedResult = await validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        fec_ids: ["H6VT00000"],
        sources: ["https://jane.example/about"],
      },
      1000,
      { allowFecIds: false }
    );

    expect(disallowedResult.ok).toBe(false);
    if (!disallowedResult.ok) {
      expect(disallowedResult.reason).toBe(
        "payload.fec_ids is not allowed for this contest mode; omit fec_ids from the profile payload — identity IDs are inherited from the staged roster row"
      );
    }
    expect(verifyHttpUrlReachabilityMock).not.toHaveBeenCalled();
  });

  it("validates candidate record discovery payloads and normalizes verified source URLs", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => ({
      ok: true,
      finalUrl: `${url}/final`,
      status: 200,
    }));

    const result = await validateCandidateRecordDiscoveryPayload(
      {
        records: [
          {
            description: "Voted for the city budget.",
            source_url: "https://city.example/minutes",
            event_date: "2026-01-05",
          },
        ],
      },
      1000
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.records).toEqual([
        {
          description: "Voted for the city budget.",
          source_url: "https://city.example/minutes/final",
          event_date: "2026-01-05",
        },
      ]);
      expect(result.droppedRecords).toEqual([]);
      expect(result.validationDebug).toMatchObject({
        dropped_records_count: 0,
        verified_records_count: 1,
      });
    }
  });

  it("reports candidate record rows that need source or schema repair", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) =>
      url.includes("missing")
        ? { ok: false, reason: "citation fetch returned status 404" }
        : { ok: true, finalUrl: url, status: 200 }
    );

    const result = await validateCandidateRecordDiscoveryPayload(
      {
        records: [
          {
            description: "Voted for the city budget.",
            source_url: "https://city.example/minutes",
            event_date: "2026-01-05",
          },
          {
            description: "Spoke at a hearing.",
            source_url: "https://city.example/missing",
            event_date: "2026-01-06",
          },
          {
            description: "Missing date.",
            source_url: "https://city.example/missing-date",
          },
        ],
      },
      1000
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.records).toHaveLength(1);
      expect(result.droppedRecords).toHaveLength(2);
      expect(result.droppedRecords[0]).toMatchObject({
        reason: "citation fetch returned status 404",
        failureKind: "source_url",
        failureType: "permanent",
      });
      expect(result.droppedRecords[1]).toMatchObject({
        reason: "schema invalid row index=2: event_date must be parseable date",
        failureKind: "schema",
        failureType: "permanent",
      });
    }
  });

  it("reports top-level candidate record discovery parser failures", async () => {
    const result = await validateCandidateRecordDiscoveryPayload({ records: "bad" }, 1000);

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe("payload.records must be array");
      expect(result.failureDebug).toEqual({
        parser_reason: "payload.records must be array",
      });
    }
    expect(verifyHttpUrlReachabilityMock).not.toHaveBeenCalled();
  });

  it("retries transient citation failures in-place and passes when the retry succeeds", async () => {
    vi.useFakeTimers();
    let attempts = 0;
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      attempts += 1;
      if (attempts === 1) {
        return { ok: false, reason: "citation URL fetch timed out" };
      }
      return { ok: true, finalUrl: url, status: 200 };
    });

    const resultPromise = validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        party: "Democratic",
        summary: "Former city council member.",
        sources: ["https://slow.example/about"],
      },
      1000
    );
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.ok).toBe(true);
    expect(attempts).toBe(2);
  });

  it("retries only transient URLs in a mixed citation batch", async () => {
    vi.useFakeTimers();
    const transientUrl = "https://slow.example/about";
    const permanentUrl = "https://dead.example/about";
    const successfulUrl = "https://ok.example/about";
    const attemptsByUrl = new Map<string, number>();
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      const attempts = (attemptsByUrl.get(url) ?? 0) + 1;
      attemptsByUrl.set(url, attempts);
      if (url === transientUrl && attempts === 1) {
        return { ok: false, reason: "citation URL fetch timed out" };
      }
      if (url === permanentUrl) {
        return { ok: false, reason: "citation fetch returned status 404" };
      }
      return { ok: true, finalUrl: url, status: 200 };
    });

    const resultPromise = validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        party: "Democratic",
        summary: "Former city council member.",
        sources: [transientUrl, permanentUrl, successfulUrl],
      },
      1000
    );
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.ok).toBe(false);
    expect(attemptsByUrl.get(transientUrl)).toBe(2);
    expect(attemptsByUrl.get(permanentUrl)).toBe(1);
    expect(attemptsByUrl.get(successfulUrl)).toBe(1);
  });

  it("does not retry permanent citation failures", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async () => ({
      ok: false,
      reason: "citation fetch returned status 404",
    }));

    const result = await validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        party: "Democratic",
        summary: "Former city council member.",
        sources: ["https://dead.example/about"],
      },
      1000
    );

    expect(result.ok).toBe(false);
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(1);
  });

  it("fails with the transient reason when retries keep timing out", async () => {
    vi.useFakeTimers();
    verifyHttpUrlReachabilityMock.mockImplementation(async () => ({
      ok: false,
      reason: "citation URL fetch timed out",
    }));

    const resultPromise = validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        has_held_public_office: true,
        party: "Democratic",
        summary: "Former city council member.",
        sources: ["https://down.example/about"],
      },
      1000
    );
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("transient");
    }
    // initial pass + CITATION_TRANSIENT_RETRY_ATTEMPTS retries
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(3);
  });
});
