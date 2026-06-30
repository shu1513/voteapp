import { beforeEach, describe, expect, it, vi } from "vitest";

const { verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../src/ai/urlReachability.js", () => ({
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

  it("validates candidate profile payload shape and source reachability", async () => {
    const result = await validateCandidateProfileAiPayload(
      {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        party: "Democratic",
        official_website_url: "https://jane.example",
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
});
