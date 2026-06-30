import { beforeEach, describe, expect, it, vi } from "vitest";

const { callResearchProviderMock, verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  callResearchProviderMock: vi.fn(),
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../src/ai/researchProviderClient.js", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (text: string) => text,
}));

vi.mock("../../src/ai/urlReachability.js", () => ({
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

import {
  enrichCandidateRecords,
  validateCandidateRecordDiscoveryPayload,
} from "../../src/ai/enrichCandidateRecords.js";

describe("enrichCandidateRecords", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("keeps verified records and drops records with unreachable source URLs", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        records: [
          {
            description: "Cast vote in committee.",
            source_url: "https://good.example/a",
            event_date: "2026-01-05",
          },
          {
            description: "Discussed issue in event.",
            source_url: "https://bad.example/404",
            event_date: "2026-01-10",
          },
        ],
      },
      rawText: "ok",
    });

    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      if (url.includes("bad.example")) {
        return { ok: false, reason: "citation fetch returned status 404" };
      }
      return {
        ok: true,
        normalizedUrl: url,
        finalUrl: `${url}/final`,
        status: 200,
      };
    });

    const result = await enrichCandidateRecords(
      {
        candidateDisplayName: "Jane Doe",
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Assessor",
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }

    expect(result.records).toHaveLength(1);
    expect(result.records[0]?.source_url).toBe("https://good.example/a/final");
    expect(result.droppedRecords).toHaveLength(1);
    expect(result.droppedRecords[0]?.record.source_url).toBe("https://bad.example/404");
    expect(result.droppedRecords[0]?.failureType).toBe("permanent");
    expect(result.aiRawDebug?.dropped_records_count).toBe(1);
    expect(result.aiRawDebug?.verified_records_count).toBe(1);
  });

  it("classifies timeout URL failures as transient dropped records", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        records: [
          {
            description: "Released policy statement.",
            source_url: "https://slow.example/timeout",
            event_date: "2026-02-01",
          },
        ],
      },
      rawText: "ok",
    });

    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: false,
      reason: "citation URL fetch timed out",
    });

    const result = await enrichCandidateRecords(
      {
        candidateDisplayName: "John Smith",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Governor",
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }

    expect(result.records).toHaveLength(0);
    expect(result.droppedRecords).toHaveLength(1);
    expect(result.droppedRecords[0]?.failureType).toBe("transient");
  });

  it("keeps valid rows and marks schema-invalid rows as dropped for repair", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        records: [
          {
            description: "Valid description",
            source_url: "https://good.example/valid",
            event_date: "2026-02-01",
          },
          {
            description: "Has no date",
            source_url: "https://good.example/missing-date",
          },
        ],
      },
      rawText: "ok",
    });

    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://good.example/valid",
      finalUrl: "https://good.example/valid",
      status: 200,
    });

    const result = await enrichCandidateRecords(
      {
        candidateDisplayName: "Jane Doe",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Governor",
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }

    expect(result.records).toHaveLength(1);
    expect(result.droppedRecords).toHaveLength(1);
    expect(result.droppedRecords[0]?.failureKind).toBe("schema");
    expect(result.droppedRecords[0]?.reason).toContain("schema invalid row index=1");
  });

  it("drops pure candidacy rows as quality gaps before source verification", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        records: [
          {
            description: "The Secretary of State lists Jane Doe as a candidate for Governor.",
            source_url: "https://sos.example/candidates",
            event_date: "2026-05-01",
          },
          {
            description: "Jane Doe served as chair of the city budget committee.",
            source_url: "https://city.example/budget-committee",
            event_date: "2024-01-15",
          },
        ],
      },
      rawText: "ok",
    });

    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://city.example/budget-committee",
      finalUrl: "https://city.example/budget-committee",
      status: 200,
    });

    const result = await enrichCandidateRecords(
      {
        candidateDisplayName: "Jane Doe",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2026-11-03",
        officialBallotTitle: "Governor",
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }

    expect(result.records).toHaveLength(1);
    expect(result.records[0]?.source_url).toBe("https://city.example/budget-committee");
    expect(result.droppedRecords).toHaveLength(1);
    expect(result.droppedRecords[0]?.failureKind).toBe("quality_gap");
    expect(result.droppedRecords[0]?.failureType).toBe("permanent");
    expect(result.droppedRecords[0]?.reason).toContain("pure_candidacy");
    expect(result.aiRawDebug?.dropped_records_quality_count).toBe(1);
    expect(result.aiRawDebug?.quality_accepted_row_count).toBe(1);
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(1);
    expect(verifyHttpUrlReachabilityMock.mock.calls[0]?.[0]).toBe("https://city.example/budget-committee");
  });

  it("validator drops pure candidacy rows and keeps public-service records", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://city.example/service",
      finalUrl: "https://city.example/service",
      status: 200,
    });

    const result = await validateCandidateRecordDiscoveryPayload(
      {
        records: [
          {
            description: "The official candidate list shows Jane Doe qualified for the ballot.",
            source_url: "https://sos.example/ballot",
            event_date: "2026-05-01",
          },
          {
            description: "Jane Doe served as chair of the city budget committee.",
            source_url: "https://city.example/service",
            event_date: "2024-01-15",
          },
        ],
      },
      90000
    );

    expect(result.ok).toBe(true);
    if (!result.ok) {
      return;
    }

    expect(result.records).toEqual([
      {
        description: "Jane Doe served as chair of the city budget committee.",
        source_url: "https://city.example/service",
        event_date: "2024-01-15",
      },
    ]);
    expect(result.droppedRecords).toEqual([
      expect.objectContaining({
        failureKind: "quality_gap",
        failureType: "permanent",
        reason: expect.stringContaining("pure_candidacy"),
        record: expect.objectContaining({
          source_url: "https://sos.example/ballot",
        }),
      }),
    ]);
    expect(result.validationDebug).toEqual(
      expect.objectContaining({
        dropped_records_quality_count: 1,
        quality_accepted_row_count: 1,
        verified_records_count: 1,
      })
    );
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(1);
    expect(verifyHttpUrlReachabilityMock.mock.calls[0]?.[0]).toBe("https://city.example/service");
  });
});
