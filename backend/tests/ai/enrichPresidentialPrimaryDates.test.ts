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

describe("enrichPresidentialPrimaryDates", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://www.sos.ca.gov/elections/calendar",
      finalUrl: "https://www.sos.ca.gov/elections/calendar",
      status: 200,
    });
  });

  it("returns validated presidential primary dates from a provider payload", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        results: [
          {
            state_fips: "06",
            state_name: "California",
            status: "official_found",
            primary_date: "2028-03-07",
            sources: ["https://www.sos.ca.gov/elections/calendar"],
            notes: "Official election calendar lists the presidential primary date.",
          },
        ],
      },
      rawText: "{\"results\":[]}",
      debugMeta: { provider_debug: "ok" },
    });

    const { enrichPresidentialPrimaryDates } = await import(
      "../../src/ai/enrichPresidentialPrimaryDates.js"
    );
    const result = await enrichPresidentialPrimaryDates(
      {
        cycleId: "00000000-0000-4000-8000-000000000001",
        electionName: "2028 Democratic presidential primary",
        electionYear: 2028,
        party: "Democratic",
        stateFipsList: ["06"],
        scheduledFor: "2027-03-07T00:00:00.000Z",
      },
      { timeoutMs: 30_000 },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.provider).toBe("claude");
    expect(result.payload.results[0]).toMatchObject({
      state_fips: "06",
      status: "official_found",
      primary_date: "2028-03-07",
    });
    expect(result.aiRawDebug?.source_verifications).toEqual([
      {
        sourceUrl: "https://www.sos.ca.gov/elections/calendar",
        finalUrl: "https://www.sos.ca.gov/elections/calendar",
        status: 200,
        authority: "verified",
        sourceKind: "official_like",
      },
    ]);
  });

  it("retries with feedback after an unreachable source URL", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          results: [
            {
              state_fips: "06",
              state_name: "California",
              status: "official_found",
              primary_date: "2028-03-07",
              sources: ["https://dead.example.gov/calendar"],
              notes: "",
            },
          ],
        },
        rawText: "{\"bad\":true}",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          results: [
            {
              state_fips: "06",
              state_name: "California",
              status: "not_official_yet",
              primary_date: null,
              sources: ["https://www.sos.ca.gov/elections/calendar"],
              notes: "No official date is listed yet.",
            },
          ],
        },
        rawText: "{\"good\":true}",
      });
    verifyHttpUrlReachabilityMock
      .mockResolvedValueOnce({
        ok: false,
        normalizedUrl: "https://dead.example.gov/calendar",
        finalUrl: "https://dead.example.gov/calendar",
        reason: "HTTP 404",
      })
      .mockResolvedValueOnce({
        ok: true,
        normalizedUrl: "https://www.sos.ca.gov/elections/calendar",
        finalUrl: "https://www.sos.ca.gov/elections/calendar",
        status: 200,
      });

    const { enrichPresidentialPrimaryDates } = await import(
      "../../src/ai/enrichPresidentialPrimaryDates.js"
    );
    const result = await enrichPresidentialPrimaryDates(
      {
        cycleId: "00000000-0000-4000-8000-000000000001",
        electionName: "2028 Republican presidential primary",
        electionYear: 2028,
        party: "Republican",
        stateFipsList: ["06"],
        scheduledFor: "2027-03-07T00:00:00.000Z",
      },
      { timeoutMs: 30_000 },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
    const secondPrompt = callResearchProviderMock.mock.calls[1]?.[1] as string;
    expect(secondPrompt).toContain("Do not reuse this unreachable/dead source URL");
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.payload.results[0]?.status).toBe("not_official_yet");
    expect(result.aiRawDebug?.prior_failed_attempts).toHaveLength(1);
  });

  it("returns valid rows with failedRows after retry when only part of the batch remains invalid", async () => {
    const partialPayload = {
      results: [
        {
          state_fips: "06",
          state_name: "California",
          status: "official_found",
          primary_date: "2028-03-07",
          sources: ["https://www.sos.ca.gov/elections/calendar"],
          notes: "Official date found.",
        },
        {
          state_fips: "12",
          state_name: "Florida",
          status: "official_found",
          primary_date: "2028-03-14",
          sources: ["https://dead.example.gov/calendar"],
          notes: "Bad source keeps failing.",
        },
      ],
    };
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: partialPayload,
        rawText: "{\"partial\":1}",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: partialPayload,
        rawText: "{\"partial\":2}",
      });
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      if (url.includes("dead")) {
        return {
          ok: false,
          normalizedUrl: url,
          finalUrl: url,
          reason: "HTTP 404",
        };
      }
      return {
        ok: true,
        normalizedUrl: url,
        finalUrl: url,
        status: 200,
      };
    });

    const { enrichPresidentialPrimaryDates } = await import(
      "../../src/ai/enrichPresidentialPrimaryDates.js"
    );
    const result = await enrichPresidentialPrimaryDates(
      {
        cycleId: "00000000-0000-4000-8000-000000000001",
        electionName: "2028 Democratic presidential primary",
        electionYear: 2028,
        party: "Democratic",
        stateFipsList: ["06", "12"],
        scheduledFor: "2027-03-07T00:00:00.000Z",
      },
      { timeoutMs: 30_000 },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.payload.results.map((row) => row.state_fips)).toEqual(["06"]);
    expect(result.failedRows).toEqual([
      expect.objectContaining({
        state_fips: "12",
      }),
    ]);
    expect(result.aiRawDebug?.failed_rows).toEqual(result.failedRows);
    expect(result.aiRawDebug?.prior_failed_attempts).toHaveLength(1);
  });
});
