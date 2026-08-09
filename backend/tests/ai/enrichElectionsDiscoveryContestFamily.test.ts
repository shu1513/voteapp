import { afterEach, describe, expect, it, vi } from "vitest";

const callResearchProviderMock = vi.fn();

vi.mock("../../src/ai/researchProviderClient.ts", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (input: string) => input,
}));

vi.mock("../../src/ai/urlReachability.ts", () => ({
  verifyHttpUrlReachability: vi.fn(async (url: string) => ({
    ok: true,
    finalUrl: url,
  })),
}));

function buildEntry(title: string) {
  return {
    official_ballot_title: title,
    election_date: "2026-11-03",
    sources: ["https://example.org/election"],
  };
}

function buildEmptyResult() {
  return {
    ok: true as const,
    parsed: { entries: [] },
    rawText: "{\"entries\":[]}",
    debugMeta: {},
  };
}

afterEach(() => {
  callResearchProviderMock.mockReset();
  vi.restoreAllMocks();
});

describe("enrichElections discovery contest family provenance", () => {
  it("attaches the producing contest family to returned entries", async () => {
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: judicial_office")) {
        return {
          ok: true,
          parsed: { entries: [buildEntry("Judge of the Superior Court")] },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildEmptyResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:family:judicial",
        draft: {
          district_id: "district-1",
          district_name: "Sample County",
          district_type: "county",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries).toHaveLength(1);
      expect(result.payload.entries[0].discovery_contest_family).toBe("judicial_office");
    }
  });

  it("does not blindly prefer judicial family when specific families conflict during dedupe", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { dedupeMergedEntries } = await import("../../src/ai/enrichElections.ts");

    const result = dedupeMergedEntries([
      {
        ...buildEntry("County Clerk"),
        race_type: "office",
        discovery_contest_family: "non_judicial_office",
      },
      {
        ...buildEntry("County Clerk"),
        race_type: "office",
        discovery_contest_family: "judicial_office",
      },
    ]);

    expect(result).toHaveLength(1);
    expect(result[0].discovery_contest_family).toBe("non_judicial_office");
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("election family provenance conflict")
    );
  });

  it("keeps specific family over missing provenance during dedupe", async () => {
    const { dedupeMergedEntries } = await import("../../src/ai/enrichElections.ts");

    const result = dedupeMergedEntries([
      {
        ...buildEntry("County Clerk"),
        race_type: "office",
      },
      {
        ...buildEntry("County Clerk"),
        race_type: "office",
        discovery_contest_family: "non_judicial_office",
      },
    ]);

    expect(result).toHaveLength(1);
    expect(result[0].discovery_contest_family).toBe("non_judicial_office");
  });

  it("keeps us_senate over non_judicial_office without logging a conflict", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { dedupeMergedEntries } = await import("../../src/ai/enrichElections.ts");

    const result = dedupeMergedEntries([
      {
        ...buildEntry("United States Senator"),
        race_type: "office",
        discovery_contest_family: "non_judicial_office",
      },
      {
        ...buildEntry("United States Senator"),
        race_type: "office",
        discovery_contest_family: "us_senate",
      },
    ]);

    expect(result).toHaveLength(1);
    expect(result[0].discovery_contest_family).toBe("us_senate");
    expect(warnSpy).not.toHaveBeenCalled();
  });

  it("drops presidential entries from non-judicial family results without discarding valid offices", async () => {
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: non_judicial_office")) {
        return {
          ok: true,
          parsed: {
            entries: [
              buildEntry("Governor"),
              buildEntry("President and Vice President"),
            ],
          },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildEmptyResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:family:president-filter",
        draft: {
          district_id: "district-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries).toHaveLength(1);
      expect(result.payload.entries[0].official_ballot_title).toBe("Governor");
      expect(result.payload.entries[0].discovery_contest_family).toBe("non_judicial_office");
      expect(result.aiRawDebug?.family_debug).toMatchObject({
        non_judicial_office: {
          dropped_presidential_titles: ["President and Vice President"],
        },
      });
    }
  });

  it("keeps a prosecutor in the non-judicial family and leaves its partisanship alone", async () => {
    // Every Georgia DA's ballot title names the circuit, and O.C.G.A. 15-6-1
    // names all 50+ circuits "<X> Judicial Circuit". The family validator's own
    // copy of the judicial keyword list counted that as judicial, so a pass
    // holding only the district attorney was rejected as "fully judicial".
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: non_judicial_office")) {
        return {
          ok: true,
          parsed: {
            entries: [
              {
                ...buildEntry("District Attorney - Paulding Judicial Circuit"),
                is_partisan: true,
              },
            ],
          },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildEmptyResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:family:prosecutor",
        draft: {
          district_id: "1dca234a-876f-4957-812c-3fedf8e0a7cb",
          district_name: "Paulding County, Georgia",
          district_type: "county",
          state: "GA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries).toHaveLength(1);
      expect(result.payload.entries[0].discovery_contest_family).toBe("non_judicial_office");
      // Georgia is nonpartisan for judges; its DAs are nominated in party
      // primaries and printed with a party, so the researched value stands.
      expect(result.payload.entries[0].is_partisan).toBe(true);
    }
  });

  it("retries when non-judicial family results contain only presidential entries", async () => {
    let nonJudicialCalls = 0;
    callResearchProviderMock.mockImplementation(async (_candidate, prompt: string) => {
      if (prompt.includes("Contest family for this call: non_judicial_office")) {
        nonJudicialCalls += 1;
        if (nonJudicialCalls === 1) {
          return {
            ok: true,
            parsed: { entries: [buildEntry("President and Vice President")] },
            rawText: "{}",
            debugMeta: {},
          };
        }
        expect(prompt).toContain("returned only presidential contests");
        return {
          ok: true,
          parsed: { entries: [buildEntry("Governor")] },
          rawText: "{}",
          debugMeta: {},
        };
      }
      return buildEmptyResult();
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:family:president-only-retry",
        draft: {
          district_id: "district-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      { timeoutMs: 1000, openAiApiKey: "test-key" },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(nonJudicialCalls).toBe(2);
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.entries.map((entry) => entry.official_ballot_title)).toEqual([
        "Governor",
      ]);
    }
  });
});
