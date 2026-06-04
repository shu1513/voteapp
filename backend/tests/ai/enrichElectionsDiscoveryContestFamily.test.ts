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

  it("keeps specific family over all during dedupe", async () => {
    const { dedupeMergedEntries } = await import("../../src/ai/enrichElections.ts");

    const result = dedupeMergedEntries([
      {
        ...buildEntry("County Clerk"),
        race_type: "office",
        discovery_contest_family: "all",
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
});
