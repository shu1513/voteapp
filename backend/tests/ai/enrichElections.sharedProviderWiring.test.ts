import { afterEach, describe, expect, it, vi } from "vitest";

const callResearchProviderMock = vi.fn();
const trimDebugTextMock = vi.fn((input: string) => input);

vi.mock("../../src/ai/researchProviderClient.ts", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: trimDebugTextMock,
}));

vi.mock("../../src/ai/urlReachability.ts", () => ({
  verifyHttpUrlReachability: vi.fn(async (url: string) => ({
    ok: true,
    finalUrl: url,
  })),
}));

afterEach(() => {
  callResearchProviderMock.mockReset();
  trimDebugTextMock.mockClear();
  vi.restoreAllMocks();
});

describe("enrichElections shared provider wiring", () => {
  it("routes provider calls through researchProviderClient", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        entries: [
          {
            official_ballot_title: "School Board Member",
            election_date: "2026-11-03",
            impact: "Sets school district policy and budget priorities.",
            race_type: "office",
            sources: ["https://example.org/election"],
          },
        ],
      },
      rawText: "{\"entries\":[{\"official_ballot_title\":\"School Board Member\"}]}",
      debugMeta: { provider_test_flag: true },
    });

    const { enrichElections } = await import("../../src/ai/enrichElections.ts");
    const result = await enrichElections(
      {
        ingestKey: "test:elections:wiring",
        draft: {
          district_id: "district-1",
          district_name: "Demo Unified School District",
          district_type: "school_unified",
          state: "CA",
        },
        promptVersion: "elections_v2",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      {
        timeoutMs: 1000,
        openAiApiKey: "test-openai-key",
      },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(callResearchProviderMock).toHaveBeenCalledTimes(1);
    expect(callResearchProviderMock).toHaveBeenCalledWith(
      { provider: "openai", model: "gpt-5.4-mini" },
      expect.any(String),
      expect.objectContaining({
        timeoutMs: 1000,
        openAiApiKey: "test-openai-key",
      })
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.provider).toBe("openai");
      expect(result.model).toBe("gpt-5.4-mini");
      expect(result.payload.entries).toHaveLength(1);
      expect(result.payload.entries[0]?.official_ballot_title).toBe("School Board Member");
      expect(result.payload.entries[0]?.race_type).toBe("office");
      expect(result.payload.entries[0]?.is_partisan).toBe(false);
    }
  });
});
