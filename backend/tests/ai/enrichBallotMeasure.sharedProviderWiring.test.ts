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

describe("enrichBallotMeasure shared provider wiring", () => {
  it("routes provider calls through researchProviderClient", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        official_measure_url: "https://example.org/measure-er.pdf",
        what_yes_means: "Approves a county sales tax increase for health services.",
        what_no_means: "Keeps current tax rates and funding levels.",
        sources: [
          "https://example.org/measure-er.pdf",
          "https://example.org/county-voter-guide",
        ],
      },
      rawText: "{\"official_measure_url\":\"https://example.org/measure-er.pdf\"}",
      debugMeta: { provider_test_flag: true },
    });

    const { enrichBallotMeasure } = await import("../../src/ai/enrichBallotMeasure.ts");
    const result = await enrichBallotMeasure(
      {
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Measure ER",
        seedUrls: [],
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
      expect(result.officialMeasureUrl).toBe("https://example.org/measure-er.pdf");
      expect(result.researchUrls).toContain("https://example.org/measure-er.pdf");
    }
  });
});

