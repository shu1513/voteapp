import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { callResearchProviderMock, trimDebugTextMock, verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  callResearchProviderMock: vi.fn(),
  trimDebugTextMock: vi.fn((input: string) => input),
  verifyHttpUrlReachabilityMock: vi.fn(async (url: string) => ({
    ok: true,
    finalUrl: url,
    status: 200,
  })),
}));

vi.mock("../../src/ai/researchProviderClient.ts", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: trimDebugTextMock,
}));

vi.mock("../../src/ai/urlReachability.ts", () => ({
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

beforeEach(() => {
  verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => ({
    ok: true,
    finalUrl: url,
    status: 200,
  }));
});

afterEach(() => {
  callResearchProviderMock.mockReset();
  trimDebugTextMock.mockClear();
  verifyHttpUrlReachabilityMock.mockReset();
  vi.restoreAllMocks();
});

describe("enrichBallotMeasure shared provider wiring", () => {
  it("routes provider calls through researchProviderClient", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        official_measure_url: "https://example.org/measure-er.pdf",
        summary: "County measure to increase sales tax for public health services.",
        what_yes_means: "Approves a county sales tax increase for health services.",
        what_no_means: "Keeps current tax rates and funding levels.",
        research_area_tags: [
          { research_area_slug: "healthcare_affordability", stance: "for" },
          { research_area_slug: "cost_of_living_reduction", stance: "against" },
        ],
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
        allowedResearchAreaSlugs: ["healthcare_affordability", "cost_of_living_reduction"],
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
      expect(result.summary).toBe("County measure to increase sales tax for public health services.");
      expect(result.researchAreaTags).toEqual([
        { researchAreaSlug: "healthcare_affordability", stance: "for" },
        { researchAreaSlug: "cost_of_living_reduction", stance: "against" },
      ]);
      expect(result.researchUrls).toContain("https://example.org/measure-er.pdf");
    }
  });

  it("rejects research-area tags outside the allowed ballot-measure list", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        official_measure_url: "https://example.org/measure-er.pdf",
        summary: "County measure to increase sales tax for public health services.",
        what_yes_means: "Approves a county sales tax increase for health services.",
        what_no_means: "Keeps current tax rates and funding levels.",
        research_area_tags: [{ research_area_slug: "legal_competence", stance: "for" }],
        sources: ["https://example.org/measure-er.pdf"],
      },
      rawText: "{\"official_measure_url\":\"https://example.org/measure-er.pdf\"}",
      debugMeta: {},
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
        allowedResearchAreaSlugs: ["healthcare_affordability", "cost_of_living_reduction"],
      },
      {
        timeoutMs: 1000,
        openAiApiKey: "test-openai-key",
      },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("legal_competence");
    }
  });

  it("retries when official_measure_url returns 403 and asks the model to actively investigate it", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          official_measure_url: "https://elections.example.gov/measure-er",
          summary: "County measure to increase sales tax for public health services.",
          what_yes_means: "Approves a county sales tax increase for health services.",
          what_no_means: "Keeps current tax rates and funding levels.",
          research_area_tags: [{ research_area_slug: "healthcare_affordability", stance: "for" }],
          sources: ["https://elections.example.gov/measure-er"],
        },
        rawText: "{\"official_measure_url\":\"https://elections.example.gov/measure-er\"}",
        debugMeta: {},
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          official_measure_url: "https://elections.example.gov/measure-er-full-text.pdf",
          summary: "County measure to increase sales tax for public health services.",
          what_yes_means: "Approves a county sales tax increase for health services.",
          what_no_means: "Keeps current tax rates and funding levels.",
          research_area_tags: [{ research_area_slug: "healthcare_affordability", stance: "for" }],
          sources: ["https://elections.example.gov/measure-er-full-text.pdf"],
        },
        rawText: "{\"official_measure_url\":\"https://elections.example.gov/measure-er-full-text.pdf\"}",
        debugMeta: {},
      });
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => ({
      ok: true,
      finalUrl: url,
      status: url.endsWith("/measure-er") ? 403 : 200,
    }));

    const { enrichBallotMeasure } = await import("../../src/ai/enrichBallotMeasure.ts");
    const result = await enrichBallotMeasure(
      {
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Measure ER",
        seedUrls: [],
        allowedResearchAreaSlugs: ["healthcare_affordability"],
      },
      {
        timeoutMs: 1000,
        openAiApiKey: "test-openai-key",
      },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
    expect(callResearchProviderMock.mock.calls[1]?.[1]).toContain(
      "Actively investigate that URL and related official election-authority pages."
    );
    expect(callResearchProviderMock.mock.calls[1]?.[1]).toContain(
      "Do not return that URL again as official_measure_url."
    );
    expect(callResearchProviderMock.mock.calls[1]?.[1]).not.toContain(
      "Do not use or cite this URL: https://elections.example.gov/measure-er"
    );
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.officialMeasureUrl).toBe("https://elections.example.gov/measure-er-full-text.pdf");
    }
  });

  it("rejects a 403 official URL when the retry returns it again", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        official_measure_url: "https://elections.example.gov/measure-er",
        summary: "County measure to increase sales tax for public health services.",
        what_yes_means: "Approves a county sales tax increase for health services.",
        what_no_means: "Keeps current tax rates and funding levels.",
        research_area_tags: [{ research_area_slug: "healthcare_affordability", stance: "for" }],
        sources: ["https://elections.example.gov/measure-er"],
      },
      rawText: "{\"official_measure_url\":\"https://elections.example.gov/measure-er\"}",
      debugMeta: {},
    });
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      finalUrl: "https://elections.example.gov/measure-er",
      status: 403,
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
        allowedResearchAreaSlugs: ["healthcare_affordability"],
      },
      {
        timeoutMs: 1000,
        openAiApiKey: "test-openai-key",
      },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("official_measure_url returned HTTP 403");
    }
  });
});
