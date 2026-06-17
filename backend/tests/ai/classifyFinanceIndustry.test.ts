import { afterEach, describe, expect, it, vi } from "vitest";

const callResearchProviderMock = vi.hoisted(() => vi.fn());

vi.mock("../../src/ai/researchProviderClient.js", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (text: string) => text,
}));

import { classifyFinanceIndustriesWithAi } from "../../src/ai/classifyFinanceIndustry.js";

const aiCandidates = [
  { provider: "openai" as const, model: "model-a" },
  { provider: "claude" as const, model: "model-b" },
];

describe("classifyFinanceIndustriesWithAi", () => {
  afterEach(() => {
    callResearchProviderMock.mockReset();
  });

  it("matches classifications by label type and normalized label", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        classifications: [
          {
            label_type: "employer",
            normalized_label: "ACME",
            industry_slug: "technology",
            confidence: "high",
          },
          {
            label_type: "donor",
            normalized_label: "ACME",
            industry_slug: "environmental_group",
            confidence: "medium",
          },
        ],
      },
      rawText: "{}",
      debugMeta: {},
    });

    const result = await classifyFinanceIndustriesWithAi({
      aiCandidates,
      config: { timeoutMs: 1000 },
      labels: [
        {
          rawLabel: "Acme LLC",
          labelType: "employer",
          normalizedLabel: "ACME",
          amount: 250_000,
        },
        {
          rawLabel: "Acme Advocacy",
          labelType: "donor",
          normalizedLabel: "ACME",
          amount: 500_000,
        },
      ],
    });

    expect(result.ok).toBe(true);
    expect(result.ok ? result.classifications : []).toEqual([
      {
        rawLabel: "Acme LLC",
        labelType: "employer",
        normalizedLabel: "ACME",
        industrySlug: "technology",
        confidence: "high",
        classificationSource: "ai",
        matchedRule: null,
      },
      {
        rawLabel: "Acme Advocacy",
        labelType: "donor",
        normalizedLabel: "ACME",
        industrySlug: "environmental_group",
        confidence: "medium",
        classificationSource: "ai",
        matchedRule: null,
      },
    ]);
  });

  it("does not try later providers after a non-retryable provider failure", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: false,
      retryable: false,
      errorCode: "CONFIGURATION_ERROR",
      reason: "bad key",
      failureDebug: { provider: "openai" },
    });

    const result = await classifyFinanceIndustriesWithAi({
      aiCandidates,
      config: { timeoutMs: 1000 },
      labels: [
        {
          rawLabel: "Acme LLC",
          labelType: "employer",
          normalizedLabel: "ACME",
          amount: 250_000,
        },
      ],
    });

    expect(result).toEqual({
      ok: false,
      retryable: false,
      errorCode: "CONFIGURATION_ERROR",
      reason: "bad key",
      failureDebug: { provider: "openai" },
    });
    expect(callResearchProviderMock).toHaveBeenCalledTimes(1);
  });
});
