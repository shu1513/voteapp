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

  it("matches classifications by input id while preserving label context", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        classifications: [
          {
            id: "1",
            industry_slug: "technology",
            confidence: "high",
          },
          {
            id: "2",
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
        },
        {
          rawLabel: "Acme Advocacy",
          labelType: "donor",
          normalizedLabel: "ACME",
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

  it("tries later providers after a non-retryable provider-local failure", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: false,
        retryable: false,
        errorCode: "CONFIGURATION_ERROR",
        reason: "OPENAI_API_KEY is missing",
        failureDebug: { provider: "openai" },
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          classifications: [
            {
              id: "1",
              industry_slug: "technology",
              confidence: "high",
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
        },
      ],
    });

    expect(result).toMatchObject({
      ok: true,
      provider: "claude",
      model: "model-b",
      classifications: [
        {
          rawLabel: "Acme LLC",
          labelType: "employer",
          normalizedLabel: "ACME",
          industrySlug: "technology",
        },
      ],
    });
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
  });

  it("tries later providers when a provider omits an expected classification id", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          classifications: [
            {
              id: "1",
              industry_slug: "technology",
              confidence: "high",
            },
          ],
        },
        rawText: "{}",
        debugMeta: {},
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          classifications: [
            {
              id: "1",
              industry_slug: "technology",
              confidence: "high",
            },
            {
              id: "2",
              industry_slug: "unknown",
              confidence: "unknown",
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
        },
        {
          rawLabel: "Unknown Group",
          labelType: "donor",
          normalizedLabel: "UNKNOWN GROUP",
        },
      ],
    });

    expect(result).toMatchObject({
      ok: true,
      provider: "claude",
      model: "model-b",
      classifications: [
        {
          rawLabel: "Acme LLC",
          labelType: "employer",
          normalizedLabel: "ACME",
          industrySlug: "technology",
          classificationSource: "ai",
        },
        {
          rawLabel: "Unknown Group",
          labelType: "donor",
          normalizedLabel: "UNKNOWN GROUP",
          industrySlug: null,
          classificationSource: "unknown",
        },
      ],
    });
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
  });

  it("returns schema mismatch when every provider returns incomplete classifications", async () => {
    callResearchProviderMock.mockResolvedValue({
      ok: true,
      parsed: {
        classifications: [
          {
            id: "1",
            industry_slug: "technology",
            confidence: "high",
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
        },
        {
          rawLabel: "Missing LLC",
          labelType: "employer",
          normalizedLabel: "MISSING",
        },
      ],
    });

    expect(result).toMatchObject({
      ok: false,
      retryable: true,
      errorCode: "SCHEMA_MISMATCH",
    });
    expect(result.ok ? "" : result.reason).toContain("Expected one classification for each input id");
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
  });
});
