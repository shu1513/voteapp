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

import { enrichCandidateRoster } from "../../src/ai/enrichCandidateRoster.js";
import { enrichCandidateProfile } from "../../src/ai/enrichCandidateProfile.js";

describe("candidate citation verification retry behavior", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("retries roster once on same model with blocked URL feedback", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          candidates: [
            {
              display_name: "Jane Doe",
              sources: ["https://bad.example/404"],
            },
          ],
        },
        rawText: "first",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          candidates: [
            {
              display_name: "Jane Doe",
              sources: ["https://good.example/profile"],
            },
          ],
        },
        rawText: "second",
      });

    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      if (url.includes("bad.example")) {
        return { ok: false, reason: "citation fetch returned status 404" };
      }
      return { ok: true };
    });

    const result = await enrichCandidateRoster(
      {
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Assessor",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [{ provider: "openai", model: "gpt-test" }]
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);

    const secondPrompt = callResearchProviderMock.mock.calls[1]?.[1];
    expect(secondPrompt).toContain(
      'Do not use or cite this URL for "Jane Doe": https://bad.example/404 (citation fetch returned status 404)'
    );
  });

  it("carries blocked URL feedback from one profile model to the next", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          display_name: "John Smith",
          first_name: "John",
          last_name: "Smith",
          sources: ["https://bad.example/404"],
        },
        rawText: "first-model-attempt-1",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          display_name: "John Smith",
          first_name: "John",
          last_name: "Smith",
          sources: ["https://bad.example/404"],
        },
        rawText: "first-model-attempt-2",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          display_name: "John Smith",
          first_name: "John",
          last_name: "Smith",
          sources: ["https://good.example/profile"],
        },
        rawText: "second-model-attempt-1",
      });

    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      if (url.includes("bad.example")) {
        return { ok: false, reason: "citation fetch returned status 404" };
      }
      return { ok: true };
    });

    const result = await enrichCandidateProfile(
      {
        candidateDisplayName: "John Smith",
        districtName: "Los Angeles County, California",
        districtType: "county",
        state: "CA",
        electionDate: "2026-06-02",
        officialBallotTitle: "Assessor",
        seedUrls: [],
      },
      {
        timeoutMs: 90000,
      },
      [
        { provider: "openai", model: "gpt-model-a" },
        { provider: "openai", model: "gpt-model-b" },
      ]
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(3);

    const thirdPrompt = callResearchProviderMock.mock.calls[2]?.[1];
    expect(thirdPrompt).toContain(
      'Do not use or cite this URL for "John Smith": https://bad.example/404 (citation fetch returned status 404)'
    );
  });
});
