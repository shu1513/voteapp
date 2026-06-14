import { beforeEach, describe, expect, it, vi } from "vitest";
import { PRESIDENTIAL_NOMINEE_AI_CANDIDATES } from "../../src/ai/aiCandidates.js";

const callResearchProviderMock = vi.hoisted(() => vi.fn());

vi.mock("../../src/ai/researchProviderClient.js", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (text: string) => text,
}));

const input = {
  cycleId: "cycle-1",
  electionYear: 2028,
  party: "Democratic",
  candidates: [
    {
      candidateId: "candidate-1",
      displayName: "Jane President",
      party: "Democratic",
      fecIds: ["P80000001"],
    },
  ],
};

describe("enrichPresidentialNominee", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns parsed nominee payload from a provider response", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        nominee_found: true,
        candidate_name: " Jane President ",
        fec_candidate_id: "p80000001",
        sources: ["https://example.org/nominee"],
      },
      rawText: "{\"nominee_found\":true}",
      debugMeta: { provider_debug: "ok" },
    });

    const { enrichPresidentialNominee } = await import("../../src/ai/enrichPresidentialNominee.js");
    const result = await enrichPresidentialNominee(input, { timeoutMs: 30_000 }, [
      { provider: "claude", model: "claude-opus-4-8" },
    ]);

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.provider).toBe("claude");
    expect(result.payload).toEqual({
      nominee_found: true,
      candidate_name: "Jane President",
      fec_candidate_id: "P80000001",
      sources: ["https://example.org/nominee"],
    });
    expect(result.aiRawDebug?.provider_debug).toBe("ok");
  });

  it("retries once with validation feedback after a schema mismatch", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          nominee_found: true,
          candidate_name: "Jane President",
          fec_candidate_id: "H0CA00001",
          sources: ["https://example.org/bad"],
        },
        rawText: "{\"bad\":true}",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          nominee_found: false,
          sources: ["https://example.org/no-nominee"],
        },
        rawText: "{\"good\":true}",
      });

    const { enrichPresidentialNominee } = await import("../../src/ai/enrichPresidentialNominee.js");
    const result = await enrichPresidentialNominee(input, { timeoutMs: 30_000 }, [
      { provider: "claude", model: "claude-opus-4-8" },
    ]);

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
    const secondPrompt = String(callResearchProviderMock.mock.calls[1]?.[1]);
    expect(secondPrompt).toContain("Previous feedback to fix:");
    expect(secondPrompt).toContain("payload.fec_candidate_id must be a presidential FEC ID when present");
  });

  it("returns provider failures with a nominee prompt preview", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: "temporary outage",
      failureDebug: { detail: "upstream" },
    });

    const { enrichPresidentialNominee } = await import("../../src/ai/enrichPresidentialNominee.js");
    const result = await enrichPresidentialNominee(input, { timeoutMs: 30_000 }, [
      { provider: "claude", model: "claude-opus-4-8" },
    ]);

    expect(result).toMatchObject({
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: "temporary outage",
    });
    expect(result.ok ? undefined : result.failureDebug?.prompt_preview).toContain(
      "researching whether one U.S. presidential primary has a nominee"
    );
  });

  it("uses the dedicated presidential nominee model list by default", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        nominee_found: false,
        sources: ["https://example.org/no-nominee"],
      },
      rawText: "{\"nominee_found\":false}",
    });

    const { enrichPresidentialNominee } = await import("../../src/ai/enrichPresidentialNominee.js");
    const result = await enrichPresidentialNominee(input, { timeoutMs: 30_000 });

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledWith(
      PRESIDENTIAL_NOMINEE_AI_CANDIDATES[0],
      expect.any(String),
      expect.objectContaining({ timeoutMs: 30_000 })
    );
  });
});
