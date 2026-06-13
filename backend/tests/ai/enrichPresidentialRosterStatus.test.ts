import { beforeEach, describe, expect, it, vi } from "vitest";

const callResearchProviderMock = vi.hoisted(() => vi.fn());

vi.mock("../../src/ai/researchProviderClient.js", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (text: string) => text,
}));

const input = {
  cycleId: "cycle-1",
  electionYear: 2028,
  stage: "primary" as const,
  party: "Democratic",
  candidates: [
    {
      candidateId: "candidate-1",
      displayName: "Jane President",
      party: "Democratic",
      fecIds: ["P80000001"],
      sources: ["https://example.org/jane"],
    },
  ],
};

describe("enrichPresidentialRosterStatus", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns parsed presidential roster status rows from a provider payload", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            candidate_id: "candidate-1",
            status: "WITHDRAWN",
            sources: ["https://example.org/jane-withdrawn"],
            notes: "Suspended campaign.",
          },
        ],
      },
      rawText: "{\"candidates\":[]}",
      debugMeta: { provider_debug: "ok" },
    });

    const { enrichPresidentialRosterStatus } = await import("../../src/ai/enrichPresidentialRosterStatus.js");
    const result = await enrichPresidentialRosterStatus(input, { timeoutMs: 30_000 }, [
      { provider: "claude", model: "claude-sonnet-4-6" },
    ]);

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.provider).toBe("claude");
    expect(result.candidates).toEqual([
      {
        candidate_id: "candidate-1",
        status: "withdrawn",
        sources: ["https://example.org/jane-withdrawn"],
        notes: "Suspended campaign.",
      },
    ]);
    expect(result.aiRawDebug?.provider_debug).toBe("ok");
  });

  it("retries once with validation feedback after a schema mismatch", async () => {
    callResearchProviderMock
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          candidates: [
            {
              candidate_id: "candidate-x",
              status: "withdrawn",
              sources: ["https://example.org/wrong"],
              notes: "Wrong candidate.",
            },
          ],
        },
        rawText: "{\"bad\":true}",
      })
      .mockResolvedValueOnce({
        ok: true,
        parsed: {
          candidates: [
            {
              candidate_id: "candidate-1",
              status: "unknown",
              sources: ["https://example.org/jane"],
              notes: "Could not confirm current status.",
            },
          ],
        },
        rawText: "{\"good\":true}",
      });

    const { enrichPresidentialRosterStatus } = await import("../../src/ai/enrichPresidentialRosterStatus.js");
    const result = await enrichPresidentialRosterStatus(input, { timeoutMs: 30_000 }, [
      { provider: "claude", model: "claude-sonnet-4-6" },
    ]);

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
    const secondPrompt = String(callResearchProviderMock.mock.calls[1]?.[1]);
    expect(secondPrompt).toContain("Previous feedback to fix:");
    expect(secondPrompt).toContain("candidate_id candidate-x was not provided for verification");
  });

  it("normalizes expected candidate IDs before schema validation", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            candidate_id: "candidate-1",
            status: "active",
            sources: ["https://example.org/jane"],
            notes: "Still campaigning.",
          },
        ],
      },
      rawText: "{\"candidates\":[]}",
    });

    const { enrichPresidentialRosterStatus } = await import("../../src/ai/enrichPresidentialRosterStatus.js");
    const result = await enrichPresidentialRosterStatus(
      {
        ...input,
        candidates: [
          {
            ...input.candidates[0]!,
            candidateId: " candidate-1 ",
          },
        ],
      },
      { timeoutMs: 30_000 },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(1);
  });

  it("returns provider failures with a status prompt preview", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: "temporary outage",
      failureDebug: { detail: "upstream" },
    });

    const { enrichPresidentialRosterStatus } = await import("../../src/ai/enrichPresidentialRosterStatus.js");
    const result = await enrichPresidentialRosterStatus(input, { timeoutMs: 30_000 }, [
      { provider: "claude", model: "claude-sonnet-4-6" },
    ]);

    expect(result).toMatchObject({
      ok: false,
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      reason: "temporary outage",
    });
    expect(result.ok ? undefined : result.failureDebug?.prompt_preview).toContain(
      "verifying the current status of presidential candidates omitted"
    );
  });
});
