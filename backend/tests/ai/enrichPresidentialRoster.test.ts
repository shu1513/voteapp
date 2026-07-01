import { beforeEach, describe, expect, it, vi } from "vitest";
import { PRESIDENTIAL_ROSTER_AI_CANDIDATES } from "../../src/ai/aiCandidates.js";

const callResearchProviderMock = vi.hoisted(() => vi.fn());

vi.mock("../../src/ai/researchProviderClient.js", () => ({
  callResearchProvider: callResearchProviderMock,
  trimDebugText: (text: string) => text,
}));

describe("enrichPresidentialRoster", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns parsed presidential roster candidates from a provider payload", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "p80000001",
            sources: ["https://example.org/jane"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "active",
          },
        ],
      },
      rawText: "{\"candidates\":[]}",
      debugMeta: { provider_debug: "ok" },
    });

    const { enrichPresidentialRoster } = await import("../../src/ai/enrichPresidentialRoster.js");
    const result = await enrichPresidentialRoster(
      {
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
      },
      { timeoutMs: 30_000 },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.provider).toBe("claude");
    expect(result.candidates).toEqual([
      {
        display_name: "Jane President",
        party: "Democratic",
        fec_candidate_id: "P80000001",
        sources: ["https://example.org/jane"],
        qualification_evidence: [
          {
            kind: "official_campaign_website",
            source_url: "https://jane.example.org",
          },
        ],
        status: "active",
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
              display_name: "Wrong Primary",
              party: "Republican",
              fec_candidate_id: "P80000002",
              sources: ["https://example.org/wrong"],
              qualification_evidence: [
                {
                  kind: "official_campaign_website",
                  source_url: "https://wrong.example.org",
                },
              ],
              status: "active",
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
              display_name: "Jane President",
              party: "Democratic",
              fec_candidate_id: "P80000001",
              sources: ["https://example.org/jane"],
              qualification_evidence: [
                {
                  kind: "official_campaign_website",
                  source_url: "https://jane.example.org",
                },
              ],
              status: "active",
            },
          ],
        },
        rawText: "{\"good\":true}",
      });

    const { enrichPresidentialRoster } = await import("../../src/ai/enrichPresidentialRoster.js");
    const result = await enrichPresidentialRoster(
      {
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
      },
      { timeoutMs: 30_000 },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(2);
    const secondPrompt = String(callResearchProviderMock.mock.calls[1]?.[1]);
    expect(secondPrompt).toContain("Previous feedback to fix:");
    expect(secondPrompt).toContain("candidate.party does not match expected party Democratic");
  });

  it("normalizes expected primary party before schema validation", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "active",
          },
        ],
      },
      rawText: "{\"candidates\":[]}",
    });

    const { enrichPresidentialRoster } = await import("../../src/ai/enrichPresidentialRoster.js");
    const result = await enrichPresidentialRoster(
      {
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: " Democratic ",
      },
      { timeoutMs: 30_000 },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledTimes(1);
  });

  it("uses the dedicated presidential roster model list by default", async () => {
    callResearchProviderMock.mockResolvedValueOnce({
      ok: true,
      parsed: {
        candidates: [
          {
            display_name: "Jane President",
            party: "Democratic",
            fec_candidate_id: "P80000001",
            sources: ["https://example.org/jane"],
            qualification_evidence: [
              {
                kind: "official_campaign_website",
                source_url: "https://jane.example.org",
              },
            ],
            status: "active",
          },
        ],
      },
      rawText: "{\"candidates\":[]}",
    });

    const { enrichPresidentialRoster } = await import("../../src/ai/enrichPresidentialRoster.js");
    const result = await enrichPresidentialRoster(
      {
        cycleId: "cycle-1",
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
      },
      { timeoutMs: 30_000 }
    );

    expect(result.ok).toBe(true);
    expect(callResearchProviderMock).toHaveBeenCalledWith(
      PRESIDENTIAL_ROSTER_AI_CANDIDATES[0],
      expect.any(String),
      expect.objectContaining({ timeoutMs: 30_000 })
    );
  });
});
