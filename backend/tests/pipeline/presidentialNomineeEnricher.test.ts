import { describe, expect, it, vi } from "vitest";

import { enrichPresidentialNomineeForCycle } from "../../src/pipeline/enrichers/presidentialNomineeEnricher.js";
import type { PresidentialNomineeCandidateForResolution } from "../../src/pipeline/presidential/presidentialNomineeResolver.js";

const CYCLE_ID = "11111111-1111-4111-8111-111111111111";

const activeCandidates: PresidentialNomineeCandidateForResolution[] = [
  {
    candidateId: "candidate-1",
    displayName: "Jane President",
    party: "Democratic",
    fecIds: ["P80000001"],
  },
];

describe("enrichPresidentialNomineeForCycle", () => {
  it("loads active candidates, calls nominee AI, and resolves the nominee", async () => {
    const loadCandidatesForResolution = vi.fn().mockResolvedValue(activeCandidates);
    const enrichNominee = vi.fn().mockResolvedValue({
      ok: true,
      provider: "claude",
      model: "claude-opus-4-8",
      payload: {
        nominee_found: true,
        candidate_name: "Jane President",
        fec_candidate_id: "P80000001",
        sources: ["https://example.org/nominee"],
      },
      aiRawDebug: { debug: "ok" },
    });

    const result = await enrichPresidentialNomineeForCycle({
      db: { query: vi.fn() } as never,
      cycleId: ` ${CYCLE_ID} `,
      electionYear: 2028,
      party: " Democratic ",
      aiConfig: { timeoutMs: 1000 },
      loadCandidatesForResolution,
      enrichNominee,
    });

    expect(loadCandidatesForResolution).toHaveBeenCalledWith(expect.any(Object), CYCLE_ID);
    expect(enrichNominee).toHaveBeenCalledWith(
      {
        cycleId: CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        candidates: activeCandidates,
      },
      { timeoutMs: 1000 },
      undefined
    );
    expect(result).toEqual({
      ok: true,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      provider: "claude",
      model: "claude-opus-4-8",
      candidateCount: 1,
      resolution: {
        status: "matched",
        candidateId: "candidate-1",
        displayName: "Jane President",
        method: "exact_fec_id",
        candidateName: "Jane President",
        fecCandidateId: "P80000001",
        sources: ["https://example.org/nominee"],
      },
      aiRawDebug: { debug: "ok" },
    });
  });

  it("returns no_nominee_found as a successful researched result", async () => {
    const result = await enrichPresidentialNomineeForCycle({
      db: { query: vi.fn() } as never,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      loadCandidatesForResolution: vi.fn().mockResolvedValue(activeCandidates),
      enrichNominee: vi.fn().mockResolvedValue({
        ok: true,
        provider: "openai",
        model: "gpt-5.5",
        payload: {
          nominee_found: false,
          sources: ["https://example.org/no-nominee"],
        },
        aiRawDebug: null,
      }),
    });

    expect(result).toMatchObject({
      ok: true,
      resolution: {
        status: "no_nominee_found",
        sources: ["https://example.org/no-nominee"],
      },
    });
  });

  it("fails closed when no active candidates are available for nominee research", async () => {
    const enrichNominee = vi.fn();

    const result = await enrichPresidentialNomineeForCycle({
      db: { query: vi.fn() } as never,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      loadCandidatesForResolution: vi.fn().mockResolvedValue([]),
      enrichNominee,
    });

    expect(enrichNominee).not.toHaveBeenCalled();
    expect(result).toEqual({
      ok: false,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      error: "No active presidential primary candidates are available for nominee research",
      retryable: false,
      errorCode: "NO_ACTIVE_CANDIDATES",
    });
  });

  it("passes through nominee AI failures", async () => {
    const result = await enrichPresidentialNomineeForCycle({
      db: { query: vi.fn() } as never,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      aiConfig: { timeoutMs: 1000 },
      loadCandidatesForResolution: vi.fn().mockResolvedValue(activeCandidates),
      enrichNominee: vi.fn().mockResolvedValue({
        ok: false,
        retryable: true,
        errorCode: "TEMP_PROVIDER_ERROR",
        reason: "provider unavailable",
        failureDebug: { detail: "upstream" },
      }),
    });

    expect(result).toEqual({
      ok: false,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      error: "provider unavailable",
      retryable: true,
      errorCode: "TEMP_PROVIDER_ERROR",
      failureDebug: { detail: "upstream" },
    });
  });

  it("rejects invalid cycle context before calling AI", async () => {
    const loadCandidatesForResolution = vi.fn();
    const enrichNominee = vi.fn();

    await expect(
      enrichPresidentialNomineeForCycle({
        db: { query: vi.fn() } as never,
        cycleId: " ",
        electionYear: 2028,
        party: "Democratic",
        loadCandidatesForResolution,
        enrichNominee,
      })
    ).rejects.toThrow("presidential cycle id is required");

    await expect(
      enrichPresidentialNomineeForCycle({
        db: { query: vi.fn() } as never,
        cycleId: CYCLE_ID,
        electionYear: 2026,
        party: "Democratic",
        loadCandidatesForResolution,
        enrichNominee,
      })
    ).rejects.toThrow("Invalid presidential nominee election year: 2026");

    expect(loadCandidatesForResolution).not.toHaveBeenCalled();
    expect(enrichNominee).not.toHaveBeenCalled();
  });
});
