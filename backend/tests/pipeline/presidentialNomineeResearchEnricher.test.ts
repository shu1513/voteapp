import { describe, expect, it, vi } from "vitest";

import {
  processPresidentialNomineeResearchJob,
  type PresidentialNomineeResearchJobData,
} from "../../src/pipeline/enrichers/presidentialNomineeResearchEnricher.js";
import type { PresidentialNomineeEnricherResult } from "../../src/pipeline/enrichers/presidentialNomineeEnricher.js";
import { PromotePresidentialNomineeError } from "../../src/pipeline/presidential/presidentialNomineePromotion.js";

const CYCLE_ID = "11111111-1111-4111-8111-111111111111";
const CANDIDATE_ID = "22222222-2222-4222-8222-222222222222";
const GENERAL_CYCLE_ID = "33333333-3333-4333-8333-333333333333";

function job(overrides: Partial<PresidentialNomineeResearchJobData> = {}): PresidentialNomineeResearchJobData {
  return {
    cycle_id: CYCLE_ID,
    election_year: 2028,
    party: "Democratic",
    scheduled_for: "2028-02-01T00:00:00.000Z",
    run_id: "test-run",
    ...overrides,
  };
}

function matchedNomineeResult(): Extract<PresidentialNomineeEnricherResult, { ok: true }> {
  return {
    ok: true,
    cycleId: CYCLE_ID,
    electionYear: 2028,
    party: "Democratic",
    provider: "claude",
    model: "claude-opus-4-8",
    candidateCount: 2,
    resolution: {
      status: "matched",
      candidateId: CANDIDATE_ID,
      displayName: "Jane President",
      method: "exact_fec_id",
      candidateName: "Jane President",
      fecCandidateId: "P80000001",
      sources: ["https://example.org/nominee"],
    },
    aiRawDebug: null,
  };
}

describe("processPresidentialNomineeResearchJob", () => {
  it("runs nominee research and promotes a matched nominee", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const enrichNomineeForCycle = vi.fn().mockResolvedValue(matchedNomineeResult());
    const promoteNominee = vi.fn().mockResolvedValue({
      status: "promoted",
      primaryCycleId: CYCLE_ID,
      generalCycleId: GENERAL_CYCLE_ID,
      nomineeCandidateId: CANDIDATE_ID,
      party: "Democratic",
      sources: ["https://example.org/nominee"],
    });

    const result = await processPresidentialNomineeResearchJob(job({ party: " Democratic " }), {
      pool: { query } as never,
      researchedAt: new Date("2028-03-15T12:00:00.000Z"),
      enrichNomineeForCycle,
      promoteNominee,
    });

    expect(enrichNomineeForCycle).toHaveBeenCalledWith(
      expect.objectContaining({
        cycleId: CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
      })
    );
    expect(promoteNominee).toHaveBeenCalledWith({
      db: expect.any(Object),
      primaryCycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      resolution: matchedNomineeResult().resolution,
      confirmedAt: new Date("2028-03-15T12:00:00.000Z"),
    });
    expect(result).toEqual({
      cycle_id: CYCLE_ID,
      election_year: 2028,
      party: "Democratic",
      ok: true,
      provider: "claude",
      model: "claude-opus-4-8",
      candidate_count: 2,
      resolution_status: "matched",
      promotion_status: "promoted",
      nominee_research_rows_updated: 1,
      next_research_at: null,
      nominee_candidate_id: CANDIDATE_ID,
      general_cycle_id: GENERAL_CYCLE_ID,
    });
    expect(query).toHaveBeenCalledWith(expect.stringContaining("nominee_research_last_status = 'succeeded'"), [
      CYCLE_ID,
      "2028-03-15T12:00:00.000Z",
      null,
    ]);
  });

  it("treats no nominee yet as a successful non-promoting result", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const nomineeResult: Extract<PresidentialNomineeEnricherResult, { ok: true }> = {
      ok: true,
      cycleId: CYCLE_ID,
      electionYear: 2028,
      party: "Democratic",
      provider: "openai",
      model: "gpt-5.5",
      candidateCount: 2,
      resolution: {
        status: "no_nominee_found",
        sources: ["https://example.org/no-nominee"],
      },
      aiRawDebug: null,
    };
    const promoteNominee = vi.fn().mockResolvedValue({
      status: "skipped",
      reason: "no_matched_nominee",
      resolutionStatus: "no_nominee_found",
    });

    const result = await processPresidentialNomineeResearchJob(job(), {
      pool: { query } as never,
      researchedAt: new Date("2028-02-07T12:00:00.000Z"),
      enrichNomineeForCycle: vi.fn().mockResolvedValue(nomineeResult),
      promoteNominee,
    });

    expect(promoteNominee).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      ok: true,
      provider: "openai",
      model: "gpt-5.5",
      resolution_status: "no_nominee_found",
      promotion_status: "skipped",
      nominee_research_rows_updated: 1,
      next_research_at: "2028-02-09T12:00:00.000Z",
    });
    expect(result).not.toHaveProperty("nominee_candidate_id");
  });

  it("returns nonretryable nominee research failures without promoting", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const promoteNominee = vi.fn();

    const result = await processPresidentialNomineeResearchJob(job(), {
      pool: { query } as never,
      researchedAt: new Date("2028-02-07T12:00:00.000Z"),
      enrichNomineeForCycle: vi.fn().mockResolvedValue({
        ok: false,
        cycleId: CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        retryable: false,
        error: "No active presidential primary candidates are available for nominee research",
        errorCode: "NO_ACTIVE_CANDIDATES",
      }),
      promoteNominee,
    });

    expect(promoteNominee).not.toHaveBeenCalled();
    expect(result).toEqual({
      cycle_id: CYCLE_ID,
      election_year: 2028,
      party: "Democratic",
      ok: false,
      nominee_research_rows_updated: 1,
      next_research_at: "2028-02-09T12:00:00.000Z",
      error: "No active presidential primary candidates are available for nominee research",
      error_code: "NO_ACTIVE_CANDIDATES",
    });
    expect(query).toHaveBeenCalledWith(expect.stringContaining("nominee_research_last_status = 'failed'"), [
      CYCLE_ID,
      "2028-02-07T12:00:00.000Z",
      "2028-02-09T12:00:00.000Z",
      "No active presidential primary candidates are available for nominee research",
    ]);
  });

  it("throws retryable nominee research failures so BullMQ can retry", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    await expect(
      processPresidentialNomineeResearchJob(job(), {
        pool: { query } as never,
        researchedAt: new Date("2028-02-07T12:00:00.000Z"),
        enrichNomineeForCycle: vi.fn().mockResolvedValue({
          ok: false,
          cycleId: CYCLE_ID,
          electionYear: 2028,
          party: "Democratic",
          retryable: true,
          error: "temporary provider outage",
          errorCode: "TEMP_PROVIDER_ERROR",
        }),
        promoteNominee: vi.fn(),
      })
    ).rejects.toThrow("presidential nominee research failed: temporary provider outage");
    expect(query).toHaveBeenCalledWith(expect.stringContaining("nominee_research_last_status = 'failed'"), [
      CYCLE_ID,
      "2028-02-07T12:00:00.000Z",
      "2028-02-09T12:00:00.000Z",
      "temporary provider outage",
    ]);
  });

  it("returns promotion-domain errors without retrying the job", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const result = await processPresidentialNomineeResearchJob(job(), {
      pool: { query } as never,
      researchedAt: new Date("2028-02-07T12:00:00.000Z"),
      enrichNomineeForCycle: vi.fn().mockResolvedValue(matchedNomineeResult()),
      promoteNominee: vi.fn().mockRejectedValue(
        new PromotePresidentialNomineeError(
          "primary_cycle_already_has_different_nominee",
          "Presidential primary cycle already has a different nominee"
        )
      ),
    });

    expect(result).toMatchObject({
      ok: false,
      provider: "claude",
      model: "claude-opus-4-8",
      resolution_status: "matched",
      nominee_research_rows_updated: 1,
      next_research_at: "2028-02-09T12:00:00.000Z",
      error: "Presidential primary cycle already has a different nominee",
      error_code: "primary_cycle_already_has_different_nominee",
    });
  });

  it("rejects invalid jobs before running nominee research", async () => {
    const enrichNomineeForCycle = vi.fn();

    await expect(
      processPresidentialNomineeResearchJob(job({ cycle_id: "not-a-uuid" }), {
        pool: { query: vi.fn() } as never,
        enrichNomineeForCycle,
        promoteNominee: vi.fn(),
      })
    ).rejects.toThrow("Invalid presidential nominee research job cycle_id");

    await expect(
      processPresidentialNomineeResearchJob(job({ election_year: 2026 }), {
        pool: { query: vi.fn() } as never,
        enrichNomineeForCycle,
        promoteNominee: vi.fn(),
      })
    ).rejects.toThrow("Invalid presidential nominee research job election_year");

    expect(enrichNomineeForCycle).not.toHaveBeenCalled();
  });
});
