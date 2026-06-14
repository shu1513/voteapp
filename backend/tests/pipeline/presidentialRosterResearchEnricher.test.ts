import { describe, expect, it, vi } from "vitest";

import {
  processPresidentialRosterResearchJob,
  type PresidentialRosterResearchJobResult,
} from "../../src/pipeline/enrichers/presidentialRosterResearchEnricher.js";
import type { PresidentialRosterEnricherResult } from "../../src/pipeline/enrichers/presidentialRosterEnricher.js";

const cycleId = "11111111-1111-4111-8111-111111111111";

function job() {
  return {
    cycle_id: cycleId,
    election_year: 2028,
    stage: "primary" as const,
    party: "Democratic",
    scheduled_for: "2027-03-07T00:00:00.000Z",
    run_id: "test-run",
  };
}

function successfulRosterResult(): Extract<PresidentialRosterEnricherResult, { ok: true }> {
  return {
    ok: true,
    cycleId,
    electionYear: 2028,
    stage: "primary",
    party: "Democratic",
    provider: "claude",
    model: "claude-opus-4-8",
    aiCandidateCount: 1,
    matchedCount: 1,
    ambiguousCount: 0,
    unmatchedCount: 0,
    withdrawnSkippedCount: 0,
    withdrawnDemotedCount: 0,
    emittedCount: 1,
    skippedCount: 0,
    dryRun: false,
    admissionPolicy: "fec_confirmed_only",
    statusVerification: {
      checkedCount: 0,
      withdrawnCount: 0,
      activeCount: 0,
      skippedCount: 0,
      demotedCount: 0,
      dryRun: false,
    },
    matches: [],
    aiRawDebug: null,
  };
}

function failedRosterResult(): Extract<PresidentialRosterEnricherResult, { ok: false }> {
  return {
    ok: false,
    cycleId,
    electionYear: 2028,
    stage: "primary",
    party: "Democratic",
    error: "AI roster failed",
    retryable: true,
    errorCode: "AI_ERROR",
  };
}

describe("processPresidentialRosterResearchJob", () => {
  it("runs the roster enricher and records successful schedule tracking", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });
    const enrichRosterCycle = vi.fn().mockResolvedValue(successfulRosterResult());

    const result = await processPresidentialRosterResearchJob(job(), {
      pool: { query } as never,
      redis: { sendCommand: vi.fn() },
      researchedAt: new Date("2027-03-07T12:00:00.000Z"),
      enrichRosterCycle,
    });

    expect(enrichRosterCycle).toHaveBeenCalledWith(
      expect.objectContaining({
        electionYear: 2028,
        stage: "primary",
        party: "Democratic",
        runId: "test-run",
      })
    );
    expect(query).toHaveBeenCalledWith(expect.stringContaining("roster_research_last_status = 'succeeded'"), [
      cycleId,
      "2027-03-07T12:00:00.000Z",
      "2027-03-14T12:00:00.000Z",
    ]);
    expect(result).toMatchObject<Partial<PresidentialRosterResearchJobResult>>({
      ok: true,
      rows_updated: 1,
      next_research_at: "2027-03-14T12:00:00.000Z",
      provider: "claude",
      model: "claude-opus-4-8",
      emitted_count: 1,
    });
  });

  it("records non-throwing roster failures and completes the job with an error result", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    const result = await processPresidentialRosterResearchJob(job(), {
      pool: { query } as never,
      redis: { sendCommand: vi.fn() },
      researchedAt: new Date("2027-07-07T12:00:00.000Z"),
      enrichRosterCycle: vi.fn().mockResolvedValue(failedRosterResult()),
    });

    expect(query).toHaveBeenCalledWith(expect.stringContaining("roster_research_last_status = 'failed'"), [
      cycleId,
      "2027-07-07T12:00:00.000Z",
      "2027-07-10T12:00:00.000Z",
      "AI roster failed",
    ]);
    expect(result).toMatchObject<Partial<PresidentialRosterResearchJobResult>>({
      ok: false,
      rows_updated: 1,
      next_research_at: "2027-07-10T12:00:00.000Z",
      error: "AI roster failed",
      error_code: "AI_ERROR",
    });
  });

  it("closes a locally-created pool when Redis connection fails", async () => {
    vi.resetModules();
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });
    const end = vi.fn().mockResolvedValue(undefined);
    const connect = vi.fn().mockRejectedValue(new Error("redis down"));
    const enrichRosterCycle = vi.fn();

    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({ query, end })),
    }));
    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        on: vi.fn(),
        connect,
        quit: vi.fn(),
      })),
    }));

    const { processPresidentialRosterResearchJob: processJob } = await import(
      "../../src/pipeline/enrichers/presidentialRosterResearchEnricher.js"
    );

    await expect(
      processJob(job(), {
        researchedAt: new Date("2027-03-07T12:00:00.000Z"),
        enrichRosterCycle,
      })
    ).rejects.toThrow("redis down");

    expect(enrichRosterCycle).not.toHaveBeenCalled();
    expect(query).toHaveBeenCalledWith(expect.stringContaining("roster_research_last_status = 'failed'"), [
      cycleId,
      "2027-03-07T12:00:00.000Z",
      "2027-03-14T12:00:00.000Z",
      "redis down",
    ]);
    expect(end).toHaveBeenCalledTimes(1);

    vi.doUnmock("pg");
    vi.doUnmock("redis");
    vi.resetModules();
  });

  it("rejects invalid job payloads before calling the roster enricher", async () => {
    const enrichRosterCycle = vi.fn();

    await expect(
      processPresidentialRosterResearchJob(
        {
          ...job(),
          stage: "general" as "primary",
        },
        {
          pool: { query: vi.fn() } as never,
          redis: { sendCommand: vi.fn() },
          enrichRosterCycle,
        }
      )
    ).rejects.toThrow("Unsupported presidential roster research job stage");

    expect(enrichRosterCycle).not.toHaveBeenCalled();
  });
});
