import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const cycleId = "11111111-1111-4111-8111-111111111111";

describe("runPresidentialNomineeResearchProducer", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("does nothing when disabled and not forced", async () => {
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runPresidentialNomineeResearchProducer } = await import(
      "../../../src/pipeline/producers/presidentialNomineeResearchProducer.js"
    );

    const result = await runPresidentialNomineeResearchProducer({
      now: new Date("2028-02-07T00:00:00.000Z"),
    });

    expect(result).toMatchObject({
      enabled: false,
      cyclesScanned: 0,
      dueCycleCount: 0,
      selectedCycleCount: 0,
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("selects due active primary cycles in dry-run mode", async () => {
    process.env.PRESIDENTIAL_NOMINEE_RESEARCH_ENABLED = "true";
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          cycle_id: cycleId,
          election_year: 2028,
          stage: "primary",
          party: "Democratic",
          status: "active",
          nominee_research_last_attempted_at: null,
          nominee_research_next_at: null,
        },
        {
          cycle_id: "22222222-2222-4222-8222-222222222222",
          election_year: 2028,
          stage: "primary",
          party: "Republican",
          status: "completed",
          nominee_research_last_attempted_at: null,
          nominee_research_next_at: null,
        },
      ],
    });
    const end = vi.fn().mockResolvedValue(undefined);
    vi.doMock("pg", () => ({ Pool: vi.fn(() => ({ query, end })) }));

    const { runPresidentialNomineeResearchProducer } = await import(
      "../../../src/pipeline/producers/presidentialNomineeResearchProducer.js"
    );

    const result = await runPresidentialNomineeResearchProducer({
      dryRun: true,
      now: new Date("2028-02-07T00:00:00.000Z"),
      maxCyclesPerRun: 10,
    });

    expect(result).toMatchObject({
      enabled: true,
      dryRun: true,
      cyclesScanned: 2,
      dueCycleCount: 1,
      selectedCycleCount: 1,
      enqueuedJobCount: 0,
    });
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.presidential_cycles");
    expect(sql).toContain("EXISTS");
    expect(sql).toContain("general_cycle.stage = 'general'");
    expect(sql).toContain("general_cycle.party IS NULL");
    expect(query.mock.calls[0]?.[1]).toEqual([["Democratic", "Republican"]]);
    expect(end).toHaveBeenCalled();
  });
});
