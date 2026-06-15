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
    process.env.PRESIDENTIAL_NOMINEE_RESEARCH_ENABLED = "false";
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
      cyclesScanned: 1,
      dueCycleCount: 1,
      selectedCycleCount: 1,
      enqueuedJobCount: 0,
    });
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.presidential_cycles");
    expect(sql).toContain("status = 'active'");
    expect(sql).toContain("EXISTS");
    expect(sql).toContain("general_cycle.stage = 'general'");
    expect(sql).toContain("general_cycle.party IS NULL");
    expect(query.mock.calls[0]?.[1]).toEqual([["Democratic", "Republican"]]);
    expect(end).toHaveBeenCalled();
  });

  it("skips an existing job that becomes active before removal", async () => {
    process.env.PRESIDENTIAL_NOMINEE_RESEARCH_ENABLED = "true";
    process.env.DATABASE_URL = "postgres://example";
    process.env.REDIS_URL = "redis://localhost:6379/0";
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
      ],
    });
    const end = vi.fn().mockResolvedValue(undefined);
    const close = vi.fn().mockResolvedValue(undefined);
    const add = vi.fn();
    const existingJob = {
      getState: vi.fn().mockResolvedValueOnce("waiting").mockResolvedValueOnce("active"),
      remove: vi.fn().mockRejectedValue(new Error("job is locked")),
    };
    vi.doMock("pg", () => ({ Pool: vi.fn(() => ({ query, end })) }));
    vi.doMock("bullmq", () => ({
      Queue: vi.fn(() => ({
        add,
        close,
        getJob: vi.fn().mockResolvedValue(existingJob),
      })),
    }));

    const { runPresidentialNomineeResearchProducer } = await import(
      "../../../src/pipeline/producers/presidentialNomineeResearchProducer.js"
    );

    const result = await runPresidentialNomineeResearchProducer({
      now: new Date("2028-02-07T00:00:00.000Z"),
      maxCyclesPerRun: 10,
    });

    expect(result).toMatchObject({
      skippedActiveJobCount: 1,
      enqueuedJobCount: 0,
      updatedJobCount: 0,
    });
    expect(add).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalled();
    expect(end).toHaveBeenCalled();
  });

  it("closes the pool when queue creation fails", async () => {
    process.env.PRESIDENTIAL_NOMINEE_RESEARCH_ENABLED = "true";
    process.env.DATABASE_URL = "postgres://example";
    process.env.REDIS_URL = "redis://localhost:6379/0";
    const query = vi.fn();
    const end = vi.fn().mockResolvedValue(undefined);
    vi.doMock("pg", () => ({ Pool: vi.fn(() => ({ query, end })) }));
    vi.doMock("bullmq", () => ({
      Queue: vi.fn(() => {
        throw new Error("queue init failed");
      }),
    }));

    const { runPresidentialNomineeResearchProducer } = await import(
      "../../../src/pipeline/producers/presidentialNomineeResearchProducer.js"
    );

    await expect(
      runPresidentialNomineeResearchProducer({
        now: new Date("2028-02-07T00:00:00.000Z"),
        maxCyclesPerRun: 10,
      })
    ).rejects.toThrow("queue init failed");

    expect(query).not.toHaveBeenCalled();
    expect(end).toHaveBeenCalledTimes(1);
  });
});
