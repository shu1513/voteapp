import { describe, expect, it, vi } from "vitest";

const baseSendResult = {
  dryRun: false,
  targetElectionDate: "2026-11-03",
  eligibleUserCount: 2,
  electionsPendingCount: 5,
  usersEmailedCount: 2,
  usersMarkedCount: 2,
  failures: [],
};

function mockReminderModule(sendMock: ReturnType<typeof vi.fn>, opts: { lockBusy?: boolean } = {}) {
  vi.doMock("../../src/scripts/sendElectionReminders.js", () => ({
    DEFAULT_REMINDER_MAX_USERS: 500,
    DEFAULT_REMINDER_MAX_ITEMS_PER_EMAIL: 20,
    buildReminderMailerFromEnv: vi.fn(() => ({ sendReminderEmail: vi.fn() })),
    sendElectionReminders: sendMock,
    withElectionReminderRunLock: vi.fn(async (_pool: unknown, fn: () => Promise<unknown>) =>
      opts.lockBusy ? null : fn()
    ),
  }));
  vi.doMock("../../src/scripts/sendCandidateFollowDigests.js", () => ({
    buildUnsubscribeUrlBuilderFromEnv: vi.fn(() => null),
  }));
}

function mockPipelineEnv() {
  vi.doMock("../../src/config/env.js", () => ({
    getPipelineEnv: () => ({
      DATABASE_URL: "postgresql://localhost:5432/test",
      REDIS_URL: "redis://localhost:6379/0",
    }),
  }));
}

function mockPg(endMock: ReturnType<typeof vi.fn>) {
  vi.doMock("pg", () => ({
    Pool: vi.fn(() => ({ query: vi.fn(), end: endMock })),
  }));
}

describe("runElectionReminderJob", () => {
  it("runs a live send with default caps and preserves triggeredBy", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => ({ ...baseSendResult }));
    const endMock = vi.fn(async () => {});
    mockReminderModule(sendMock);
    mockPipelineEnv();
    mockPg(endMock);

    const { runElectionReminderJob } = await import("../../src/scheduler/electionReminderScheduler.js");

    const result = await runElectionReminderJob({ triggeredBy: "daily" });

    expect(result.triggeredBy).toBe("daily");
    expect(result.usersEmailedCount).toBe(2);
    expect(result.targetElectionDate).toBe("2026-11-03");
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(sendMock.mock.calls[0][2]).toEqual({
      live: true,
      maxUsers: 500,
      maxItemsPerEmail: 20,
    });
    expect(endMock).toHaveBeenCalledTimes(1);
  });

  it("passes job-data caps through and closes the pool on send failure", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => {
      throw new Error("db unavailable");
    });
    const endMock = vi.fn(async () => {});
    mockReminderModule(sendMock);
    mockPipelineEnv();
    mockPg(endMock);

    const { runElectionReminderJob } = await import("../../src/scheduler/electionReminderScheduler.js");

    await expect(
      runElectionReminderJob({ triggeredBy: "manual", maxUsers: 10, maxItemsPerEmail: 5 })
    ).rejects.toThrow("db unavailable");
    expect(sendMock.mock.calls[0][2]).toEqual({ live: true, maxUsers: 10, maxItemsPerEmail: 5 });
    expect(endMock).toHaveBeenCalledTimes(1);
  });

  it("returns a lockSkipped result when another live run holds the advisory lock", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => ({ ...baseSendResult }));
    const endMock = vi.fn(async () => {});
    mockReminderModule(sendMock, { lockBusy: true });
    mockPipelineEnv();
    mockPg(endMock);

    const { runElectionReminderJob } = await import("../../src/scheduler/electionReminderScheduler.js");

    const result = await runElectionReminderJob({ triggeredBy: "daily" });

    expect(result.lockSkipped).toBe(true);
    expect(result.usersEmailedCount).toBe(0);
    expect(sendMock).not.toHaveBeenCalled();
    expect(endMock).toHaveBeenCalledTimes(1);
  });
});
