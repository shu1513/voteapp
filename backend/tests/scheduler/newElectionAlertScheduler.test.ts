import { describe, expect, it, vi } from "vitest";

const baseSendResult = {
  dryRun: false,
  resolvedWithoutEmailCount: 1,
  eligibleUserCount: 2,
  eventsPendingCount: 5,
  usersEmailedCount: 2,
  eventsDeliveredCount: 5,
  failures: [],
};

function mockAlertModule(sendMock: ReturnType<typeof vi.fn>, opts: { lockBusy?: boolean } = {}) {
  vi.doMock("../../src/scripts/sendNewElectionAlerts.js", () => ({
    DEFAULT_ALERT_MAX_USERS: 500,
    DEFAULT_ALERT_MAX_ITEMS_PER_EMAIL: 20,
    buildAlertMailerFromEnv: vi.fn(() => ({ sendAlertEmail: vi.fn() })),
    sendNewElectionAlerts: sendMock,
    withNewElectionAlertRunLock: vi.fn(async (_pool: unknown, fn: () => Promise<unknown>) =>
      opts.lockBusy ? null : fn()
    ),
  }));
  vi.doMock("../../src/scripts/sendCandidateFollowDigests.js", () => ({
    buildUnsubscribeUrlBuilderFromEnv: vi.fn(() => null),
  }));
}

const fakePushClient = { fake: "push-client" };

function mockPushModule() {
  vi.doMock("../../src/pipeline/users/pushNotificationSender.js", () => ({
    buildPushClientFromEnv: vi.fn(() => fakePushClient),
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

describe("runNewElectionAlertJob", () => {
  it("runs a live send with default caps and preserves triggeredBy", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => ({ ...baseSendResult }));
    const endMock = vi.fn(async () => {});
    mockAlertModule(sendMock);
    mockPipelineEnv();
    mockPushModule();
    mockPg(endMock);

    const { runNewElectionAlertJob } = await import("../../src/scheduler/newElectionAlertScheduler.js");

    const result = await runNewElectionAlertJob({ triggeredBy: "daily" });

    expect(result.triggeredBy).toBe("daily");
    expect(result.usersEmailedCount).toBe(2);
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(sendMock.mock.calls[0][2]).toEqual({
      live: true,
      maxUsers: 500,
      maxItemsPerEmail: 20,
      pushClient: fakePushClient,
    });
    expect(endMock).toHaveBeenCalledTimes(1);
  });

  it("passes job-data caps through and closes the pool on send failure", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => {
      throw new Error("db unavailable");
    });
    const endMock = vi.fn(async () => {});
    mockAlertModule(sendMock);
    mockPipelineEnv();
    mockPushModule();
    mockPg(endMock);

    const { runNewElectionAlertJob } = await import("../../src/scheduler/newElectionAlertScheduler.js");

    await expect(
      runNewElectionAlertJob({ triggeredBy: "manual", maxUsers: 10, maxItemsPerEmail: 5 })
    ).rejects.toThrow("db unavailable");
    expect(sendMock.mock.calls[0][2]).toEqual({
      live: true,
      maxUsers: 10,
      maxItemsPerEmail: 5,
      pushClient: fakePushClient,
    });
    expect(endMock).toHaveBeenCalledTimes(1);
  });

  it("returns a lockSkipped result when another live run holds the advisory lock", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => ({ ...baseSendResult }));
    const endMock = vi.fn(async () => {});
    mockAlertModule(sendMock, { lockBusy: true });
    mockPipelineEnv();
    mockPushModule();
    mockPg(endMock);

    const { runNewElectionAlertJob } = await import("../../src/scheduler/newElectionAlertScheduler.js");

    const result = await runNewElectionAlertJob({ triggeredBy: "daily" });

    expect(result.lockSkipped).toBe(true);
    expect(result.usersEmailedCount).toBe(0);
    expect(sendMock).not.toHaveBeenCalled();
    expect(endMock).toHaveBeenCalledTimes(1);
  });
});
