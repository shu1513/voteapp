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

function mockDigestModule(sendMock: ReturnType<typeof vi.fn>, opts: { lockBusy?: boolean } = {}) {
  vi.doMock("../../src/scripts/sendCandidateFollowDigests.js", () => ({
    DEFAULT_DIGEST_MAX_USERS: 500,
    DEFAULT_DIGEST_MAX_ITEMS_PER_EMAIL: 20,
    buildDigestMailerFromEnv: vi.fn(() => ({ sendDigestEmail: vi.fn() })),
    buildUnsubscribeUrlBuilderFromEnv: vi.fn(() => null),
    sendCandidateFollowDigests: sendMock,
    withDigestRunLock: vi.fn(async (_pool: unknown, fn: () => Promise<unknown>) =>
      opts.lockBusy ? null : fn()
    ),
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

describe("runCandidateFollowDigestJob", () => {
  it("runs a live send with default caps and preserves triggeredBy", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => ({ ...baseSendResult }));
    const endMock = vi.fn(async () => {});
    mockDigestModule(sendMock);
    mockPipelineEnv();
    mockPg(endMock);

    const { runCandidateFollowDigestJob } = await import(
      "../../src/scheduler/candidateFollowDigestScheduler.js"
    );

    const result = await runCandidateFollowDigestJob({ triggeredBy: "daily" });

    expect(result.triggeredBy).toBe("daily");
    expect(result.usersEmailedCount).toBe(2);
    expect(result.eventsDeliveredCount).toBe(5);
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
    mockDigestModule(sendMock);
    mockPipelineEnv();
    mockPg(endMock);

    const { runCandidateFollowDigestJob } = await import(
      "../../src/scheduler/candidateFollowDigestScheduler.js"
    );

    await expect(
      runCandidateFollowDigestJob({ triggeredBy: "manual", maxUsers: 10, maxItemsPerEmail: 5 })
    ).rejects.toThrow("db unavailable");
    expect(sendMock.mock.calls[0][2]).toEqual({ live: true, maxUsers: 10, maxItemsPerEmail: 5 });
    expect(endMock).toHaveBeenCalledTimes(1);
  });

  it("keeps per-user failures in the result instead of failing the job", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => ({
      ...baseSendResult,
      usersEmailedCount: 1,
      failures: [{ userId: "u2", stage: "send", reason: "SES exploded" }],
    }));
    const endMock = vi.fn(async () => {});
    mockDigestModule(sendMock);
    mockPipelineEnv();
    mockPg(endMock);

    const { runCandidateFollowDigestJob } = await import(
      "../../src/scheduler/candidateFollowDigestScheduler.js"
    );

    const result = await runCandidateFollowDigestJob({ triggeredBy: "daily" });

    expect(result.failures).toEqual([{ userId: "u2", stage: "send", reason: "SES exploded" }]);
    expect(result.usersEmailedCount).toBe(1);
  });

  it("returns a lockSkipped result when another live run holds the advisory lock", async () => {
    vi.resetModules();
    const sendMock = vi.fn(async () => ({ ...baseSendResult }));
    const endMock = vi.fn(async () => {});
    mockDigestModule(sendMock, { lockBusy: true });
    mockPipelineEnv();
    mockPg(endMock);

    const { runCandidateFollowDigestJob } = await import(
      "../../src/scheduler/candidateFollowDigestScheduler.js"
    );

    const result = await runCandidateFollowDigestJob({ triggeredBy: "daily" });

    expect(result.lockSkipped).toBe(true);
    expect(result.usersEmailedCount).toBe(0);
    expect(sendMock).not.toHaveBeenCalled();
    expect(endMock).toHaveBeenCalledTimes(1);
  });
});
