import { beforeEach, describe, expect, it, vi } from "vitest";

const poolEndMock = vi.fn(async () => {});
const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});

const listEligibilityMock = vi.fn();
const enqueueDraftsMock = vi.fn();

describe("runCandidateRosterRolloverProducer", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.CANDIDATE_ROSTER_ENABLE_DAILY_ROLLOVER_PRODUCER = "true";
  });

  it("emits only eligible rows and reports gating counters", async () => {
    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        end: poolEndMock,
      })),
    }));

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        quit: redisQuitMock,
      })),
    }));

    vi.doMock("../../src/config/env.js", () => ({
      getPipelineEnv: () => ({
        DATABASE_URL: "postgresql://localhost:5432/test",
        REDIS_URL: "redis://localhost:6379/0",
        AI_PROVIDER: "openai",
        AI_MODEL: "gpt-5.4-mini",
        AI_TIMEOUT_MS: 90000,
        ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
        STATE_RESOURCES_PROMPT_VERSION: "state_resources_v2",
        CENSUS_API_KEYS: [],
      }),
    }));

    vi.doMock("../../src/pipeline/candidates/officeCandidateEligibility.js", () => ({
      defaultOfficeCandidateEligibilityConfig: () => ({
        asOfDate: "2026-05-23",
        defaultBufferDays: 7,
        shortStageGapDays: 60,
        shortStageBufferDays: 3,
        statewideUsHouseLookaheadDays: 90,
        localOfficeLookaheadDays: 75,
      }),
      listOfficeCandidateEligibilityForUpcomingOffices: listEligibilityMock,
      summarizeOfficeCandidateEligibilityReasons: () => ({
        eligible: 2,
        not_office_or_missing: 0,
        not_upcoming: 0,
        already_written: 1,
        not_nearest_in_track: 3,
        buffer_not_elapsed: 4,
        too_far_in_future: 5,
      }),
    }));

    vi.doMock("../../src/pipeline/candidates/candidateRosterDraftEmitter.js", () => ({
      enqueueCandidateRosterDrafts: enqueueDraftsMock,
    }));

    const { runCandidateRosterRolloverProducer } = await import(
      "../../src/pipeline/producers/candidateRosterRolloverProducer.js"
    );

    listEligibilityMock.mockResolvedValue([
      {
        election_id: "e-1",
        reason: "eligible",
        prior_election_date: null,
        stage_gap_days: null,
        buffer_days: 7,
        eligible_after_date: null,
      },
      {
        election_id: "e-2",
        reason: "eligible",
        prior_election_date: "2026-09-15",
        stage_gap_days: 49,
        buffer_days: 3,
        eligible_after_date: "2026-09-18",
      },
      {
        election_id: "e-3",
        reason: "buffer_not_elapsed",
        prior_election_date: "2026-06-02",
        stage_gap_days: 154,
        buffer_days: 7,
        eligible_after_date: "2026-06-09",
      },
    ]);
    enqueueDraftsMock.mockResolvedValue({ emittedCount: 1, skippedCount: 1 });

    const result = await runCandidateRosterRolloverProducer();

    expect(enqueueDraftsMock).toHaveBeenCalledWith(expect.anything(), ["e-1", "e-2"], expect.any(String));
    expect(result).toMatchObject({
      enabled: true,
      asOfDate: "2026-05-23",
      eligibleRows: 2,
      emittedRows: 1,
      markerSkippedRows: 1,
      alreadyWrittenRows: 1,
      bufferBlockedRows: 4,
      notNearestRows: 3,
      tooFarFutureRows: 5,
    });
  });

  it("returns disabled result without touching DB/Redis when feature flag is off", async () => {
    process.env.CANDIDATE_ROSTER_ENABLE_DAILY_ROLLOVER_PRODUCER = "false";

    const { runCandidateRosterRolloverProducer } = await import(
      "../../src/pipeline/producers/candidateRosterRolloverProducer.js"
    );

    const result = await runCandidateRosterRolloverProducer();
    expect(result.enabled).toBe(false);
    expect(result.eligibleRows).toBe(0);
    expect(result.emittedRows).toBe(0);
    expect(result.markerSkippedRows).toBe(0);

    expect(listEligibilityMock).not.toHaveBeenCalled();
    expect(enqueueDraftsMock).not.toHaveBeenCalled();
  });
});
