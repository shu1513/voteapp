import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});
const poolConnectMock = vi.fn();
const clientQueryMock = vi.fn();
const clientReleaseMock = vi.fn();

const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});
const redisXGroupCreateMock = vi.fn(async () => "OK");
const redisXAutoClaimMock = vi.fn(async () => ({ nextId: "0-0", messages: [] }));
const redisXReadGroupMock = vi.fn();
const redisXAckMock = vi.fn(async () => 1);
const redisXAddMock = vi.fn(async () => "1-0");
const redisSendCommandMock = vi.fn(async () => 1);

const evaluateEligibilityMock = vi.fn();
const enqueueCandidateRosterDraftsMock = vi.fn(async () => ({ emittedCount: 1, skippedCount: 0 }));

describe("runElectionsWriter candidate roster eligibility gate", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.CANDIDATE_ROSTER_ENABLE_WRITER_ELIGIBILITY_FILTER = "true";

    poolConnectMock.mockResolvedValue({
      query: clientQueryMock,
      release: clientReleaseMock,
    });

    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:validated",
        messages: [
          {
            id: "1-0",
            message: {
              ingest_key: "elections:test:gated",
              item_type: "election",
            },
          },
        ],
      },
    ]);
  });

  it("enqueues only eligible office election ids when writer gate is enabled", async () => {
    vi.doMock("pg", () => {
      return {
        Pool: vi.fn(() => ({
          query: poolQueryMock,
          connect: poolConnectMock,
          end: poolEndMock,
        })),
      };
    });

    vi.doMock("redis", () => {
      return {
        createClient: vi.fn(() => ({
          connect: redisConnectMock,
          quit: redisQuitMock,
          xGroupCreate: redisXGroupCreateMock,
          xAutoClaim: redisXAutoClaimMock,
          xReadGroup: redisXReadGroupMock,
          xAck: redisXAckMock,
          xAdd: redisXAddMock,
          sendCommand: redisSendCommandMock,
        })),
      };
    });

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
      evaluateOfficeCandidateEligibilityByElectionIds: evaluateEligibilityMock,
      summarizeOfficeCandidateEligibilityReasons: () => ({
        eligible: 1,
        not_office_or_missing: 0,
        not_upcoming: 0,
        already_written: 0,
        not_nearest_in_track: 1,
        buffer_not_elapsed: 0,
        too_far_in_future: 0,
      }),
    }));

    vi.doMock("../../src/pipeline/candidates/candidateRosterDraftEmitter.js", () => ({
      enqueueCandidateRosterDrafts: enqueueCandidateRosterDraftsMock,
    }));

    const { runElectionsWriter } = await import("../../src/pipeline/writers/electionsWriter.js");

    const payload = {
      district_id: "d-1",
      district_name: "Sample District",
      district_type: "county",
      state: "CA",
      entries: [
        {
          official_ballot_title: "County Treasurer",
          election_date: "2026-06-02",
          race_type: "office",
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/1"],
        },
        {
          official_ballot_title: "County Treasurer",
          election_date: "2026-11-03",
          race_type: "office",
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/2"],
        },
      ],
    };

    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:gated",
          payload,
          status: "validated",
          run_id: "run_1",
          ai_raw_debug: null,
        },
      ],
    });

    let upsertCounter = 0;
    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: "00000000-0000-0000-0000-000000000100",
              canonical_name: "County Treasurer",
            },
          ],
        };
      }
      if (sql.includes("UPDATE staging_items") && sql.includes("status = $3")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE public.districts")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        upsertCounter += 1;
        return {
          rowCount: 1,
          rows: [{ id: `00000000-0000-0000-0000-00000000010${upsertCounter}`, race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    evaluateEligibilityMock.mockResolvedValue([
      {
        election_id: "00000000-0000-0000-0000-000000000101",
        reason: "eligible",
        prior_election_date: null,
        stage_gap_days: null,
        buffer_days: 7,
        eligible_after_date: null,
      },
      {
        election_id: "00000000-0000-0000-0000-000000000102",
        reason: "not_nearest_in_track",
        prior_election_date: "2026-06-02",
        stage_gap_days: 154,
        buffer_days: 7,
        eligible_after_date: "2026-06-09",
      },
    ]);

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    expect(evaluateEligibilityMock).toHaveBeenCalledTimes(1);
    expect(enqueueCandidateRosterDraftsMock).toHaveBeenCalledWith(
      expect.anything(),
      ["00000000-0000-0000-0000-000000000101"],
      "run_1"
    );
  });

  it("bypasses eligibility selector and enqueues all office ids when writer gate is disabled", async () => {
    process.env.CANDIDATE_ROSTER_ENABLE_WRITER_ELIGIBILITY_FILTER = "false";

    vi.doMock("pg", () => {
      return {
        Pool: vi.fn(() => ({
          query: poolQueryMock,
          connect: poolConnectMock,
          end: poolEndMock,
        })),
      };
    });

    vi.doMock("redis", () => {
      return {
        createClient: vi.fn(() => ({
          connect: redisConnectMock,
          quit: redisQuitMock,
          xGroupCreate: redisXGroupCreateMock,
          xAutoClaim: redisXAutoClaimMock,
          xReadGroup: redisXReadGroupMock,
          xAck: redisXAckMock,
          xAdd: redisXAddMock,
          sendCommand: redisSendCommandMock,
        })),
      };
    });

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
      evaluateOfficeCandidateEligibilityByElectionIds: evaluateEligibilityMock,
      summarizeOfficeCandidateEligibilityReasons: () => ({
        eligible: 0,
        not_office_or_missing: 0,
        not_upcoming: 0,
        already_written: 0,
        not_nearest_in_track: 0,
        buffer_not_elapsed: 0,
        too_far_in_future: 0,
      }),
    }));

    vi.doMock("../../src/pipeline/candidates/candidateRosterDraftEmitter.js", () => ({
      enqueueCandidateRosterDrafts: enqueueCandidateRosterDraftsMock,
    }));

    const { runElectionsWriter } = await import("../../src/pipeline/writers/electionsWriter.js");

    const payload = {
      district_id: "d-1",
      district_name: "Sample District",
      district_type: "county",
      state: "CA",
      entries: [
        {
          official_ballot_title: "County Treasurer",
          election_date: "2026-06-02",
          race_type: "office",
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/1"],
        },
        {
          official_ballot_title: "County Treasurer",
          election_date: "2026-11-03",
          race_type: "office",
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/2"],
        },
      ],
    };

    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:gated",
          payload,
          status: "validated",
          run_id: "run_1",
          ai_raw_debug: null,
        },
      ],
    });

    let upsertCounter = 0;
    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: "00000000-0000-0000-0000-000000000100",
              canonical_name: "County Treasurer",
            },
          ],
        };
      }
      if (sql.includes("UPDATE staging_items") && sql.includes("status = $3")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE public.districts")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        upsertCounter += 1;
        return {
          rowCount: 1,
          rows: [{ id: `00000000-0000-0000-0000-00000000010${upsertCounter}`, race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    expect(evaluateEligibilityMock).not.toHaveBeenCalled();
    expect(enqueueCandidateRosterDraftsMock).toHaveBeenCalledWith(
      expect.anything(),
      [
        "00000000-0000-0000-0000-000000000101",
        "00000000-0000-0000-0000-000000000102",
      ],
      "run_1"
    );
  });
});
