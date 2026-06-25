import { beforeEach, describe, expect, it, vi } from "vitest";

const poolEndMock = vi.hoisted(() => vi.fn());
const redisConnectMock = vi.hoisted(() => vi.fn());
const redisQuitMock = vi.hoisted(() => vi.fn());
const redisXGroupCreateMock = vi.hoisted(() => vi.fn());
const redisXAutoClaimMock = vi.hoisted(() => vi.fn());
const redisSendCommandMock = vi.hoisted(() => vi.fn());
const redisXReadGroupMock = vi.hoisted(() => vi.fn());
const redisXAckMock = vi.hoisted(() => vi.fn());
const redisSetMock = vi.hoisted(() => vi.fn());
const poolQueryMock = vi.hoisted(() => vi.fn());
const poolConnectMock = vi.hoisted(() => vi.fn());
const poolClientReleaseMock = vi.hoisted(() => vi.fn());
const enrichCandidateRecordsMock = vi.hoisted(() => vi.fn());
const enrichCandidateRecordAreasMock = vi.hoisted(() => vi.fn());
const runLifecycleMock = vi.hoisted(() => vi.fn());
const summarizeLifecycleMock = vi.hoisted(() => vi.fn());
const loadElectionContextMock = vi.hoisted(() => vi.fn());
const loadPresidentialContextMock = vi.hoisted(() => vi.fn());
const createCandidateRecordUpdateNotificationEventsMock = vi.hoisted(() => vi.fn());

vi.mock("pg", () => ({
  Pool: vi.fn(() => ({
    query: poolQueryMock,
    connect: poolConnectMock,
    end: poolEndMock,
  })),
}));

vi.mock("redis", () => ({
  createClient: vi.fn(() => ({
    connect: redisConnectMock,
    quit: redisQuitMock,
    xGroupCreate: redisXGroupCreateMock,
    xAutoClaim: redisXAutoClaimMock,
    sendCommand: redisSendCommandMock,
    xReadGroup: redisXReadGroupMock,
    xAck: redisXAckMock,
    set: redisSetMock,
  })),
}));

vi.mock("../../src/config/env.js", () => ({
  getPipelineEnv: () => ({
    DATABASE_URL: "postgres://test/test",
    REDIS_URL: "redis://localhost:6379/0",
    AI_TIMEOUT_MS: 1000,
    ANTHROPIC_WEB_SEARCH_MAX_USES: 1,
  }),
}));

vi.mock("../../src/ai/enrichCandidateRecords.js", () => ({
  buildCandidateRecordsConfigFromEnv: vi.fn(() => ({ timeoutMs: 1000 })),
  enrichCandidateRecords: enrichCandidateRecordsMock,
}));

vi.mock("../../src/ai/enrichCandidateRecordSourcesRepair.js", () => ({
  buildCandidateRecordSourcesRepairConfigFromEnv: vi.fn(() => ({ timeoutMs: 1000 })),
  enrichCandidateRecordSourcesRepair: vi.fn(),
}));

vi.mock("../../src/ai/enrichCandidateRecordAreas.js", () => ({
  buildCandidateRecordAreasConfigFromEnv: vi.fn(() => ({ timeoutMs: 1000 })),
  enrichCandidateRecordAreas: enrichCandidateRecordAreasMock,
}));

vi.mock("../../src/pipeline/candidates/candidateRecordOfficeContext.js", () => ({
  loadCandidateElectionOfficeContext: loadElectionContextMock,
  loadCandidatePresidentialCycleOfficeContext: loadPresidentialContextMock,
}));

vi.mock("../../src/pipeline/candidates/candidateRecordsSearchLifecycle.js", () => ({
  runCandidateRecordsSearchLifecycle: runLifecycleMock,
  summarizeCandidateRecordsLifecycleResults: summarizeLifecycleMock,
}));

vi.mock("../../src/pipeline/users/candidateFollowNotificationEvents.js", () => ({
  createCandidateRecordUpdateNotificationEvents: createCandidateRecordUpdateNotificationEventsMock,
}));

import {
  PRESIDENTIAL_CANDIDATE_RECORD_AREA_LABEL_AI_CANDIDATES,
  PRESIDENTIAL_CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES,
} from "../../src/ai/aiCandidates.js";
import { runCandidateRecordEnricher } from "../../src/pipeline/enrichers/candidateRecordEnricher.js";

describe("runCandidateRecordEnricher presidential-cycle routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
    redisConnectMock.mockResolvedValue(undefined);
    redisQuitMock.mockResolvedValue(undefined);
    redisXGroupCreateMock.mockResolvedValue(undefined);
    redisXAutoClaimMock.mockResolvedValue({ nextId: "0-0", messages: [] });
    redisSendCommandMock.mockResolvedValue([]);
    redisXAckMock.mockResolvedValue(1);
    redisSetMock.mockResolvedValue("OK");
    poolQueryMock.mockResolvedValue({ rows: [] });
    poolConnectMock.mockResolvedValue({
      query: poolQueryMock,
      release: poolClientReleaseMock,
    });
    poolClientReleaseMock.mockReturnValue(undefined);
    createCandidateRecordUpdateNotificationEventsMock.mockResolvedValue({ createdCount: 0 });
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:record:draft",
        messages: [
          {
            id: "1-0",
            message: {
              context_type: "presidential_cycle",
              candidate_id: "candidate-president",
              presidential_cycle_id: "cycle-2028",
              presidential_role: "president",
              item_type: "candidate_record",
              run_id: "run-president",
            },
          },
        ],
      },
    ]);
    poolEndMock.mockResolvedValue(undefined);
    loadPresidentialContextMock.mockResolvedValue({
      candidateId: "candidate-president",
      candidateDisplayName: "Jane President",
      electionId: "",
      presidentialCycleId: "cycle-2028",
      presidentialRole: "president",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: "2028-11-07",
      officialBallotTitle: "President of the United States, 2028 Democratic primary",
      electionStage: "primary",
      senateClass: null,
      termEndYear: null,
      officeId: "office-president",
      discoveryContestFamily: null,
      electionSources: [],
    });
    enrichCandidateRecordsMock.mockResolvedValue({
      ok: true,
      records: [],
      droppedRecords: [],
      aiRawDebug: null,
      provider: "openai",
      model: "test-model",
    });
    enrichCandidateRecordAreasMock.mockResolvedValue({
      ok: true,
      labels: [],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    runLifecycleMock.mockImplementation(async (_pool, options, executeSearch) => {
      const window = { mode: "full", sinceDate: null };
      const metrics = await executeSearch({ candidateId: options.candidateId, window });
      return {
        status: "completed",
        candidateId: options.candidateId,
        window,
        metrics,
      };
    });
    summarizeLifecycleMock.mockReturnValue({
      claimed_count: 1,
      skipped_cooldown_or_claim_count: 0,
      discovered_count: 0,
      inserted_count: 0,
      deduped_count: 0,
      tagged_specific_count: 0,
      tagged_general_count: 0,
    });
  });

  it("acks and skips presidential record drafts when presidential elections are disabled", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    redisSendCommandMock.mockResolvedValueOnce([["1-0", "consumer", 0, 8]]);

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-0"
    );
    expect(redisSendCommandMock).not.toHaveBeenCalled();
    expect(loadPresidentialContextMock).not.toHaveBeenCalled();
    expect(loadElectionContextMock).not.toHaveBeenCalled();
    expect(runLifecycleMock).not.toHaveBeenCalled();
    expect(enrichCandidateRecordsMock).not.toHaveBeenCalled();
    expect(enrichCandidateRecordAreasMock).not.toHaveBeenCalled();
    expect(poolQueryMock).not.toHaveBeenCalled();
    expect(poolConnectMock).not.toHaveBeenCalled();
  });

  it("loads presidential context and passes it to candidate-record discovery", async () => {
    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(loadElectionContextMock).not.toHaveBeenCalled();
    expect(loadPresidentialContextMock).toHaveBeenCalledWith(
      expect.anything(),
      "candidate-president",
      "cycle-2028",
      "president"
    );
    expect(runLifecycleMock).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        candidateId: "candidate-president",
        ignoreCooldown: true,
      }),
      expect.any(Function)
    );
    expect(enrichCandidateRecordsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane President",
        districtName: "United States",
        districtType: "presidential",
        state: "US",
        electionDate: "2028-11-07",
        officialBallotTitle: "President of the United States, 2028 Democratic primary",
        electionStage: "primary",
      }),
      { timeoutMs: 1000 },
      PRESIDENTIAL_CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES
    );
    expect(enrichCandidateRecordsMock.mock.calls[0]?.[0]).not.toHaveProperty("existingRecordsToAvoid");
    expect(redisSetMock).toHaveBeenCalledWith("staging:candidate_record_run_processed:run-president", "completed", {
      EX: 86_400,
    });
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-0"
    );
  });

  it("keeps existing records when a presidential refresh completes with no new records", async () => {
    poolQueryMock.mockResolvedValue({
      rows: [],
      rowCount: 3,
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(poolConnectMock).not.toHaveBeenCalled();
    expect(poolQueryMock).not.toHaveBeenCalledWith(
      expect.stringContaining("DELETE FROM public.candidate_records"),
      expect.anything()
    );
    expect(enrichCandidateRecordsMock.mock.calls[0]?.[0]).not.toHaveProperty("existingRecordsToAvoid");
  });

  it("uses presidential AI candidates for presidential record discovery and area labeling", async () => {
    enrichCandidateRecordsMock.mockResolvedValue({
      ok: true,
      records: [
        {
          description: "Jane President signed a national defense authorization bill.",
          source_url: "https://example.gov/defense",
          event_date: "2025-05-01",
        },
      ],
      droppedRecords: [],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    enrichCandidateRecordAreasMock.mockResolvedValue({
      ok: true,
      labels: [
        {
          record_index: 0,
          research_area_slug: "general",
        },
      ],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("DELETE FROM public.candidate_records")) {
        return { rows: [], rowCount: 2 };
      }
      if (sql.includes("SELECT id, description, record_identity_key")) {
        return { rows: [] };
      }
      if (sql.includes("INSERT INTO public.candidate_records")) {
        return { rows: [{ id: "record-1", inserted: true }], rowCount: 1 };
      }
      if (sql.includes("WITH office_bound")) {
        return { rows: [{ id: "area-general", slug: "general" }] };
      }
      if (sql.includes("INSERT INTO public.candidate_record_area_tags")) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(createCandidateRecordUpdateNotificationEventsMock).not.toHaveBeenCalled();
    expect(enrichCandidateRecordsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane President",
        officialBallotTitle: "President of the United States, 2028 Democratic primary",
      }),
      { timeoutMs: 1000 },
      PRESIDENTIAL_CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES
    );
    expect(enrichCandidateRecordAreasMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane President",
        allowedResearchAreaSlugs: ["general"],
        records: [
          {
            description: "Jane President signed a national defense authorization bill.",
            sourceUrl: "https://example.gov/defense",
            eventDate: "2025-05-01",
          },
        ],
      }),
      { timeoutMs: 1000 },
      PRESIDENTIAL_CANDIDATE_RECORD_AREA_LABEL_AI_CANDIDATES
    );
  });

  it("rolls back presidential replacement when the replacement insert fails", async () => {
    enrichCandidateRecordsMock.mockResolvedValue({
      ok: true,
      records: [
        {
          description: "Jane President signed a national defense authorization bill.",
          source_url: "https://example.gov/defense",
          event_date: "2025-05-01",
        },
      ],
      droppedRecords: [],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    enrichCandidateRecordAreasMock.mockResolvedValue({
      ok: true,
      labels: [
        {
          record_index: 0,
          research_area_slug: "general",
        },
      ],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("WITH office_bound")) {
        return { rows: [{ id: "area-general", slug: "general" }] };
      }
      if (sql.includes("DELETE FROM public.candidate_records")) {
        return { rows: [], rowCount: 2 };
      }
      if (sql.includes("SELECT id, description, record_identity_key")) {
        return { rows: [] };
      }
      if (sql.includes("INSERT INTO public.candidate_records")) {
        throw new Error("insert failed");
      }
      return { rows: [], rowCount: 0 };
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(poolQueryMock).toHaveBeenCalledWith("BEGIN");
    expect(poolQueryMock).toHaveBeenCalledWith("ROLLBACK");
    expect(poolQueryMock.mock.calls.some((call) => call[0] === "COMMIT")).toBe(false);
    expect(poolClientReleaseMock).toHaveBeenCalled();
    expect(redisXAckMock).not.toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-0"
    );
  });

  it("counts presidential label validation failures in batch stats", async () => {
    enrichCandidateRecordsMock.mockResolvedValue({
      ok: true,
      records: [
        {
          description: "Jane President signed a national defense authorization bill.",
          source_url: "https://example.gov/defense",
          event_date: "2025-05-01",
        },
      ],
      droppedRecords: [],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    enrichCandidateRecordAreasMock.mockResolvedValue({
      ok: true,
      labels: [
        {
          record_index: 0,
          research_area_slug: "unknown_area",
        },
      ],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("WITH office_bound")) {
        return { rows: [{ id: "area-general", slug: "general" }] };
      }
      if (sql.includes("DELETE FROM public.candidate_records")) {
        return { rows: [], rowCount: 2 };
      }
      if (sql.includes("SELECT id, description, record_identity_key")) {
        return { rows: [] };
      }
      if (sql.includes("INSERT INTO public.candidate_records")) {
        return { rows: [{ id: "record-1", inserted: true }], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    });
    const consoleLogSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    try {
      expect(poolQueryMock).toHaveBeenCalledWith("BEGIN");
      expect(poolQueryMock).toHaveBeenCalledWith("ROLLBACK");
      expect(poolQueryMock.mock.calls.some((call) => call[0] === "COMMIT")).toBe(false);
      expect(consoleLogSpy).toHaveBeenCalledWith(
        expect.stringContaining("label_validation_rejected=1")
      );
      expect(redisXAckMock).not.toHaveBeenCalledWith(
        "staging:candidates:record:draft",
        "candidate_record_enricher",
        "1-0"
      );
    } finally {
      consoleLogSpy.mockRestore();
    }
  });

  it("loads vice-president presidential context for running-mate record drafts", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:record:draft",
        messages: [
          {
            id: "1-1",
            message: {
              context_type: "presidential_cycle",
              candidate_id: "candidate-vp",
              presidential_cycle_id: "cycle-2028",
              presidential_role: "vice_president",
              item_type: "candidate_record",
              run_id: "run-vp",
            },
          },
        ],
      },
    ]);
    loadPresidentialContextMock.mockResolvedValue({
      candidateId: "candidate-vp",
      candidateDisplayName: "Pat Running Mate",
      electionId: "",
      presidentialCycleId: "cycle-2028",
      presidentialRole: "vice_president",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: "2028-11-07",
      officialBallotTitle: "Vice President of the United States, 2028 general election",
      electionStage: "general",
      senateClass: null,
      termEndYear: null,
      officeId: "office-vp",
      discoveryContestFamily: null,
      electionSources: [],
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(loadElectionContextMock).not.toHaveBeenCalled();
    expect(loadPresidentialContextMock).toHaveBeenCalledWith(
      expect.anything(),
      "candidate-vp",
      "cycle-2028",
      "vice_president"
    );
    expect(runLifecycleMock).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        candidateId: "candidate-vp",
        ignoreCooldown: true,
      }),
      expect.any(Function)
    );
    expect(enrichCandidateRecordsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Pat Running Mate",
        districtName: "United States",
        districtType: "presidential",
        state: "US",
        electionDate: "2028-11-07",
        officialBallotTitle: "Vice President of the United States, 2028 general election",
        electionStage: "general",
      }),
      { timeoutMs: 1000 },
      PRESIDENTIAL_CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES
    );
    expect(enrichCandidateRecordsMock.mock.calls[0]?.[0]).not.toHaveProperty("existingRecordsToAvoid");
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-1"
    );
  });

  it("keeps normal election record drafts on the election context path without cooldown bypass", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:record:draft",
        messages: [
          {
            id: "1-2",
            message: {
              candidate_id: "candidate-election",
              election_id: "election-1",
              item_type: "candidate_record",
              run_id: "run-election",
            },
          },
        ],
      },
    ]);
    loadElectionContextMock.mockResolvedValue({
      candidateId: "candidate-election",
      candidateDisplayName: "Jane Candidate",
      electionId: "election-1",
      districtName: "California",
      districtType: "statewide",
      state: "CA",
      electionDate: "2028-11-07",
      officialBallotTitle: "Governor",
      electionStage: "general",
      senateClass: null,
      termEndYear: null,
      officeId: "office-governor",
      discoveryContestFamily: "non_judicial_office",
      electionSources: [],
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(loadPresidentialContextMock).not.toHaveBeenCalled();
    expect(loadElectionContextMock).toHaveBeenCalledWith(expect.anything(), "candidate-election", "election-1");
    const lifecycleOptions = runLifecycleMock.mock.calls[0]?.[1] as { ignoreCooldown?: boolean };
    expect(lifecycleOptions.ignoreCooldown).toBe(false);
    expect(enrichCandidateRecordsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane Candidate",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2028-11-07",
        officialBallotTitle: "Governor",
        electionStage: "general",
        discoveryContestFamily: "non_judicial_office",
      }),
      { timeoutMs: 1000 },
      undefined
    );
    expect(enrichCandidateRecordsMock.mock.calls[0]?.[0]).not.toHaveProperty("existingRecordsToAvoid");
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-2"
    );
  });

  it("creates notification events for inserted normal election candidate records", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:record:draft",
        messages: [
          {
            id: "1-3",
            message: {
              candidate_id: "candidate-election",
              election_id: "election-1",
              item_type: "candidate_record",
              run_id: "run-election-with-record",
            },
          },
        ],
      },
    ]);
    loadElectionContextMock.mockResolvedValue({
      candidateId: "candidate-election",
      candidateDisplayName: "Jane Candidate",
      electionId: "election-1",
      districtName: "California",
      districtType: "statewide",
      state: "CA",
      electionDate: "2028-11-07",
      officialBallotTitle: "Governor",
      electionStage: "general",
      senateClass: null,
      termEndYear: null,
      officeId: "office-governor",
      discoveryContestFamily: "non_judicial_office",
      electionSources: [],
    });
    enrichCandidateRecordsMock.mockResolvedValue({
      ok: true,
      records: [
        {
          description: "Jane Candidate sponsored a transportation bill.",
          source_url: "https://example.gov/transportation",
          event_date: "2026-04-01",
        },
      ],
      droppedRecords: [],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    enrichCandidateRecordAreasMock.mockResolvedValue({
      ok: true,
      labels: [
        {
          record_index: 0,
          research_area_slug: "general",
        },
      ],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("SELECT id, description, record_identity_key")) {
        return { rows: [] };
      }
      if (sql.includes("INSERT INTO public.candidate_records")) {
        return { rows: [{ id: "record-1", inserted: true }], rowCount: 1 };
      }
      if (sql.includes("WITH office_bound")) {
        return { rows: [{ id: "area-general", slug: "general" }] };
      }
      if (sql.includes("INSERT INTO public.candidate_record_area_tags")) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(createCandidateRecordUpdateNotificationEventsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        query: poolQueryMock,
      }),
      "record-1"
    );
    expect(poolQueryMock).toHaveBeenCalledWith("BEGIN");
    expect(poolQueryMock).toHaveBeenCalledWith("COMMIT");
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-3"
    );
  });

  it("does not create notification events for deduped normal election candidate records", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:record:draft",
        messages: [
          {
            id: "1-4",
            message: {
              candidate_id: "candidate-election",
              election_id: "election-1",
              item_type: "candidate_record",
              run_id: "run-election-deduped-record",
            },
          },
        ],
      },
    ]);
    loadElectionContextMock.mockResolvedValue({
      candidateId: "candidate-election",
      candidateDisplayName: "Jane Candidate",
      electionId: "election-1",
      districtName: "California",
      districtType: "statewide",
      state: "CA",
      electionDate: "2028-11-07",
      officialBallotTitle: "Governor",
      electionStage: "general",
      senateClass: null,
      termEndYear: null,
      officeId: "office-governor",
      discoveryContestFamily: "non_judicial_office",
      electionSources: [],
    });
    enrichCandidateRecordsMock.mockResolvedValue({
      ok: true,
      records: [
        {
          description: "Jane Candidate sponsored a transportation bill.",
          source_url: "https://example.gov/transportation",
          event_date: "2026-04-01",
        },
      ],
      droppedRecords: [],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    enrichCandidateRecordAreasMock.mockResolvedValue({
      ok: true,
      labels: [
        {
          record_index: 0,
          research_area_slug: "general",
        },
      ],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("SELECT id, description, record_identity_key")) {
        return { rows: [] };
      }
      if (sql.includes("INSERT INTO public.candidate_records")) {
        return { rows: [{ id: "record-1", inserted: false }], rowCount: 1 };
      }
      if (sql.includes("WITH office_bound")) {
        return { rows: [{ id: "area-general", slug: "general" }] };
      }
      if (sql.includes("INSERT INTO public.candidate_record_area_tags")) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(createCandidateRecordUpdateNotificationEventsMock).not.toHaveBeenCalled();
    expect(poolQueryMock).toHaveBeenCalledWith("BEGIN");
    expect(poolQueryMock).toHaveBeenCalledWith("COMMIT");
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-4"
    );
  });

  it("rolls back normal election candidate record enrichment when notification event creation fails", async () => {
    createCandidateRecordUpdateNotificationEventsMock.mockRejectedValueOnce(new Error("notification insert failed"));
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:record:draft",
        messages: [
          {
            id: "1-5",
            message: {
              candidate_id: "candidate-election",
              election_id: "election-1",
              item_type: "candidate_record",
              run_id: "run-election-notification-failure",
            },
          },
        ],
      },
    ]);
    loadElectionContextMock.mockResolvedValue({
      candidateId: "candidate-election",
      candidateDisplayName: "Jane Candidate",
      electionId: "election-1",
      districtName: "California",
      districtType: "statewide",
      state: "CA",
      electionDate: "2028-11-07",
      officialBallotTitle: "Governor",
      electionStage: "general",
      senateClass: null,
      termEndYear: null,
      officeId: "office-governor",
      discoveryContestFamily: "non_judicial_office",
      electionSources: [],
    });
    enrichCandidateRecordsMock.mockResolvedValue({
      ok: true,
      records: [
        {
          description: "Jane Candidate sponsored a transportation bill.",
          source_url: "https://example.gov/transportation",
          event_date: "2026-04-01",
        },
      ],
      droppedRecords: [],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    enrichCandidateRecordAreasMock.mockResolvedValue({
      ok: true,
      labels: [
        {
          record_index: 0,
          research_area_slug: "general",
        },
      ],
      aiRawDebug: null,
      provider: "claude",
      model: "test-model",
    });
    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("SELECT id, description, record_identity_key")) {
        return { rows: [] };
      }
      if (sql.includes("INSERT INTO public.candidate_records")) {
        return { rows: [{ id: "record-1", inserted: true }], rowCount: 1 };
      }
      if (sql.includes("WITH office_bound")) {
        return { rows: [{ id: "area-general", slug: "general" }] };
      }
      if (sql.includes("INSERT INTO public.candidate_record_area_tags")) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    });

    await runCandidateRecordEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(createCandidateRecordUpdateNotificationEventsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        query: poolQueryMock,
      }),
      "record-1"
    );
    expect(poolQueryMock).toHaveBeenCalledWith("ROLLBACK");
    expect(poolQueryMock).not.toHaveBeenCalledWith("COMMIT");
    expect(redisXAckMock).not.toHaveBeenCalledWith(
      "staging:candidates:record:draft",
      "candidate_record_enricher",
      "1-5"
    );
  });
});
