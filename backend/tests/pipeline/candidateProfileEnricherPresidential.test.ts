import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.hoisted(() => vi.fn());
const poolConnectMock = vi.hoisted(() => vi.fn());
const poolEndMock = vi.hoisted(() => vi.fn());
const clientQueryMock = vi.hoisted(() => vi.fn());
const clientReleaseMock = vi.hoisted(() => vi.fn());
const redisConnectMock = vi.hoisted(() => vi.fn());
const redisQuitMock = vi.hoisted(() => vi.fn());
const redisXGroupCreateMock = vi.hoisted(() => vi.fn());
const redisXAutoClaimMock = vi.hoisted(() => vi.fn());
const redisSendCommandMock = vi.hoisted(() => vi.fn());
const redisXReadGroupMock = vi.hoisted(() => vi.fn());
const redisXAckMock = vi.hoisted(() => vi.fn());
const redisXAddMock = vi.hoisted(() => vi.fn());
const enrichCandidateProfileMock = vi.hoisted(() => vi.fn());
const enqueueCandidateRecordDraftsMock = vi.hoisted(() => vi.fn());
const enqueueCandidateLinkCandidateFinanceSyncJobMock = vi.hoisted(() => vi.fn());
const enqueueManualCaliforniaCandidateFinanceSyncJobMock = vi.hoisted(() => vi.fn());
const buildCaliforniaCandidateFinanceLinkedElectionSyncJobIdMock = vi.hoisted(() => vi.fn());
const enqueueManualColoradoCandidateFinanceSyncJobMock = vi.hoisted(() => vi.fn());
const buildColoradoCandidateFinanceLinkedElectionSyncJobIdMock = vi.hoisted(() => vi.fn());
const enqueueManualConnecticutCandidateFinanceSyncJobMock = vi.hoisted(() => vi.fn());
const buildConnecticutCandidateFinanceLinkedElectionSyncJobIdMock = vi.hoisted(() => vi.fn());

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
    xAdd: redisXAddMock,
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

vi.mock("../../src/ai/enrichCandidateProfile.js", () => ({
  buildCandidateProfileConfigFromEnv: vi.fn(() => ({ timeoutMs: 1000 })),
  enrichCandidateProfile: enrichCandidateProfileMock,
}));

vi.mock("../../src/pipeline/candidates/candidateRecordDraftEmitter.js", () => ({
  enqueueCandidateRecordDrafts: enqueueCandidateRecordDraftsMock,
}));

vi.mock("../../src/scheduler/candidateFinanceSyncScheduler.js", () => ({
  enqueueCandidateLinkCandidateFinanceSyncJob: enqueueCandidateLinkCandidateFinanceSyncJobMock,
}));

vi.mock("../../src/scheduler/californiaCandidateFinanceSyncScheduler.js", () => ({
  buildCaliforniaCandidateFinanceLinkedElectionSyncJobId:
    buildCaliforniaCandidateFinanceLinkedElectionSyncJobIdMock,
  enqueueManualCaliforniaCandidateFinanceSyncJob: enqueueManualCaliforniaCandidateFinanceSyncJobMock,
}));

vi.mock("../../src/scheduler/coloradoCandidateFinanceSyncScheduler.js", () => ({
  buildColoradoCandidateFinanceLinkedElectionSyncJobId:
    buildColoradoCandidateFinanceLinkedElectionSyncJobIdMock,
  enqueueManualColoradoCandidateFinanceSyncJob: enqueueManualColoradoCandidateFinanceSyncJobMock,
}));

vi.mock("../../src/scheduler/connecticutCandidateFinanceSyncScheduler.js", () => ({
  buildConnecticutCandidateFinanceLinkedElectionSyncJobId:
    buildConnecticutCandidateFinanceLinkedElectionSyncJobIdMock,
  enqueueManualConnecticutCandidateFinanceSyncJob: enqueueManualConnecticutCandidateFinanceSyncJobMock,
}));

import { runCandidateProfileEnricher } from "../../src/pipeline/enrichers/candidateProfileEnricher.js";
import { PRESIDENTIAL_PROFILE_AI_CANDIDATES } from "../../src/ai/aiCandidates.js";

describe("runCandidateProfileEnricher presidential cycle routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    delete process.env.PRESIDENTIAL_ELECTIONS_ENABLED;
    redisConnectMock.mockResolvedValue(undefined);
    redisQuitMock.mockResolvedValue(undefined);
    redisXGroupCreateMock.mockResolvedValue(undefined);
    redisXAutoClaimMock.mockResolvedValue({ nextId: "0-0", messages: [] });
    redisSendCommandMock.mockResolvedValue([]);
    redisXAckMock.mockResolvedValue(1);
    redisXAddMock.mockResolvedValue("2-0");
    enqueueCandidateLinkCandidateFinanceSyncJobMock.mockResolvedValue("finance-job-1");
    enqueueManualCaliforniaCandidateFinanceSyncJobMock.mockResolvedValue("california-finance-job-1");
    enqueueManualColoradoCandidateFinanceSyncJobMock.mockResolvedValue("colorado-finance-job-1");
    enqueueManualConnecticutCandidateFinanceSyncJobMock.mockResolvedValue("connecticut-finance-job-1");
    buildCaliforniaCandidateFinanceLinkedElectionSyncJobIdMock.mockReturnValue(
      "california-candidate-finance-linked-election-sync-2026-06-01"
    );
    buildColoradoCandidateFinanceLinkedElectionSyncJobIdMock.mockReturnValue(
      "colorado-candidate-finance-linked-election-sync-2026-06-01"
    );
    buildConnecticutCandidateFinanceLinkedElectionSyncJobIdMock.mockReturnValue(
      "connecticut-candidate-finance-linked-election-sync-2026-06-01"
    );
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-0",
            message: {
              context_type: "presidential_cycle",
              presidential_cycle_id: "cycle-1",
              item_type: "candidate_profile",
              candidate_display_name: "Jane President",
              roster_party: "Republican",
              roster_fec_ids: JSON.stringify(["P80000001"]),
              seed_urls: JSON.stringify(["https://www.fec.gov/data/candidate/P80000001"]),
              run_id: "run-1",
            },
          },
        ],
      },
    ]);

    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (String(sql).includes("FROM public.presidential_cycles")) {
        expect(params).toEqual(["cycle-1"]);
        return {
          rows: [
            {
              id: "cycle-1",
              election_year: 2028,
              stage: "primary",
              party: "Democratic",
              election_date: null,
              sources: ["https://example.gov/primary"],
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });

    poolConnectMock.mockResolvedValue({ query: clientQueryMock, release: clientReleaseMock });
    poolEndMock.mockResolvedValue(undefined);
    clientReleaseMock.mockReturnValue(undefined);
    clientQueryMock.mockImplementation(async (sql: string) => {
      const text = String(sql);
      if (text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK") {
        return { rows: [], rowCount: null };
      }
      if (text.includes("FROM public.candidates")) {
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("INSERT INTO public.candidates")) {
        return { rows: [{ id: "candidate-1" }], rowCount: 1 };
      }
      if (text.includes("INSERT INTO public.presidential_cycle_candidates")) {
        return { rows: [], rowCount: 1 };
      }
      if (text.includes("FROM public.presidential_cycle_candidates AS cycle_candidate")) {
        return { rows: [{ candidate_id: "candidate-parent" }], rowCount: 1 };
      }
      if (text.includes("UPDATE public.presidential_cycle_candidates")) {
        return { rows: [], rowCount: 1 };
      }
      if (text.includes("INSERT INTO public.candidate_elections")) {
        return { rows: [], rowCount: 1 };
      }
      throw new Error(`Unexpected client query: ${text}`);
    });

    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane President",
        first_name: "Jane",
        last_name: "President",
        party: "Democratic",
        fec_ids: ["P80000001"],
        sources: ["https://www.fec.gov/data/candidate/P80000001"],
      },
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("acks and skips presidential profile drafts when presidential elections are disabled", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    redisSendCommandMock.mockResolvedValueOnce([["1-0", "consumer", 0, 8]]);

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-0"
    );
    expect(poolQueryMock).not.toHaveBeenCalled();
    expect(poolConnectMock).not.toHaveBeenCalled();
    expect(redisSendCommandMock).not.toHaveBeenCalled();
    expect(enrichCandidateProfileMock).not.toHaveBeenCalled();
    expect(enqueueCandidateRecordDraftsMock).not.toHaveBeenCalled();
    expect(redisXAddMock).not.toHaveBeenCalled();
  });

  it("links presidential profile drafts to presidential cycle candidates only", async () => {
    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enrichCandidateProfileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane President",
        districtName: "United States",
        districtType: "presidential",
        state: "US",
        electionDate: null,
        officialBallotTitle: "President of the United States, 2028 Democratic primary",
        electionStage: "primary",
        rosterParty: "Democratic",
        rosterFecIds: ["P80000001"],
        seedUrls: ["https://www.fec.gov/data/candidate/P80000001", "https://example.gov/primary"],
      }),
      { timeoutMs: 1000 },
      PRESIDENTIAL_PROFILE_AI_CANDIDATES
    );

    const executedSql = clientQueryMock.mock.calls.map((call) => String(call[0]));
    const candidateLookupSql = executedSql.find((sql) => sql.includes("FROM public.candidates"));
    expect(candidateLookupSql).not.toContain("AND state = $3");
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.presidential_cycle_candidates"))).toBe(true);
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.candidate_elections"))).toBe(false);
    const presidentialUpsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.presidential_cycle_candidates")
    );
    expect(presidentialUpsertCall?.[1]).toEqual([
      "cycle-1",
      "candidate-1",
      "Democratic",
      "active",
      JSON.stringify(["https://www.fec.gov/data/candidate/P80000001"]),
    ]);
    expect(
      clientQueryMock.mock.calls.some((call) =>
        String(call[0]).includes("presidential_profile_researched = true")
      )
    ).toBe(true);
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        contextType: "presidential_cycle",
        candidateId: "candidate-1",
        presidentialCycleId: "cycle-1",
        presidentialRole: "president",
        runId: "run-1",
      },
    ]);
    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).toHaveBeenCalledWith({
      candidateId: "candidate-1",
      fecCandidateId: "P80000001",
      electionYear: 2028,
      source: "presidential_cycle",
      includeOutside: true,
    });
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-0"
    );
  });

  it("does not enqueue presidential finance sync after the general election grace window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-11-07T12:00:00.000Z"));
    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (String(sql).includes("FROM public.presidential_cycles")) {
        expect(params).toEqual(["cycle-1"]);
        return {
          rows: [
            {
              id: "cycle-1",
              election_year: 2024,
              stage: "primary",
              party: "Democratic",
              election_date: null,
              sources: ["https://example.gov/primary"],
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        contextType: "presidential_cycle",
        candidateId: "candidate-1",
        presidentialCycleId: "cycle-1",
        presidentialRole: "president",
        runId: "run-1",
      },
    ]);
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-0"
    );
  });

  it("links vice-president profile drafts as running mates without creating presidential candidate links", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-2",
            message: {
              context_type: "presidential_cycle",
              presidential_cycle_id: "cycle-1",
              presidential_role: "vice_president",
              parent_presidential_candidate_fec_id: "P80000001",
              item_type: "candidate_profile",
              candidate_display_name: "Pat Running Mate",
              roster_party: "Democratic",
              seed_urls: JSON.stringify(["https://example.gov/running-mate"]),
              run_id: "run-vp",
            },
          },
        ],
      },
    ]);
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Pat Running Mate",
        first_name: "Pat",
        last_name: "Running Mate",
        party: "Democratic",
        official_website_url: "https://example.gov/running-mate",
        sources: ["https://example.gov/running-mate"],
      },
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enrichCandidateProfileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Pat Running Mate",
        officialBallotTitle: "Vice President of the United States, 2028 Democratic primary",
        rosterParty: "Democratic",
        rosterFecIds: [],
        allowMissingFederalFecIds: true,
      }),
      { timeoutMs: 1000 },
      PRESIDENTIAL_PROFILE_AI_CANDIDATES
    );

    const executedSql = clientQueryMock.mock.calls.map((call) => String(call[0]));
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.candidates"))).toBe(true);
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.presidential_cycle_candidates"))).toBe(false);
    expect(
      clientQueryMock.mock.calls.some(
        (call) =>
          String(call[0]).includes("running_mate_candidate_id = $3::uuid") &&
          JSON.stringify(call[1]) === JSON.stringify(["cycle-1", "candidate-parent", "candidate-1"])
      )
    ).toBe(true);
    expect(
      clientQueryMock.mock.calls.some((call) =>
        String(call[0]).includes("running_mate_profile_researched = true")
      )
    ).toBe(true);
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        contextType: "presidential_cycle",
        candidateId: "candidate-1",
        presidentialCycleId: "cycle-1",
        presidentialRole: "vice_president",
        runId: "run-vp",
      },
    ]);
    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-2"
    );
  });

  it("parks vice-president drafts that resolve to the parent presidential candidate", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-4",
            message: {
              context_type: "presidential_cycle",
              presidential_cycle_id: "cycle-1",
              presidential_role: "vice_president",
              parent_presidential_candidate_fec_id: "P80000001",
              item_type: "candidate_profile",
              candidate_display_name: "Jane President",
              roster_party: "Democratic",
              seed_urls: JSON.stringify(["https://example.gov/running-mate"]),
              run_id: "run-self-vp",
            },
          },
        ],
      },
    ]);
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane President",
        first_name: "Jane",
        last_name: "President",
        party: "Democratic",
        sources: ["https://example.gov/running-mate"],
      },
    });
    clientQueryMock.mockImplementation(async (sql: string) => {
      const text = String(sql);
      if (text === "BEGIN" || text === "ROLLBACK") {
        return { rows: [], rowCount: null };
      }
      if (text.includes("FROM public.candidates")) {
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("INSERT INTO public.candidates")) {
        return { rows: [{ id: "candidate-1" }], rowCount: 1 };
      }
      if (text.includes("FROM public.presidential_cycle_candidates AS cycle_candidate")) {
        return { rows: [{ candidate_id: "candidate-1" }], rowCount: 1 };
      }
      throw new Error(`Unexpected client query: ${text}`);
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(clientQueryMock).toHaveBeenCalledWith("ROLLBACK");
    expect(
      clientQueryMock.mock.calls.some((call) =>
        String(call[0]).includes("running_mate_candidate_id = $3::uuid")
      )
    ).toBe(false);
    expect(enqueueCandidateRecordDraftsMock).not.toHaveBeenCalled();
    expect(redisXAddMock).toHaveBeenCalledWith(
      "staging:candidates:profile:rejected",
      "*",
      expect.objectContaining({
        reason: "vice president profile resolved to the parent presidential candidate for FEC ID P80000001",
        presidential_role: "vice_president",
        parent_presidential_candidate_fec_id: "P80000001",
      })
    );
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-4"
    );
  });

  it("parks president profile drafts that are missing roster FEC IDs", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-3",
            message: {
              context_type: "presidential_cycle",
              presidential_cycle_id: "cycle-1",
              presidential_role: "president",
              item_type: "candidate_profile",
              candidate_display_name: "No FEC President",
              roster_party: "Democratic",
              seed_urls: JSON.stringify(["https://example.gov/no-fec"]),
              run_id: "run-no-fec",
            },
          },
        ],
      },
    ]);

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enrichCandidateProfileMock).not.toHaveBeenCalled();
    expect(poolConnectMock).not.toHaveBeenCalled();
    expect(redisXAddMock).toHaveBeenCalledWith(
      "staging:candidates:profile:rejected",
      "*",
      expect.objectContaining({
        reason: "president profile draft requires at least one roster_fec_ids value",
        presidential_role: "president",
      })
    );
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-3"
    );
  });

  it("preserves election profile drafts as candidate election links with record drafts", async () => {
    process.env.PRESIDENTIAL_ELECTIONS_ENABLED = "false";
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-1",
            message: {
              election_id: "election-1",
              item_type: "candidate_profile",
              candidate_display_name: "Jane Candidate",
              roster_party: "Democratic",
              roster_is_incumbent: "true",
              roster_fec_ids: JSON.stringify(["S80000002"]),
              seed_urls: JSON.stringify(["https://example.gov/candidate"]),
              run_id: "run-election",
            },
          },
        ],
      },
    ]);
    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      const text = String(sql);
      if (text.includes("FROM public.candidate_elections AS ce")) {
        expect(params).toEqual(["election-1"]);
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("FROM public.elections AS e")) {
        expect(params).toEqual(["election-1"]);
        return {
          rows: [
            {
              id: "election-1",
              state: "CA",
              district_name: "California",
              district_type: "statewide",
              election_date: "2028-11-07",
              official_ballot_title: "United States Senator",
              election_stage: "general",
              senate_class: null,
              term_end_year: null,
              is_partisan: true,
              sources: ["https://example.gov/election"],
              office_scope: "statewide",
              office_canonical_name: "United States Senator",
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane Candidate",
        first_name: "Jane",
        last_name: "Candidate",
        party: "Democratic",
        fec_ids: ["S80000002"],
        sources: ["https://example.gov/candidate"],
      },
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enrichCandidateProfileMock).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateDisplayName: "Jane Candidate",
        districtName: "California",
        districtType: "statewide",
        state: "CA",
        electionDate: "2028-11-07",
        officialBallotTitle: "United States Senator",
        electionStage: "general",
        rosterParty: "Democratic",
        rosterIncumbent: true,
        rosterFecIds: ["S80000002"],
        seedUrls: ["https://example.gov/candidate", "https://example.gov/election"],
      }),
      { timeoutMs: 1000 }
    );

    const executedSql = clientQueryMock.mock.calls.map((call) => String(call[0]));
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.candidate_elections"))).toBe(true);
    expect(executedSql.some((sql) => sql.includes("INSERT INTO public.presidential_cycle_candidates"))).toBe(false);
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        runId: "run-election",
      },
    ]);
    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).toHaveBeenCalledWith({
      candidateId: "candidate-1",
      fecCandidateId: "S80000002",
      electionYear: 2028,
      includeOutside: true,
    });
    expect(enqueueManualCaliforniaCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueManualColoradoCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueManualConnecticutCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-1"
    );
  });

  it("dedupes automatic California finance batch syncs for eligible California elections", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-4",
            message: {
              election_id: "election-ca-governor",
              item_type: "candidate_profile",
              candidate_display_name: "Jane Governor",
              roster_party: "Democratic",
              roster_is_incumbent: "false",
              seed_urls: JSON.stringify(["https://example.gov/governor"]),
              run_id: "run-ca-governor",
            },
          },
        ],
      },
    ]);
    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      const text = String(sql);
      if (text.includes("FROM public.candidate_elections AS ce")) {
        expect(params).toEqual(["election-ca-governor"]);
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("FROM public.elections AS e")) {
        expect(params).toEqual(["election-ca-governor"]);
        return {
          rows: [
            {
              id: "election-ca-governor",
              state: "CA",
              district_name: "California",
              district_type: "statewide",
              election_date: "2026-11-03",
              official_ballot_title: "Governor",
              election_stage: "general",
              senate_class: null,
              term_end_year: null,
              is_partisan: true,
              sources: ["https://example.gov/election"],
              office_scope: "statewide",
              office_canonical_name: "Governor",
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane Governor",
        first_name: "Jane",
        last_name: "Governor",
        party: "Democratic",
        fec_ids: [],
        sources: ["https://example.gov/governor"],
      },
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(buildCaliforniaCandidateFinanceLinkedElectionSyncJobIdMock).toHaveBeenCalledTimes(1);
    expect(enqueueManualCaliforniaCandidateFinanceSyncJobMock).toHaveBeenCalledWith(
      {
        includeOutside: true,
        triggeredBy: "manual",
      },
      {
        jobId: "california-candidate-finance-linked-election-sync-2026-06-01",
      }
    );
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        candidateId: "candidate-1",
        electionId: "election-ca-governor",
        runId: "run-ca-governor",
      },
    ]);
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-4"
    );
  });

  it("dedupes automatic Colorado finance batch syncs for eligible Colorado elections", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-6",
            message: {
              election_id: "election-co-governor",
              item_type: "candidate_profile",
              candidate_display_name: "Jane Governor",
              roster_party: "Democratic",
              roster_is_incumbent: "false",
              seed_urls: JSON.stringify(["https://example.gov/governor"]),
              run_id: "run-co-governor",
            },
          },
        ],
      },
    ]);
    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      const text = String(sql);
      if (text.includes("FROM public.candidate_elections AS ce")) {
        expect(params).toEqual(["election-co-governor"]);
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("FROM public.elections AS e")) {
        expect(params).toEqual(["election-co-governor"]);
        return {
          rows: [
            {
              id: "election-co-governor",
              state: "CO",
              district_name: "Colorado",
              district_type: "statewide",
              election_date: "2026-11-03",
              official_ballot_title: "Governor",
              election_stage: "general",
              senate_class: null,
              term_end_year: null,
              is_partisan: true,
              sources: ["https://example.gov/election"],
              office_scope: "statewide",
              office_canonical_name: "Governor",
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane Governor",
        first_name: "Jane",
        last_name: "Governor",
        party: "Democratic",
        fec_ids: [],
        sources: ["https://example.gov/governor"],
      },
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueManualCaliforniaCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(buildColoradoCandidateFinanceLinkedElectionSyncJobIdMock).toHaveBeenCalledTimes(1);
    expect(enqueueManualColoradoCandidateFinanceSyncJobMock).toHaveBeenCalledWith(
      {
        triggeredBy: "manual",
      },
      {
        jobId: "colorado-candidate-finance-linked-election-sync-2026-06-01",
      }
    );
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        candidateId: "candidate-1",
        electionId: "election-co-governor",
        runId: "run-co-governor",
      },
    ]);
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-6"
    );
  });

  it("dedupes automatic Connecticut finance batch syncs for eligible Connecticut elections", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-7",
            message: {
              election_id: "election-ct-sos",
              item_type: "candidate_profile",
              candidate_display_name: "Jane Secretary",
              roster_party: "Democratic",
              roster_is_incumbent: "false",
              seed_urls: JSON.stringify(["https://example.gov/secretary"]),
              run_id: "run-ct-sos",
            },
          },
        ],
      },
    ]);
    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      const text = String(sql);
      if (text.includes("FROM public.candidate_elections AS ce")) {
        expect(params).toEqual(["election-ct-sos"]);
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("FROM public.elections AS e")) {
        expect(params).toEqual(["election-ct-sos"]);
        return {
          rows: [
            {
              id: "election-ct-sos",
              state: "CT",
              district_name: "Connecticut",
              district_type: "statewide",
              election_date: "2026-11-03",
              official_ballot_title: "Secretary of State",
              election_stage: "general",
              senate_class: null,
              term_end_year: null,
              is_partisan: true,
              sources: ["https://example.gov/election"],
              office_scope: "statewide",
              office_canonical_name: "Secretary of State",
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane Secretary",
        first_name: "Jane",
        last_name: "Secretary",
        party: "Democratic",
        fec_ids: [],
        sources: ["https://example.gov/secretary"],
      },
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueManualCaliforniaCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueManualColoradoCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(buildConnecticutCandidateFinanceLinkedElectionSyncJobIdMock).toHaveBeenCalledTimes(1);
    expect(enqueueManualConnecticutCandidateFinanceSyncJobMock).toHaveBeenCalledWith(
      {
        triggeredBy: "manual",
      },
      {
        jobId: "connecticut-candidate-finance-linked-election-sync-2026-06-01",
      }
    );
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        candidateId: "candidate-1",
        electionId: "election-ct-sos",
        runId: "run-ct-sos",
      },
    ]);
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-7"
    );
  });

  it("preserves election profile drafts with missing office metadata and skips finance sync", async () => {
    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:profile:draft",
        messages: [
          {
            id: "1-5",
            message: {
              election_id: "election-governor",
              item_type: "candidate_profile",
              candidate_display_name: "Jane Governor",
              roster_party: "Democratic",
              roster_is_incumbent: "false",
              roster_fec_ids: JSON.stringify(["S80000003"]),
              seed_urls: JSON.stringify(["https://example.gov/governor"]),
              run_id: "run-governor",
            },
          },
        ],
      },
    ]);
    poolQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      const text = String(sql);
      if (text.includes("FROM public.candidate_elections AS ce")) {
        expect(params).toEqual(["election-governor"]);
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("FROM public.elections AS e")) {
        expect(params).toEqual(["election-governor"]);
        expect(text).toContain("LEFT JOIN public.offices AS office");
        return {
          rows: [
            {
              id: "election-governor",
              state: "CA",
              district_name: "California",
              district_type: "statewide",
              election_date: "2026-11-03",
              official_ballot_title: "Governor",
              election_stage: "general",
              senate_class: null,
              term_end_year: null,
              is_partisan: true,
              sources: ["https://example.gov/election"],
              office_scope: null,
              office_canonical_name: null,
            },
          ],
        };
      }
      throw new Error(`Unexpected pool query: ${sql}`);
    });
    enrichCandidateProfileMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "test-model",
      aiRawDebug: null,
      profile: {
        display_name: "Jane Governor",
        first_name: "Jane",
        last_name: "Governor",
        party: "Democratic",
        fec_ids: ["S80000003"],
        sources: ["https://example.gov/governor"],
      },
    });

    await runCandidateProfileEnricher({ once: true, blockMs: 1, batchSize: 1 });

    expect(enqueueCandidateLinkCandidateFinanceSyncJobMock).not.toHaveBeenCalled();
    expect(enqueueCandidateRecordDraftsMock).toHaveBeenCalledWith(expect.anything(), [
      {
        candidateId: "candidate-1",
        electionId: "election-governor",
        runId: "run-governor",
      },
    ]);
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:profile:draft",
      "candidate_profile_enricher",
      "1-5"
    );
  });
});
