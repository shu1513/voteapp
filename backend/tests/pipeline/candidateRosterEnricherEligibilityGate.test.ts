import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});

const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});
const redisXGroupCreateMock = vi.fn(async () => "OK");
const redisXAutoClaimMock = vi.fn(async () => ({ nextId: "0-0", messages: [] }));
const redisXReadGroupMock = vi.fn();
const redisXAckMock = vi.fn(async () => 1);
const redisSendCommandMock = vi.fn(async () => []);

const enrichCandidateRosterMock = vi.fn();
const eligibilityMock = vi.fn();

describe("runCandidateRosterEnricher eligibility gate", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.CANDIDATE_ROSTER_ENABLE_ENRICHER_ELIGIBILITY_GATE = "true";

    redisXReadGroupMock.mockResolvedValue([
      {
        name: "staging:candidates:roster:draft",
        messages: [
          {
            id: "1-0",
            message: {
              election_id: "00000000-0000-0000-0000-000000000999",
              item_type: "candidate_roster",
              run_id: "run_1",
            },
          },
        ],
      },
    ]);
  });

  it("acks and skips AI call when election is not currently eligible", async () => {
    vi.doMock("pg", () => ({
      Pool: vi.fn(() => ({
        query: poolQueryMock,
        end: poolEndMock,
      })),
    }));

    vi.doMock("redis", () => ({
      createClient: vi.fn(() => ({
        connect: redisConnectMock,
        quit: redisQuitMock,
        xGroupCreate: redisXGroupCreateMock,
        xAutoClaim: redisXAutoClaimMock,
        xReadGroup: redisXReadGroupMock,
        xAck: redisXAckMock,
        sendCommand: redisSendCommandMock,
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

    vi.doMock("../../src/ai/enrichCandidateRoster.js", () => ({
      buildCandidateRosterConfigFromEnv: () => ({}),
      enrichCandidateRoster: enrichCandidateRosterMock,
      disambiguateCandidateDuplicateGroup: vi.fn(),
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
      getOfficeCandidateEligibilityForElectionId: eligibilityMock,
    }));

    const { runCandidateRosterEnricher } = await import(
      "../../src/pipeline/enrichers/candidateRosterEnricher.js"
    );

    eligibilityMock.mockResolvedValue({
      election_id: "00000000-0000-0000-0000-000000000999",
      reason: "buffer_not_elapsed",
      prior_election_date: "2026-06-02",
      stage_gap_days: 154,
      buffer_days: 7,
      eligible_after_date: "2026-06-09",
    });

    await runCandidateRosterEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(eligibilityMock).toHaveBeenCalledTimes(1);
    expect(enrichCandidateRosterMock).not.toHaveBeenCalled();
    expect(redisXAckMock).toHaveBeenCalledWith(
      "staging:candidates:roster:draft",
      "candidate_roster_enricher",
      "1-0"
    );
  });
});
