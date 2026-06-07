import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  poolQueryMock: vi.fn(),
  poolConnectMock: vi.fn(),
  poolEndMock: vi.fn(async () => {}),
  clientQueryMock: vi.fn(),
  clientReleaseMock: vi.fn(),
  redisConnectMock: vi.fn(async () => {}),
  redisQuitMock: vi.fn(async () => {}),
  redisXGroupCreateMock: vi.fn(async () => "OK"),
  redisXAutoClaimMock: vi.fn(async () => ({ nextId: "0-0", messages: [] })),
  redisXReadGroupMock: vi.fn(),
  redisXAckMock: vi.fn(async () => 1),
  redisXAddMock: vi.fn(async () => "1-0"),
  redisSendCommandMock: vi.fn(async () => []),
  enrichBallotMeasureMock: vi.fn(),
}));

vi.mock("pg", () => ({
  Pool: vi.fn(() => ({
    query: mocks.poolQueryMock,
    connect: mocks.poolConnectMock,
    end: mocks.poolEndMock,
  })),
}));

vi.mock("redis", () => ({
  createClient: vi.fn(() => ({
    connect: mocks.redisConnectMock,
    quit: mocks.redisQuitMock,
    xGroupCreate: mocks.redisXGroupCreateMock,
    xAutoClaim: mocks.redisXAutoClaimMock,
    xReadGroup: mocks.redisXReadGroupMock,
    xAck: mocks.redisXAckMock,
    xAdd: mocks.redisXAddMock,
    sendCommand: mocks.redisSendCommandMock,
  })),
}));

vi.mock("../../src/config/env.js", () => ({
  getPipelineEnv: () => ({
    DATABASE_URL: "postgresql://localhost:5432/test",
    REDIS_URL: "redis://localhost:6379/0",
    AI_TIMEOUT_MS: 90000,
    ANTHROPIC_WEB_SEARCH_MAX_USES: 3,
    OPENAI_API_KEY: "test-openai-key",
    ANTHROPIC_API_KEY: undefined,
    GEMINI_API_KEY: undefined,
  }),
}));

vi.mock("../../src/ai/enrichBallotMeasure.js", () => ({
  buildBallotMeasureAiConfigFromEnv: () => ({ timeoutMs: 90000, openAiApiKey: "test-openai-key" }),
  enrichBallotMeasure: mocks.enrichBallotMeasureMock,
}));

import {
  STAGING_BALLOT_MEASURE_DRAFT_STREAM,
  STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
  STAGING_ITEM_TYPE_BALLOT_MEASURE,
} from "../../src/config/electionsPipeline.js";
import { runBallotMeasuresEnricher } from "../../src/pipeline/enrichers/ballotMeasuresEnricher.js";

describe("runBallotMeasuresEnricher", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.poolConnectMock.mockResolvedValue({
      query: mocks.clientQueryMock,
      release: mocks.clientReleaseMock,
    });
    mocks.redisXReadGroupMock.mockResolvedValue([
      {
        name: STAGING_BALLOT_MEASURE_DRAFT_STREAM,
        messages: [
          {
            id: "1-0",
            message: {
              election_id: "election-1",
              item_type: STAGING_ITEM_TYPE_BALLOT_MEASURE,
              run_id: "run-1",
            },
          },
        ],
      },
    ]);
    mocks.enrichBallotMeasureMock.mockResolvedValue({
      ok: true,
      provider: "openai",
      model: "gpt-5.4-mini",
      officialMeasureUrl: "https://example.org/measure.pdf",
      summary: "Measure increases sales tax to fund county hospitals.",
      whatYesMeans: "Approves the tax increase and hospital funding.",
      whatNoMeans: "Rejects the tax increase and hospital funding.",
      researchAreaTags: [
        { researchAreaSlug: "healthcare_affordability", stance: "for" },
        { researchAreaSlug: "cost_of_living_reduction", stance: "against" },
      ],
      researchUrls: ["https://example.org/measure.pdf"],
      aiRawDebug: null,
    });
    mocks.poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            id: "election-1",
            district_id: "district-1",
            district_name: "Los Angeles County, California",
            district_type: "county",
            state: "CA",
            election_date: "2026-06-02",
            official_ballot_title: "Measure H",
            sources: ["https://example.org/measure.pdf"],
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({
        rows: [
          { id: "ra-health", slug: "healthcare_affordability" },
          { id: "ra-cost", slug: "cost_of_living_reduction" },
        ],
      });
    mocks.clientQueryMock.mockImplementation(async (sql: string) => {
      const text = String(sql);
      if (text.includes("INSERT INTO public.ballot_measures")) {
        return { rowCount: 1, rows: [{ id: "ballot-measure-1" }] };
      }
      return { rowCount: 1, rows: [] };
    });
  });

  it("persists ballot-measure research-area tags from the AI result", async () => {
    await runBallotMeasuresEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(mocks.enrichBallotMeasureMock).toHaveBeenCalledWith(
      expect.objectContaining({
        officialBallotTitle: "Measure H",
        allowedResearchAreaSlugs: ["healthcare_affordability", "cost_of_living_reduction"],
      }),
      expect.objectContaining({ timeoutMs: 90000 })
    );

    const ballotMeasureUpsert = mocks.clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.ballot_measures")
    );
    expect(ballotMeasureUpsert).toBeTruthy();
    expect(String(ballotMeasureUpsert?.[0])).toContain("ON CONFLICT (election_id)");
    expect(String(ballotMeasureUpsert?.[0])).toContain("RETURNING id");
    expect(String(ballotMeasureUpsert?.[0])).toContain("research_area_tags_researched_at");

    const tagUpserts = mocks.clientQueryMock.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.ballot_measure_research_area_tags")
    );
    expect(tagUpserts).toHaveLength(2);
    expect(tagUpserts[0]?.[1]).toEqual(["ballot-measure-1", "ra-health", "for"]);
    expect(tagUpserts[1]?.[1]).toEqual(["ballot-measure-1", "ra-cost", "against"]);
    expect(mocks.redisXAckMock).toHaveBeenCalledWith(
      STAGING_BALLOT_MEASURE_DRAFT_STREAM,
      STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
      "1-0"
    );
  });

  it("skips existing ballot measures whose research-area tags were already researched", async () => {
    mocks.poolQueryMock.mockReset();
    mocks.poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            id: "election-1",
            district_id: "district-1",
            district_name: "Los Angeles County, California",
            district_type: "county",
            state: "CA",
            election_date: "2026-06-02",
            official_ballot_title: "Measure H",
            sources: [],
          },
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          {
            id: "ballot-measure-1",
            research_area_tags_researched_at: new Date("2026-06-07T00:00:00.000Z"),
          },
        ],
      });

    await runBallotMeasuresEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(mocks.enrichBallotMeasureMock).not.toHaveBeenCalled();
    expect(mocks.clientQueryMock).not.toHaveBeenCalled();
    expect(mocks.redisXAckMock).toHaveBeenCalledWith(
      STAGING_BALLOT_MEASURE_DRAFT_STREAM,
      STAGING_BALLOT_MEASURE_ENRICHER_GROUP,
      "1-0"
    );
  });
});
