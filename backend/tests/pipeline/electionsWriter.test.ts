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

vi.mock("pg", () => {
  return {
    Pool: vi.fn(() => ({
      query: poolQueryMock,
      connect: poolConnectMock,
      end: poolEndMock,
    })),
  };
});

vi.mock("redis", () => {
  return {
    createClient: vi.fn(() => ({
      connect: redisConnectMock,
      quit: redisQuitMock,
      xGroupCreate: redisXGroupCreateMock,
      xAutoClaim: redisXAutoClaimMock,
      xReadGroup: redisXReadGroupMock,
      xAck: redisXAckMock,
      xAdd: redisXAddMock,
    })),
  };
});

vi.mock("../../src/config/env.js", () => {
  return {
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
  };
});

import { runElectionsWriter } from "../../src/pipeline/writers/electionsWriter.js";
import {
  STAGING_ELECTIONS_WRITER_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_VALIDATED_STREAM,
  STAGING_WRITTEN_STREAM,
} from "../../src/config/electionsPipeline.js";

describe("runElectionsWriter", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    poolConnectMock.mockResolvedValue({
      query: clientQueryMock,
      release: clientReleaseMock,
    });

    redisXReadGroupMock.mockResolvedValue([
      {
        name: STAGING_VALIDATED_STREAM,
        messages: [
          {
            id: "1-0",
            message: {
              ingest_key: "elections:test:writer",
              item_type: STAGING_ITEM_TYPE_ELECTION,
            },
          },
        ],
      },
    ]);
  });

  it("marks no_results and does not insert elections rows when entries is empty", async () => {
    const payload = {
      district_id: "d-1",
      district_name: "Vermont",
      district_type: "statewide",
      state: "VT",
      entries: [],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:writer",
            payload,
            status: "validated",
            run_id: "run_1",
          },
        ],
      })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    clientQueryMock.mockResolvedValue({ rowCount: 1 });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXGroupCreateMock).toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      STAGING_ELECTIONS_WRITER_GROUP,
      "0",
      { MKSTREAM: true }
    );

    const statusUpdateCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = $3")
    );
    expect(statusUpdateCall).toBeTruthy();
    expect(statusUpdateCall?.[1]?.[2]).toBe("no_results");

    const insertElectionCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.elections")
    );
    expect(insertElectionCall).toBeUndefined();

    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_WRITTEN_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:writer",
        item_type: STAGING_ITEM_TYPE_ELECTION,
      })
    );
  });

  it("uses upsert path for non-empty entries and never deletes district rows", async () => {
    const payload = {
      district_id: "d-1",
      district_name: "Vermont",
      district_type: "statewide",
      state: "VT",
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2099-11-03",
          description: "General election",
          race_type: "office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:writer",
            payload,
            status: "validated",
            run_id: "run_1",
          },
        ],
      })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    clientQueryMock.mockResolvedValue({ rowCount: 1 });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const deleteCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("DELETE FROM public.elections")
    );
    expect(deleteCall).toBeUndefined();

    const upsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("ON CONFLICT (district_id, official_ballot_title, election_date) DO UPDATE SET")
    );
    expect(upsertCall).toBeTruthy();

    const statusUpdateCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = $3")
    );
    expect(statusUpdateCall?.[1]?.[2]).toBe("written");
  });
});
