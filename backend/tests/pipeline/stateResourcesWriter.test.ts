import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});
const poolConnectMock = vi.fn();

const redisConnectMock = vi.fn(async () => {});
const redisQuitMock = vi.fn(async () => {});
const redisXGroupCreateMock = vi.fn(async () => "OK");
const redisXAutoClaimMock = vi.fn();
const redisXReadGroupMock = vi.fn();
const redisXAckMock = vi.fn(async () => 1);
const redisXAddMock = vi.fn(async () => "1-0");

const observerRecordMock = vi.fn();
const observerFlushMock = vi.fn();

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

vi.mock("../../src/pipeline/utils/observability.js", () => {
  return {
    createStageObserver: () => ({
      record: observerRecordMock,
      flush: observerFlushMock,
    }),
  };
});

import { runStateResourcesWriter } from "../../src/pipeline/writers/stateResourcesWriter.js";
import {
  STAGING_ITEM_TYPE_STATE_RESOURCES,
  STAGING_STATE_RESOURCES_WRITER_GROUP,
  STAGING_VALIDATED_STREAM,
  STAGING_WRITTEN_STREAM,
} from "../../src/config/stateResourcePipeline.js";

describe("runStateResourcesWriter", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    redisXAutoClaimMock
      .mockResolvedValueOnce({
        nextId: "2-0",
        messages: [
          {
            id: "2-0",
            message: {
              ingest_key: "state_resources:06:2099",
              item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
              run_id: "run_1",
            },
          },
        ],
      })
      .mockResolvedValue({
        nextId: "0-0",
        messages: [],
      });

    redisXReadGroupMock.mockResolvedValue(null);

    poolQueryMock.mockImplementation(async (query: unknown) => {
      const sql = String(query);

      if (sql.includes("SELECT ingest_key, item_type, run_id, schema_version, model, prompt_version, reason, payload, status")) {
        return {
          rows: [
            {
              ingest_key: "state_resources:06:2099",
              item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
              run_id: "run_1",
              schema_version: "state_resources_enrichment_v4",
              model: "openai:gpt-5.4-mini",
              prompt_version: "state_resources_v2",
              reason: null,
              payload: {},
              status: "written",
            },
          ],
        };
      }

      return { rowCount: 1, rows: [] };
    });
  });

  it("reclaims written rows and republishes handoff without DB upsert path", async () => {
    await runStateResourcesWriter({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXGroupCreateMock).toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      STAGING_STATE_RESOURCES_WRITER_GROUP,
      "0",
      { MKSTREAM: true }
    );

    expect(redisXAutoClaimMock).toHaveBeenCalled();

    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_WRITTEN_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "state_resources:06:2099",
        item_type: STAGING_ITEM_TYPE_STATE_RESOURCES,
      })
    );

    expect(redisXAckMock).toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      STAGING_STATE_RESOURCES_WRITER_GROUP,
      "2-0"
    );

    expect(poolConnectMock).not.toHaveBeenCalled();
  });
});
