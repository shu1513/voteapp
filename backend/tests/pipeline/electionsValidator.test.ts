import { beforeEach, describe, expect, it, vi } from "vitest";

const poolQueryMock = vi.fn();
const poolEndMock = vi.fn(async () => {});

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

import { runElectionsValidator } from "../../src/pipeline/validators/electionsValidator.js";
import {
  STAGING_ELECTIONS_VALIDATOR_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_PENDING_STREAM,
  STAGING_REJECTED_STREAM,
  STAGING_VALIDATED_STREAM,
} from "../../src/config/electionsPipeline.js";
import { ELECTION_ENRICHMENT_SCHEMA_VERSION } from "../../src/contracts/electionEnrichmentContract.js";

describe("runElectionsValidator", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    redisXReadGroupMock.mockResolvedValue([
      {
        name: STAGING_PENDING_STREAM,
        messages: [
          {
            id: "1-0",
            message: {
              ingest_key: "elections:test:1",
              item_type: STAGING_ITEM_TYPE_ELECTION,
            },
          },
        ],
      },
    ]);
  });

  it("accepts soft-fail entries on review pass when review_decision=approve", async () => {
    const payload = {
      district_id: "d-1",
      district_name: "California's 31st congressional district",
      district_type: "us_house",
      state: "CA",
      entries: [
        {
          official_ballot_title: "General Election",
          election_date: "2099-11-03",
          description: "General election.",
          race_type: "office",
          sources: ["https://example.org/election"],
        },
      ],
      review_decision: "approve",
      review_reason: "scope is acceptable",
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:1",
            payload,
            status: "pending",
            run_id: "run_1",
            failure_debug: { soft_retry_count: 1, validation_feedback: ["x"] },
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXGroupCreateMock).toHaveBeenCalledWith(
      STAGING_PENDING_STREAM,
      STAGING_ELECTIONS_VALIDATOR_GROUP,
      "0",
      { MKSTREAM: true }
    );

    const updateValidatedCall = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = 'validated'")
    );
    expect(updateValidatedCall).toBeTruthy();

    const requeueSoftRetryCall = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("schema_version = $4")
    );
    expect(requeueSoftRetryCall).toBeUndefined();

    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:1",
        item_type: STAGING_ITEM_TYPE_ELECTION,
      })
    );
  });

  it("hard-rejects statewide entries that look like state legislative races", async () => {
    const payload = {
      district_id: "d-2",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "State Senator, District 12",
          election_date: "2099-11-03",
          description: "State senate office contest.",
          race_type: "office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:1",
            payload,
            status: "pending",
            run_id: "run_2",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_REJECTED_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:1",
        item_type: STAGING_ITEM_TYPE_ELECTION,
      })
    );

    expect(redisXAddMock).not.toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:1",
      })
    );
  });

  it("accepts statewide entries for U.S. Senate contests", async () => {
    const payload = {
      district_id: "d-3",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "United States Senator",
          election_date: "2099-11-03",
          description: "Federal statewide office.",
          race_type: "office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:1",
            payload,
            status: "pending",
            run_id: "run_3",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:1",
        item_type: STAGING_ITEM_TYPE_ELECTION,
      })
    );

    const rejectedCall = redisXAddMock.mock.calls.find((call) => call[0] === STAGING_REJECTED_STREAM);
    expect(rejectedCall).toBeUndefined();
  });
});
