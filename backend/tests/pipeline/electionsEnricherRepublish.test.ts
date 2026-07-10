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

import { runElectionsEnricher } from "../../src/pipeline/enrichers/electionsEnricher.js";
import {
  STAGING_DRAFT_STREAM,
  STAGING_ELECTIONS_ENRICHER_GROUP,
  STAGING_ITEM_TYPE_ELECTION,
  STAGING_PENDING_STREAM,
} from "../../src/config/electionsPipeline.js";
import {
  ELECTION_DRAFT_SCHEMA_VERSION,
  ELECTION_ENRICHMENT_SCHEMA_VERSION,
} from "../../src/contracts/electionEnrichmentContract.js";

// The enricher's DB update (schema -> enrichment, status stays pending) and
// the pending-stream XADD are not atomic. A run that dies between them leaves
// the row transitioned with no stream message; the redelivered draft message
// must republish from the persisted row instead of acking it into a stranded
// state.
describe("runElectionsEnricher redelivery republish", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    redisXReadGroupMock.mockResolvedValue([
      {
        name: STAGING_DRAFT_STREAM,
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

  it("republishes the pending-stream message for a row already carrying the enrichment schema", async () => {
    const payload = { district_id: "d-x", entries: [] };
    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:1",
          payload,
          status: "pending",
          run_id: "run_x",
          schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          reason: null,
          failure_debug: null,
        },
      ],
    });

    await runElectionsEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(STAGING_PENDING_STREAM, "*", {
      ingest_key: "elections:test:1",
      item_type: STAGING_ITEM_TYPE_ELECTION,
      run_id: "run_x",
      payload: JSON.stringify(payload),
    });
    expect(redisXAckMock).toHaveBeenCalledWith(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, "1-0");
    const updateCall = poolQueryMock.mock.calls.find((call) => String(call[0]).includes("UPDATE"));
    expect(updateCall).toBeUndefined();
  });

  it("acks terminal rows without republishing", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:1",
          payload: {},
          status: "validated",
          run_id: "run_x",
          schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          reason: null,
          failure_debug: null,
        },
      ],
    });

    await runElectionsEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).not.toHaveBeenCalled();
    expect(redisXAckMock).toHaveBeenCalledWith(STAGING_DRAFT_STREAM, STAGING_ELECTIONS_ENRICHER_GROUP, "1-0");
  });

  it("leaves the message unacked when the republish itself fails", async () => {
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:1",
            payload: { district_id: "d-x", entries: [] },
            status: "pending",
            run_id: "run_x",
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
            reason: null,
            failure_debug: null,
          },
        ],
      })
      // catch-path getStagingStatus
      .mockResolvedValueOnce({ rows: [{ status: "pending" }] });
    redisXAddMock.mockRejectedValueOnce(new Error("redis down"));

    await runElectionsEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAckMock).not.toHaveBeenCalled();
  });

  it("does not republish rows still awaiting enrichment (draft schema falls through to the AI path)", async () => {
    // Draft-schema pending rows take the normal enrichment path; guard that
    // the republish branch does not fire for them. The AI call is not mocked,
    // so make the payload unparseable: the enricher then marks the row failed
    // before reaching the AI, which is enough to prove the gate let it through.
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:1",
            payload: "not-an-object",
            status: "pending",
            run_id: "run_x",
            schema_version: ELECTION_DRAFT_SCHEMA_VERSION,
            reason: null,
            failure_debug: null,
          },
        ],
      })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsEnricher({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).not.toHaveBeenCalled();
    const failedUpdate = poolQueryMock.mock.calls.find((call) => String(call[0]).includes("SET status = 'failed'"));
    expect(failedUpdate).toBeTruthy();
  });
});
