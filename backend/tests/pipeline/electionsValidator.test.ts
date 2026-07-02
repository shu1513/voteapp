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
  STAGING_DRAFT_STREAM,
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

  it("accepts 'United States Representative' as a clear us_house title without a review pass", async () => {
    const payload = {
      district_id: "d-ak",
      district_name: "Congressional District (at Large) (119th Congress), Alaska",
      district_type: "us_house",
      state: "AK",
      entries: [
        {
          official_ballot_title: "United States Representative",
          election_date: "2099-08-18",
          race_type: "office",
          election_stage: "primary",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:ak",
            payload,
            status: "pending",
            run_id: "run_ak",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    const updateValidatedCall = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = 'validated'")
    );
    expect(updateValidatedCall).toBeTruthy();
    const softFailCall = poolQueryMock.mock.calls.find((call) => String(call[1]?.[1] ?? "").includes("soft_fail"));
    expect(softFailCall).toBeUndefined();
  });

  it("accepts 'Judge of the Superior Court, Office No. 64' as a clear county title without a review pass", async () => {
    const payload = {
      district_id: "d-la",
      district_name: "Los Angeles County, California",
      district_type: "county",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Judge of the Superior Court, Office No. 64",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "runoff",
          is_partisan: false,
          discovery_contest_family: "judicial_office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:la",
            payload,
            status: "pending",
            run_id: "run_la",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    const updateValidatedCall = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = 'validated'")
    );
    expect(updateValidatedCall).toBeTruthy();
    const softFailCall = poolQueryMock.mock.calls.find((call) => String(call[1]?.[1] ?? "").includes("soft_fail"));
    expect(softFailCall).toBeUndefined();
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

  it("soft-retries when standard election discovery returns only presidential contests", async () => {
    const payload = {
      district_id: "d-president",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "President and Vice President",
          election_date: "2099-11-03",
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
            run_id: "run_president",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_DRAFT_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:1",
        item_type: STAGING_ITEM_TYPE_ELECTION,
      })
    );

    const rejectedCall = redisXAddMock.mock.calls.find((call) => call[0] === STAGING_REJECTED_STREAM);
    expect(rejectedCall).toBeUndefined();

    expect(redisXAddMock).not.toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:1",
      })
    );
  });

  it("drops presidential contests while validating remaining statewide offices", async () => {
    const payload = {
      district_id: "d-president-mixed",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2099-11-03",
          race_type: "office",
          sources: ["https://example.org/election/governor"],
        },
        {
          official_ballot_title: "President and Vice President",
          election_date: "2099-11-03",
          race_type: "office",
          sources: ["https://example.org/election/president"],
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
            run_id: "run_president_mixed",
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

    const validatedCall = redisXAddMock.mock.calls.find((call) => call[0] === STAGING_VALIDATED_STREAM);
    const validatedPayload = JSON.parse(String((validatedCall?.[2] as Record<string, string> | undefined)?.payload));
    expect(validatedPayload.entries).toEqual([
      expect.objectContaining({ official_ballot_title: "Governor" }),
    ]);

    const updateValidatedCall = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("payload = $3::jsonb")
    );
    expect(updateValidatedCall).toBeTruthy();
    const storedPayload = JSON.parse(String(updateValidatedCall?.[1]?.[2]));
    expect(storedPayload.entries).toEqual([
      expect.objectContaining({ official_ballot_title: "Governor" }),
    ]);

    const rejectedCall = redisXAddMock.mock.calls.find((call) => call[0] === STAGING_REJECTED_STREAM);
    expect(rejectedCall).toBeUndefined();
  });

  it("accepts state_upper entries with legislature-style upper chamber titles", async () => {
    const payload = {
      district_id: "d-4",
      district_name: "Nebraska Legislative District 12",
      district_type: "state_upper",
      state: "NE",
      entries: [
        {
          official_ballot_title: "Member of the Legislature, District 12",
          election_date: "2099-11-03",
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
            run_id: "run_4",
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

  it("accepts state_lower entries with lower chamber alias titles", async () => {
    const payload = {
      district_id: "d-5",
      district_name: "Massachusetts State House District 7",
      district_type: "state_lower",
      state: "MA",
      entries: [
        {
          official_ballot_title: "Representative in General Court, District 7",
          election_date: "2099-11-03",
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
            run_id: "run_5",
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

  it("hard-rejects state_upper entries that look like lower chamber races", async () => {
    const payload = {
      district_id: "d-6",
      district_name: "California State Senate District 12",
      district_type: "state_upper",
      state: "CA",
      entries: [
        {
          official_ballot_title: "State Representative, District 12",
          election_date: "2099-11-03",
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
            run_id: "run_6",
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

  it("soft-fails ambiguous general-assembly wording in state_upper instead of hard-rejecting it", async () => {
    const payload = {
      district_id: "d-7",
      district_name: "State Legislative District 12",
      district_type: "state_upper",
      state: "NJ",
      entries: [
        {
          official_ballot_title: "Member of the General Assembly, District 12",
          election_date: "2099-11-03",
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
            run_id: "run_7",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_DRAFT_STREAM,
      "*",
      expect.objectContaining({
        ingest_key: "elections:test:1",
        item_type: STAGING_ITEM_TYPE_ELECTION,
      })
    );

    const rejectedCall = redisXAddMock.mock.calls.find((call) => call[0] === STAGING_REJECTED_STREAM);
    expect(rejectedCall).toBeUndefined();
  });

  it("hard-rejects non-statewide entries that look like U.S. Senate contests", async () => {
    const payload = {
      district_id: "d-8",
      district_name: "Los Angeles County",
      district_type: "county",
      state: "CA",
      entries: [
        {
          official_ballot_title: "United States Senator",
          election_date: "2099-11-03",
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
            run_id: "run_8",
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
});
