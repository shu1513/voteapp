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

  it("accepts 'Representative to the 120th United States Congress - District 2' as a clear us_house title without a review pass", async () => {
    const payload = {
      district_id: "d-co",
      district_name: "Congressional District 2 (119th Congress), Colorado",
      district_type: "us_house",
      state: "CO",
      entries: [
        {
          official_ballot_title: "Representative to the 120th United States Congress - District 2",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:co",
            payload,
            status: "pending",
            run_id: "run_co",
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

  it("accepts 'Assessor' as a clear county title without a review pass", async () => {
    const payload = {
      district_id: "d-la",
      district_name: "Los Angeles County, California",
      district_type: "county",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Assessor",
          election_date: "2099-06-02",
          race_type: "office",
          election_stage: "primary",
          is_partisan: false,
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:la-assessor",
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

  it("soft-fails 'Judge of the Superior Court' in a Pennsylvania county, where the Superior Court is statewide appellate", async () => {
    const payload = {
      district_id: "d-pa",
      district_name: "Allegheny County, Pennsylvania",
      district_type: "county",
      state: "PA",
      entries: [
        {
          official_ballot_title: "Judge of the Superior Court",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          discovery_contest_family: "judicial_office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:pa",
            payload,
            status: "pending",
            run_id: "run_pa",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    const softFailCall = poolQueryMock.mock.calls.find((call) => String(call[1]?.[1] ?? "").includes("soft_fail"));
    expect(softFailCall).toBeTruthy();
    const updateValidatedCall = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = 'validated'")
    );
    expect(updateValidatedCall).toBeUndefined();
  });

  it("soft-fails municipal assessor titles like 'Town Assessor' in county scope", async () => {
    const payload = {
      district_id: "d-ny",
      district_name: "Erie County, New York",
      district_type: "county",
      state: "NY",
      entries: [
        {
          official_ballot_title: "Town Assessor",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:ny",
            payload,
            status: "pending",
            run_id: "run_ny",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    const softFailCall = poolQueryMock.mock.calls.find((call) => String(call[1]?.[1] ?? "").includes("soft_fail"));
    expect(softFailCall).toBeTruthy();
    const updateValidatedCall = poolQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = 'validated'")
    );
    expect(updateValidatedCall).toBeUndefined();
  });

  it("accepts 'US House of Representatives District 1' without hard-rejecting it as a state-house race", async () => {
    const payload = {
      district_id: "d-nc",
      district_name: "Congressional District 1 (119th Congress), North Carolina",
      district_type: "us_house",
      state: "NC",
      entries: [
        {
          official_ballot_title: "US House of Representatives District 1",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: true,
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:nc",
            payload,
            status: "pending",
            run_id: "run_nc",
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
    const rejectedCall = redisXAddMock.mock.calls.find((call) => call[0] === STAGING_REJECTED_STREAM);
    expect(rejectedCall).toBeUndefined();
  });

  it("accepts Ohio's 'For Representative to Congress' phrasing as a clear us_house title", async () => {
    const payload = {
      district_id: "d-oh",
      district_name: "Congressional District 1 (119th Congress), Ohio",
      district_type: "us_house",
      state: "OH",
      entries: [
        {
          official_ballot_title: "For Representative to Congress (1st District)",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: true,
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:oh",
            payload,
            status: "pending",
            run_id: "run_oh",
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

  it("accepts 'District Attorney' as a clear county title without a review pass", async () => {
    const payload = {
      district_id: "d-bexar",
      district_name: "Bexar County, Texas",
      district_type: "county",
      state: "TX",
      entries: [
        {
          official_ballot_title: "District Attorney",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: true,
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:bexar",
            payload,
            status: "pending",
            run_id: "run_bexar",
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

  it("accepts the spelled-out 'United States House of Representatives District 1' title in us_house scope", async () => {
    const payload = {
      district_id: "d-nc2",
      district_name: "Congressional District 1 (119th Congress), North Carolina",
      district_type: "us_house",
      state: "NC",
      entries: [
        {
          official_ballot_title: "United States House of Representatives District 1",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: true,
          sources: ["https://example.org/election"],
        },
      ],
    };

    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:nc2",
            payload,
            status: "pending",
            run_id: "run_nc2",
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
    const rejectedCall = redisXAddMock.mock.calls.find((call) => call[0] === STAGING_REJECTED_STREAM);
    expect(rejectedCall).toBeUndefined();
  });

  it("hard-rejects 'District Attorney' in statewide scope as a county-like race", async () => {
    const payload = {
      district_id: "d-tx-sw",
      district_name: "Texas",
      district_type: "statewide",
      state: "TX",
      entries: [
        {
          official_ballot_title: "District Attorney",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: true,
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
            run_id: "run_txsw",
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
          discovery_contest_family: "us_senate",
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
          discovery_contest_family: "non_judicial_office",
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
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/election/governor"],
        },
        {
          official_ballot_title: "President and Vice President",
          election_date: "2099-11-03",
          race_type: "office",
          discovery_contest_family: "non_judicial_office",
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
          // Family present so this keeps exercising the SCOPE rejection, not
          // the missing-family rejection.
          discovery_contest_family: "non_judicial_office",
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

  it("accepts a '* County School District' board title in school scope without a review pass", async () => {
    const payload = {
      district_id: "d-nv-ccsd",
      district_name: "Clark County School District, Nevada",
      district_type: "school_unified",
      state: "NV",
      entries: [
        {
          official_ballot_title: "Clark County School District Board of Trustees, District D",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: false,
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
            run_id: "run_ccsd",
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

  it("accepts a '* City Schools' board title in school scope without a review pass", async () => {
    const payload = {
      district_id: "d-tn-mcs",
      district_name: "Murfreesboro City School District, Tennessee",
      district_type: "school_elementary",
      state: "TN",
      entries: [
        {
          official_ballot_title: "Murfreesboro City Schools Board of Education",
          election_date: "2099-08-06",
          race_type: "office",
          election_stage: "general",
          is_partisan: false,
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
            run_id: "run_mcs",
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

  it("accepts Arizona's official 'Governing Board Member' title in school scope without a review pass", async () => {
    const payload = {
      district_id: "d-az-puhsd",
      district_name: "Phoenix Union High School District, Arizona",
      district_type: "school_secondary",
      state: "AZ",
      entries: [
        {
          official_ballot_title: "Governing Board Member",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: false,
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
            run_id: "run_puhsd",
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

  it("accepts a plural '* City Schools' title without another board phrase in school scope", async () => {
    const payload = {
      district_id: "d-tn-mcs",
      district_name: "Murfreesboro City School District, Tennessee",
      district_type: "school_elementary",
      state: "TN",
      entries: [
        {
          official_ballot_title: "Murfreesboro City Schools Board Member, Zone 1",
          election_date: "2099-08-06",
          race_type: "office",
          election_stage: "general",
          is_partisan: false,
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
            run_id: "run_mcs_plural",
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

  it("still hard-rejects a county race without school markers in school scope", async () => {
    const payload = {
      district_id: "d-nv-ccsd",
      district_name: "Clark County School District, Nevada",
      district_type: "school_unified",
      state: "NV",
      entries: [
        {
          official_ballot_title: "Clark County Sheriff",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "general",
          is_partisan: true,
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
            run_id: "run_ccsd_sheriff",
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
  });

  it("validates a targeted ingest key without reading or acknowledging the pending stream", async () => {
    const ingestKey = "manual:elections:targeted:2026";
    const payload = {
      district_id: "d-targeted",
      district_name: "Targeted District",
      district_type: "place",
      state: "WA",
      entries: [],
    };
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: ingestKey,
            payload,
            status: "pending",
            run_id: "run_targeted",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1, rows: [] })
      .mockResolvedValueOnce({ rows: [{ status: "validated" }] });

    await runElectionsValidator({ ingestKey });

    expect(redisXAutoClaimMock).not.toHaveBeenCalled();
    expect(redisXReadGroupMock).not.toHaveBeenCalled();
    expect(redisXAckMock).not.toHaveBeenCalled();
    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      "*",
      expect.objectContaining({ ingest_key: ingestKey })
    );
  });

  it("republishes the validated-stream message for a redelivered row already marked validated", async () => {
    // A prior run died between UPDATE ... status='validated' and the XADD:
    // the redelivered pending message must rebuild the missing message, not
    // ack it away and strand the row.
    const payload = { district_id: "d-x", entries: [] };
    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:1",
          payload,
          status: "validated",
          run_id: "run_x",
          failure_debug: null,
          schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
        },
      ],
    });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(STAGING_VALIDATED_STREAM, "*", {
      ingest_key: "elections:test:1",
      item_type: STAGING_ITEM_TYPE_ELECTION,
      run_id: "run_x",
      payload: JSON.stringify(payload),
    });
    expect(redisXAckMock).toHaveBeenCalledWith(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, "1-0");
    const updateCall = poolQueryMock.mock.calls.find((call) => String(call[0]).includes("UPDATE"));
    expect(updateCall).toBeUndefined();
  });

  it("republishes the draft-stream message for a redelivered soft-fail requeue whose publish never landed", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:1",
          payload: { district_id: "d-x", entries: [] },
          status: "pending",
          run_id: "run_x",
          failure_debug: null,
          schema_version: "elections_draft_v1",
        },
      ],
    });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(STAGING_DRAFT_STREAM, "*", {
      ingest_key: "elections:test:1",
      item_type: STAGING_ITEM_TYPE_ELECTION,
      run_id: "run_x",
    });
    expect(redisXAckMock).toHaveBeenCalledWith(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, "1-0");
  });

  it("republishes the rejected-stream event for a redelivered row already marked rejected", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:1",
          payload: {},
          status: "rejected",
          run_id: "run_x",
          reason: "hard_fail: out of scope",
          failure_debug: null,
          schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
        },
      ],
    });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).toHaveBeenCalledWith(STAGING_REJECTED_STREAM, "*", {
      ingest_key: "elections:test:1",
      item_type: STAGING_ITEM_TYPE_ELECTION,
      run_id: "run_x",
      reason: "hard_fail: out of scope",
    });
    expect(redisXAckMock).toHaveBeenCalledWith(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, "1-0");
  });

  it("leaves the message unacked when the rejected-stream publish fails after the row was marked rejected", async () => {
    poolQueryMock
      .mockResolvedValueOnce({
        rows: [
          {
            ingest_key: "elections:test:1",
            payload: "not-a-valid-payload",
            status: "pending",
            run_id: "run_x",
            reason: null,
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      // UPDATE ... status='rejected' (hard-fail on unparseable payload)
      .mockResolvedValueOnce({ rowCount: 1 })
      // catch-path getStagingStatus: row already transitioned
      .mockResolvedValueOnce({ rows: [{ status: "rejected" }] });
    redisXAddMock.mockRejectedValueOnce(new Error("redis down"));

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAckMock).not.toHaveBeenCalled();
  });

  it("still acks terminal rows without republishing anything", async () => {
    poolQueryMock.mockResolvedValueOnce({
      rows: [
        {
          ingest_key: "elections:test:1",
          payload: {},
          status: "written",
          run_id: "run_x",
          failure_debug: null,
          schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
        },
      ],
    });

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAddMock).not.toHaveBeenCalled();
    expect(redisXAckMock).toHaveBeenCalledWith(STAGING_PENDING_STREAM, STAGING_ELECTIONS_VALIDATOR_GROUP, "1-0");
  });

  it("leaves the message unacked when the validated-stream publish fails after the row was marked validated", async () => {
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
            ingest_key: "elections:test:1",
            payload,
            status: "pending",
            run_id: "run_x",
            failure_debug: null,
            schema_version: ELECTION_ENRICHMENT_SCHEMA_VERSION,
          },
        ],
      })
      // UPDATE ... status='validated'
      .mockResolvedValueOnce({ rowCount: 1 })
      // catch-path getStagingStatus: row already transitioned
      .mockResolvedValueOnce({ rows: [{ status: "validated" }] });
    redisXAddMock.mockRejectedValueOnce(new Error("redis down"));

    await runElectionsValidator({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisXAckMock).not.toHaveBeenCalled();
  });
});
