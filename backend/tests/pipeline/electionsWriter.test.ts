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
const redisSendCommandMock = vi.fn(async () => 1);

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
      sendCommand: redisSendCommandMock,
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
  STAGING_BALLOT_MEASURE_DRAFT_STREAM,
  STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
  STAGING_ELECTIONS_WRITER_GROUP,
  STAGING_ITEM_TYPE_BALLOT_MEASURE,
  STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
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
    const districtTimestampUpdateCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.districts")
    );
    expect(districtTimestampUpdateCall).toBeTruthy();
    expect(districtTimestampUpdateCall?.[1]?.[0]).toBe("d-1");

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
            ingest_key: "elections:test:writer",
            payload,
            status: "validated",
            run_id: "run_1",
          },
        ],
      })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000010", canonical_name: "Governor" }],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000011", race_type: "office", inserted: false }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const deleteCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("DELETE FROM public.elections")
    );
    expect(deleteCall).toBeUndefined();

    const upsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes(
        "ON CONFLICT (district_id, official_ballot_title_key, election_date) DO UPDATE SET"
      )
    );
    expect(upsertCall).toBeTruthy();
    expect(String(upsertCall?.[0])).toContain("is_partisan = COALESCE(EXCLUDED.is_partisan, elections.is_partisan)");
    // Reclassification to ballot_measure clears any stored office-era seat
    // count; otherwise the COALESCE preserves an existing value when a later
    // payload omits the field.
    expect(String(upsertCall?.[0])).toContain("WHEN EXCLUDED.race_type = 'ballot_measure' THEN NULL");
    expect(String(upsertCall?.[0])).toContain("ELSE COALESCE(EXCLUDED.seats_to_fill, elections.seats_to_fill)");
    expect(String(upsertCall?.[0])).toContain("discovery_contest_family");
    expect(String(upsertCall?.[0])).toContain("EXCLUDED.discovery_contest_family = 'us_senate'");
    expect(upsertCall?.[1]?.[5]).toBeNull();
    expect(upsertCall?.[1]?.[6]).toBeNull();
    expect(upsertCall?.[1]?.[7]).toBeNull();
    expect(upsertCall?.[1]?.[10]).toBe("non_judicial_office");

    const statusUpdateCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = $3")
    );
    expect(statusUpdateCall?.[1]?.[2]).toBe("written");
    const districtTimestampUpdateCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("UPDATE public.districts")
    );
    expect(districtTimestampUpdateCall).toBeTruthy();
    expect(districtTimestampUpdateCall?.[1]?.[0]).toBe("d-1");
  });

  it("writes the resolvable entries and an office-less shell when an office match is ambiguous", async () => {
    const payload = {
      district_id: "d-oregon",
      district_name: "Example County, Oregon",
      district_type: "county",
      state: "OR",
      entries: [
        {
          official_ballot_title: "Clerk",
          election_date: "2099-11-03",
          race_type: "office",
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/election"],
        },
        {
          official_ballot_title: "County Clerk",
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
            ingest_key: "elections:test:writer",
            payload,
            status: "validated",
            run_id: "run_1",
          },
        ],
      })
      .mockResolvedValue({ rowCount: 1, rows: [] });

    clientQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 2,
          rows: [
            { id: "00000000-0000-0000-0000-000000000021", canonical_name: "Clerk of Court" },
            { id: "00000000-0000-0000-0000-000000000022", canonical_name: "County Clerk" },
          ],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        const electionId =
          params?.[1] === "Clerk"
            ? "11111111-1111-4111-8111-111111111111"
            : "22222222-2222-4222-8222-222222222222";
        return {
          rowCount: 1,
          rows: [
            {
              id: electionId,
              race_type: "office",
              office_id: params?.[9] ?? null,
              inserted: true,
            },
          ],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    expect(clientQueryMock).not.toHaveBeenCalledWith("ROLLBACK");

    // The unmatched title becomes an office-less shell; the rest of the payload
    // is written normally.
    const electionInserts = clientQueryMock.mock.calls.filter((call) =>
      String(call[0]).includes("INSERT INTO public.elections")
    );
    expect(electionInserts).toHaveLength(2);
    expect(electionInserts[0]?.[1]?.[1]).toBe("Clerk");
    expect(electionInserts[0]?.[1]?.[9]).toBeNull();
    expect(electionInserts[1]?.[1]?.[1]).toBe("County Clerk");
    expect(electionInserts[1]?.[1]?.[9]).toBe("00000000-0000-0000-0000-000000000022");

    const statusUpdateCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("SET status = $3")
    );
    expect(statusUpdateCall?.[1]?.[2]).toBe("written");

    const reasonUpdateCall = clientQueryMock.mock.calls.find(
      (call) => String(call[0]).includes("SET reason = $2") && !String(call[0]).includes("status")
    );
    const reason = String(reasonUpdateCall?.[1]?.[1]);
    expect(reason).toContain(
      "writer office match unresolved: district_id=d-oregon scope=county unresolved=1"
    );
    expect(reason).toContain("method=ambiguous");
    expect(reason).toMatch(/confidence=\d+\.\d{3}/);
    expect(reason).toContain('title="Clerk" normalized_alias="clerk"');

    // Only the resolved election is handed to the candidate-roster stage: a
    // shell with no office_id would block the records stage downstream.
    const rosterEnqueueCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("'candidate_roster:' || office_id::text")
    );
    expect(rosterEnqueueCall?.[1]?.[2]).toEqual(["22222222-2222-4222-8222-222222222222"]);

    expect(redisXAckMock).toHaveBeenCalledWith(
      STAGING_VALIDATED_STREAM,
      STAGING_ELECTIONS_WRITER_GROUP,
      "1-0"
    );
    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_WRITTEN_STREAM,
      "*",
      expect.anything()
    );
  });

  it("enqueues ballot-measure and candidate-roster drafts via Lua sendCommand", async () => {
    const payload = {
      district_id: "d-1",
      district_name: "Vermont",
      district_type: "statewide",
      state: "VT",
      entries: [
        {
          official_ballot_title: "Governor",
          election_date: "2099-11-03",
          race_type: "office",
          discovery_contest_family: "non_judicial_office",
          sources: ["https://example.org/office"],
        },
        {
          official_ballot_title: "Measure A",
          election_date: "2099-11-03",
          race_type: "ballot_measure",
          discovery_contest_family: "ballot_measure",
          sources: ["https://example.org/measure"],
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

    clientQueryMock.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000303", canonical_name: "Governor" }],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        const raceType = String(params?.[4] ?? "");
        if (raceType === "office") {
          return {
            rowCount: 1,
            rows: [
              {
                id: "00000000-0000-0000-0000-000000000101",
                race_type: "office",
                office_id: params?.[9] ?? null,
              },
            ],
          };
        }
        if (raceType === "ballot_measure") {
          return {
            rowCount: 1,
            rows: [{ id: "00000000-0000-0000-0000-000000000202", race_type: "ballot_measure" }],
          };
        }
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    expect(redisSendCommandMock).toHaveBeenCalledTimes(2);

    const sendCommandCalls = redisSendCommandMock.mock.calls.map((call) => call[0] as string[]);

    expect(sendCommandCalls).toEqual(
      expect.arrayContaining([
        expect.arrayContaining([
          "EVAL",
          STAGING_BALLOT_MEASURE_DRAFT_STREAM,
          expect.stringContaining("staging:ballot_measure_emitted:"),
          "00000000-0000-0000-0000-000000000202",
          STAGING_ITEM_TYPE_BALLOT_MEASURE,
          "run_1",
        ]),
        expect.arrayContaining([
          "EVAL",
          STAGING_CANDIDATE_ROSTER_DRAFT_STREAM,
          expect.stringContaining("staging:candidate_roster_emitted:"),
          "00000000-0000-0000-0000-000000000101",
          STAGING_ITEM_TYPE_CANDIDATE_ROSTER,
          "run_1",
        ]),
      ])
    );
  });

  it("persists school-scope office titles as aliases for School Board Member", async () => {
    const payload = {
      district_id: "d-school",
      district_name: "Baldwin Park Unified School District",
      district_type: "school_unified",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Governing Board Trustee, Area 3",
          election_date: "2099-11-03",
          race_type: "office",
          sources: ["https://example.org/school-board"],
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

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000333", canonical_name: "School Board Member" }],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000444", race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const aliasInsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.office_title_aliases")
    );
    expect(aliasInsertCall).toBeTruthy();
    expect(aliasInsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-000000000333"]);
    expect(aliasInsertCall?.[1]?.[1]).toEqual(["school_unified"]);
    expect(aliasInsertCall?.[1]?.[2]).toEqual(["Governing Board Trustee, Area 3"]);
    expect(aliasInsertCall?.[1]?.[3]).toEqual(["governing board trustee area 3"]);
  });

  it("persists us_house office titles as aliases for United States Representative", async () => {
    const payload = {
      district_id: "d-us-house",
      district_name: "Congressional District 31 (119th Congress), California",
      district_type: "us_house",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Member of Congress, 31st District",
          election_date: "2099-11-03",
          race_type: "office",
          sources: ["https://example.org/us-house"],
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

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000555", canonical_name: "United States Representative" }],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000666", race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const aliasInsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.office_title_aliases")
    );
    expect(aliasInsertCall).toBeTruthy();
    expect(aliasInsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-000000000555"]);
    expect(aliasInsertCall?.[1]?.[1]).toEqual(["us_house"]);
    expect(aliasInsertCall?.[1]?.[2]).toEqual(["Member of Congress, 31st District"]);
    expect(aliasInsertCall?.[1]?.[3]).toEqual(["member of congress"]);
  });

  it("persists state_upper office titles as aliases for State Senator", async () => {
    const payload = {
      district_id: "d-state-upper",
      district_name: "California State Senate District 12",
      district_type: "state_upper",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Member of the Legislature, District 12",
          election_date: "2099-11-03",
          race_type: "office",
          sources: ["https://example.org/state-upper"],
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

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000556", canonical_name: "State Senator" }],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000667", race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const aliasInsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.office_title_aliases")
    );
    expect(aliasInsertCall).toBeTruthy();
    expect(aliasInsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-000000000556"]);
    expect(aliasInsertCall?.[1]?.[1]).toEqual(["state_upper"]);
    expect(aliasInsertCall?.[1]?.[2]).toEqual(["Member of the Legislature, District 12"]);
    expect(aliasInsertCall?.[1]?.[3]).toEqual(["member of the legislature"]);
  });

  it("persists state_lower office titles as aliases for State Lower Chamber Legislator", async () => {
    const payload = {
      district_id: "d-state-lower",
      district_name: "Massachusetts State House District 7",
      district_type: "state_lower",
      state: "MA",
      entries: [
        {
          official_ballot_title: "Representative in General Court, District 7",
          election_date: "2099-11-03",
          race_type: "office",
          sources: ["https://example.org/state-lower"],
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

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: "00000000-0000-0000-0000-000000000557",
              canonical_name: "State Lower Chamber Legislator",
            },
          ],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000668", race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const aliasInsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.office_title_aliases")
    );
    expect(aliasInsertCall).toBeTruthy();
    expect(aliasInsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-000000000557"]);
    expect(aliasInsertCall?.[1]?.[1]).toEqual(["state_lower"]);
    expect(aliasInsertCall?.[1]?.[2]).toEqual(["Representative in General Court, District 7"]);
    expect(aliasInsertCall?.[1]?.[3]).toEqual(["representative in general court"]);
  });

  it("persists clear statewide U.S. Senate titles as aliases for United States Senator", async () => {
    const payload = {
      district_id: "d-statewide",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "U.S. Senate (Special Election)",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "special",
          senate_class: "class_i",
          term_end_year: "2031",
          discovery_contest_family: "us_senate",
          sources: ["https://example.org/us-senate"],
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

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 2,
          rows: [
            { id: "00000000-0000-0000-0000-000000000777", canonical_name: "Governor" },
            { id: "00000000-0000-0000-0000-000000000888", canonical_name: "United States Senator" },
          ],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000999", race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const aliasInsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.office_title_aliases")
    );
    expect(aliasInsertCall).toBeTruthy();
    expect(aliasInsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-000000000888"]);
    expect(aliasInsertCall?.[1]?.[1]).toEqual(["statewide"]);
    expect(aliasInsertCall?.[1]?.[2]).toEqual(["U.S. Senate (Special Election)"]);
    expect(aliasInsertCall?.[1]?.[3]).toEqual(["united states senate special election"]);
  });

  it("uses us_senate family provenance to persist generic Senate titles as United States Senator aliases", async () => {
    const payload = {
      district_id: "d-statewide",
      district_name: "California",
      district_type: "statewide",
      state: "CA",
      entries: [
        {
          official_ballot_title: "Senator",
          election_date: "2099-11-03",
          race_type: "office",
          election_stage: "special",
          senate_class: "class_i",
          term_end_year: "2031",
          discovery_contest_family: "us_senate",
          sources: ["https://example.org/us-senate"],
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

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 2,
          rows: [
            { id: "00000000-0000-0000-0000-000000000777", canonical_name: "Governor" },
            { id: "00000000-0000-0000-0000-000000000888", canonical_name: "United States Senator" },
          ],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-000000000999", race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const aliasInsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.office_title_aliases")
    );
    expect(aliasInsertCall).toBeTruthy();
    expect(aliasInsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-000000000888"]);
    expect(aliasInsertCall?.[1]?.[1]).toEqual(["statewide"]);
    expect(aliasInsertCall?.[1]?.[2]).toEqual(["Senator"]);
    expect(aliasInsertCall?.[1]?.[3]).toEqual(["senator"]);

    const senateMetadataUpsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.election_senate_metadata")
    );
    expect(senateMetadataUpsertCall).toBeTruthy();
    expect(senateMetadataUpsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-000000000999"]);
    expect(senateMetadataUpsertCall?.[1]?.[1]).toEqual(["class_i"]);
    expect(senateMetadataUpsertCall?.[1]?.[2]).toEqual(["2031"]);
  });

  it("upserts senate metadata for U.S. Senate office entries", async () => {
    const payload = {
      district_id: "d-1",
      district_name: "Vermont",
      district_type: "statewide",
      state: "VT",
      entries: [
        {
          official_ballot_title: "United States Senator (Unexpired Term)",
          election_date: "2099-11-03",
          race_type: "office",
          senate_class: "class_i",
          term_end_year: "2031",
          discovery_contest_family: "us_senate",
          sources: ["https://example.org/senate"],
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

    clientQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM public.office_title_aliases")) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("FROM public.offices")) {
        return {
          rowCount: 1,
          rows: [
            {
              id: "00000000-0000-0000-0000-00000000DCBA",
              canonical_name: "United States Senator",
            },
          ],
        };
      }
      if (sql.includes("INSERT INTO public.elections")) {
        return {
          rowCount: 1,
          rows: [{ id: "00000000-0000-0000-0000-00000000ABCD", race_type: "office" }],
        };
      }
      return { rowCount: 1, rows: [] };
    });

    await runElectionsWriter({ once: true, batchSize: 5, blockMs: 10 });

    const senateMetadataUpsertCall = clientQueryMock.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.election_senate_metadata")
    );
    expect(senateMetadataUpsertCall).toBeTruthy();
    expect(senateMetadataUpsertCall?.[1]?.[0]).toEqual(["00000000-0000-0000-0000-00000000ABCD"]);
    expect(senateMetadataUpsertCall?.[1]?.[1]).toEqual(["class_i"]);
    expect(senateMetadataUpsertCall?.[1]?.[2]).toEqual(["2031"]);
  });

  it("writes a targeted ingest key without reading or acknowledging the validated stream", async () => {
    const ingestKey = "manual:elections:targeted:2026";
    const payload = {
      district_id: "d-targeted",
      district_name: "Targeted District",
      district_type: "place",
      state: "WA",
      entries: [],
    };
    let stagingReadCount = 0;
    poolQueryMock.mockImplementation(async (sql: string) => {
      if (sql.includes("FROM staging_items") && sql.includes("WHERE ingest_key")) {
        stagingReadCount += 1;
        return {
          rows: [
            {
              ingest_key: ingestKey,
              payload,
              status: stagingReadCount === 1 ? "validated" : "no_results",
              run_id: "run_targeted",
            },
          ],
        };
      }
      return { rowCount: 1, rows: [] };
    });
    clientQueryMock.mockResolvedValue({ rowCount: 1, rows: [] });

    await runElectionsWriter({ ingestKey });

    expect(redisXAutoClaimMock).not.toHaveBeenCalled();
    expect(redisXReadGroupMock).not.toHaveBeenCalled();
    expect(redisXAckMock).not.toHaveBeenCalled();
    expect(redisXAddMock).toHaveBeenCalledWith(
      STAGING_WRITTEN_STREAM,
      "*",
      expect.objectContaining({ ingest_key: ingestKey })
    );
  });
});
