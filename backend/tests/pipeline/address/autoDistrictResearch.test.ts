import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { AddressResolvedDistrict } from "../../../src/pipeline/address/addressDistrictLookup.js";
import {
  createAutoDistrictResearchTrigger,
  readAutoDistrictResearchConfigFromEnv,
  type AutoDistrictResearchConfig,
  type AutoDistrictResearchRedis,
} from "../../../src/pipeline/address/autoDistrictResearch.js";

const FIXED_NOW = new Date("2026-07-03T12:00:00.000Z");
const EXPECTED_RUN_ID = `auto_district_research_${FIXED_NOW.toISOString()}`;

function makeDistrict(overrides: Partial<AddressResolvedDistrict> = {}): AddressResolvedDistrict {
  return {
    id: "11111111-1111-4111-8111-111111111111",
    district_type: "county",
    geoid_compact: "06037",
    name: "Los Angeles County",
    state: "CA",
    state_fips: "06",
    population: 9876482,
    representation_power_score: null,
    ...overrides,
  };
}

function makeConfig(overrides: Partial<AutoDistrictResearchConfig> = {}): AutoDistrictResearchConfig {
  return { enabled: true, ttlDays: 180, ...overrides };
}

function makeRedis(overrides: Partial<AutoDistrictResearchRedis> = {}): AutoDistrictResearchRedis {
  return { isOpen: true, xAdd: vi.fn().mockResolvedValue("1-0"), ...overrides };
}

describe("createAutoDistrictResearchTrigger", () => {
  it("is a no-op with zero queries when the flag is off", async () => {
    const query = vi.fn();
    const redis = makeRedis();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig({ enabled: false }),
      now: () => FIXED_NOW,
    });

    const result = await trigger([makeDistrict()]);

    expect(result).toEqual({ checked: 0, enqueued: [], skipped_fresh: 0, skipped_claimed: 0, failed: 0 });
    expect(query).not.toHaveBeenCalled();
    expect(redis.xAdd).not.toHaveBeenCalled();
  });

  it("is a no-op when there are no districts", async () => {
    const query = vi.fn();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => makeRedis(),
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([]);

    expect(result.checked).toBe(0);
    expect(query).not.toHaveBeenCalled();
  });

  it("bails before any db writes when redis is unavailable", async () => {
    const query = vi.fn();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => null,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([makeDistrict()]);

    expect(result).toEqual({ checked: 1, enqueued: [], skipped_fresh: 0, skipped_claimed: 0, failed: 0 });
    expect(query).not.toHaveBeenCalled();
  });

  it("enqueues an unresearched district: staging upsert plus draft stream XADD", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id }] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [{ ingest_key: `elections:${district.id}:2026` }] });
    const redis = makeRedis();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([district]);

    expect(result).toEqual({
      checked: 1,
      enqueued: [district.id],
      skipped_fresh: 0,
      skipped_claimed: 0,
      failed: 0,
    });

    // Staleness query binds the district ids and the TTL.
    expect(query.mock.calls[0]?.[1]).toEqual([[district.id], 180]);

    // Upsert binds ingest key, item type, draft payload, and the auto run id.
    const upsertParams = query.mock.calls[1]?.[1] as unknown[];
    expect(upsertParams[0]).toBe(`elections:${district.id}:2026`);
    expect(upsertParams[1]).toBe("election");
    expect(JSON.parse(upsertParams[2] as string)).toEqual({
      district_id: district.id,
      district_name: district.name,
      district_type: district.district_type,
      state: district.state,
    });
    expect(upsertParams[3]).toBe(EXPECTED_RUN_ID);

    expect(redis.xAdd).toHaveBeenCalledTimes(1);
    expect(redis.xAdd).toHaveBeenCalledWith("staging:elections:draft", "*", {
      ingest_key: `elections:${district.id}:2026`,
      item_type: "election",
      run_id: EXPECTED_RUN_ID,
      payload: JSON.stringify({
        district_id: district.id,
        district_name: district.name,
        district_type: district.district_type,
        state: district.state,
      }),
    });
  });

  it("skips fresh districts without touching staging", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });
    const redis = makeRedis();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([makeDistrict()]);

    expect(result).toEqual({ checked: 1, enqueued: [], skipped_fresh: 1, skipped_claimed: 0, failed: 0 });
    expect(query).toHaveBeenCalledTimes(1);
    expect(redis.xAdd).not.toHaveBeenCalled();
  });

  it("binds a custom ttlDays into the staleness query", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => makeRedis(),
      config: makeConfig({ ttlDays: 90 }),
      now: () => FIXED_NOW,
    });

    await trigger([makeDistrict()]);

    expect(query.mock.calls[0]?.[1]?.[1]).toBe(90);
  });

  it("counts an in-flight claim (upsert rowCount 0) and skips the XADD", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id }] })
      .mockResolvedValueOnce({ rowCount: 0, rows: [] });
    const redis = makeRedis();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([district]);

    expect(result).toEqual({ checked: 1, enqueued: [], skipped_fresh: 0, skipped_claimed: 1, failed: 0 });
    expect(redis.xAdd).not.toHaveBeenCalled();
  });

  it("counts a failed upsert without rejecting", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id }] })
      .mockRejectedValueOnce(new Error("insert exploded"));
    const redis = makeRedis();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([district]);

    expect(result).toEqual({ checked: 1, enqueued: [], skipped_fresh: 0, skipped_claimed: 0, failed: 1 });
    expect(redis.xAdd).not.toHaveBeenCalled();
  });

  it("counts a failed XADD without rejecting and without enqueueing the district", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id }] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [{ ingest_key: `elections:${district.id}:2026` }] });
    const redis = makeRedis({ xAdd: vi.fn().mockRejectedValue(new Error("stream down")) });
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([district]);

    expect(result).toEqual({ checked: 1, enqueued: [], skipped_fresh: 0, skipped_claimed: 0, failed: 1 });
  });

  it("releases a claimed staging row back to 'failed' when the XADD fails, so later runs can reclaim it", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id }] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [{ ingest_key: `elections:${district.id}:2026` }] })
      .mockResolvedValueOnce({ rowCount: 1 });
    const redis = makeRedis({ xAdd: vi.fn().mockRejectedValue(new Error("stream down")) });
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    await trigger([district]);

    const releaseCall = query.mock.calls.find((call) => String(call[0]).includes("SET status = 'failed'"));
    expect(releaseCall).toBeTruthy();
    expect(String(releaseCall?.[0])).toContain("AND status = 'pending'");
    expect(releaseCall?.[1]?.[0]).toBe(`elections:${district.id}:2026`);
    expect(String(releaseCall?.[1]?.[1])).toContain("stream down");
    expect(releaseCall?.[1]?.[2]).toBe(EXPECTED_RUN_ID);
  });

  it("does not release anything when the failure happened before the row was claimed", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id }] })
      .mockRejectedValueOnce(new Error("insert exploded"));
    const redis = makeRedis();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    await trigger([district]);

    const releaseCall = query.mock.calls.find((call) => String(call[0]).includes("SET status = 'failed'"));
    expect(releaseCall).toBeUndefined();
  });

  it("resolves with all districts failed when the staleness query throws", async () => {
    const query = vi.fn().mockRejectedValueOnce(new Error("db down"));
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => makeRedis(),
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([makeDistrict(), makeDistrict({ id: "22222222-2222-4222-8222-222222222222" })]);

    expect(result).toEqual({ checked: 2, enqueued: [], skipped_fresh: 0, skipped_claimed: 0, failed: 2 });
  });

  it("processes a mixed batch: one stale enqueued, one fresh skipped", async () => {
    const stale = makeDistrict();
    const fresh = makeDistrict({
      id: "22222222-2222-4222-8222-222222222222",
      district_type: "place",
      geoid_compact: "0644000",
      name: "Los Angeles city",
    });
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: stale.id }] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [{ ingest_key: `elections:${stale.id}:2026` }] });
    const redis = makeRedis();
    const trigger = createAutoDistrictResearchTrigger({
      db: { query },
      getRedis: () => redis,
      config: makeConfig(),
      now: () => FIXED_NOW,
    });

    const result = await trigger([stale, fresh]);

    expect(result).toEqual({
      checked: 2,
      enqueued: [stale.id],
      skipped_fresh: 1,
      skipped_claimed: 0,
      failed: 0,
    });
    expect(query.mock.calls[0]?.[1]).toEqual([[stale.id, fresh.id], 180]);
    expect(redis.xAdd).toHaveBeenCalledTimes(1);
  });
});

describe("readAutoDistrictResearchConfigFromEnv", () => {
  const ENV_KEYS = [
    "AUTO_DISTRICT_RESEARCH_ENABLED",
    "ELECTIONS_SEARCH_COOLDOWN_DAYS",
    "ELECTIONS_SEARCH_MAX_ENQUEUE_PER_RUN",
  ] as const;
  const saved: Record<string, string | undefined> = {};

  beforeEach(() => {
    for (const key of ENV_KEYS) {
      saved[key] = process.env[key];
      delete process.env[key];
    }
  });

  afterEach(() => {
    for (const key of ENV_KEYS) {
      if (saved[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = saved[key];
      }
    }
  });

  it("does not parse cooldown or rollover env when the feature is disabled", () => {
    process.env.ELECTIONS_SEARCH_COOLDOWN_DAYS = "not-a-number";
    process.env.ELECTIONS_SEARCH_MAX_ENQUEUE_PER_RUN = "also-bad";

    expect(readAutoDistrictResearchConfigFromEnv()).toEqual({ enabled: false, ttlDays: 180 });
  });

  it("never parses rollover-only env, even when enabled", () => {
    process.env.AUTO_DISTRICT_RESEARCH_ENABLED = "true";
    process.env.ELECTIONS_SEARCH_COOLDOWN_DAYS = "90";
    process.env.ELECTIONS_SEARCH_MAX_ENQUEUE_PER_RUN = "not-a-number";

    expect(readAutoDistrictResearchConfigFromEnv()).toEqual({ enabled: true, ttlDays: 90 });
  });

  it("still fails fast on an invalid cooldown when the feature is enabled", () => {
    process.env.AUTO_DISTRICT_RESEARCH_ENABLED = "true";
    process.env.ELECTIONS_SEARCH_COOLDOWN_DAYS = "not-a-number";

    expect(() => readAutoDistrictResearchConfigFromEnv()).toThrow(/ELECTIONS_SEARCH_COOLDOWN_DAYS/);
  });
});
