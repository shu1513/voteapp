import { afterEach, describe, expect, it, vi } from "vitest";

import type { AddressResolvedDistrict } from "../../../src/pipeline/address/addressDistrictLookup.js";
import {
  claimNextManualDistrictResearchRequest,
  enqueueManualDistrictResearchRequestsForStaleDistricts,
  markManualDistrictResearchRequestFailed,
  markManualDistrictResearchRequestRunning,
  markManualDistrictResearchRequestSucceeded,
  releaseManualDistrictResearchRequest,
  releaseStaleManualDistrictResearchClaims,
  MANUAL_RESEARCH_MAX_ATTEMPTS,
} from "../../../src/pipeline/address/manualDistrictResearchRequests.js";

const DISTRICT_ID = "11111111-1111-4111-8111-111111111111";
const OTHER_DISTRICT_ID = "22222222-2222-4222-8222-222222222222";
const REQUEST_ID = "33333333-3333-4333-8333-333333333333";

function makeDistrict(overrides: Partial<AddressResolvedDistrict> = {}): AddressResolvedDistrict {
  return {
    id: DISTRICT_ID,
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

function makeClaimRow(overrides: Record<string, unknown> = {}) {
  return {
    id: REQUEST_ID,
    district_id: DISTRICT_ID,
    district_name_snapshot: "Los Angeles County",
    district_type_snapshot: "county",
    state_snapshot: "CA",
    request_count: 3,
    trigger_source: "address_resolve",
    last_elections_searched_at_at_request: null,
    district_last_searched_at: null,
    ...overrides,
  };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("enqueueManualDistrictResearchRequestsForStaleDistricts", () => {
  it("returns an empty result without queries for an empty district list", async () => {
    const query = vi.fn();

    const result = await enqueueManualDistrictResearchRequestsForStaleDistricts(
      { query },
      { districts: [], triggerSource: "address_resolve", cooldownDays: 180 }
    );

    expect(result).toEqual({ checked: 0, enqueued: [], bumped: [], skipped_fresh: 0, failed: 0 });
    expect(query).not.toHaveBeenCalled();
  });

  it("enqueues a stale district and binds snapshots, source, and freshness", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id, last_elections_searched_at: null }] })
      .mockResolvedValueOnce({ rows: [{ request_count: 1 }] });
    vi.spyOn(console, "log").mockImplementation(() => {});

    const result = await enqueueManualDistrictResearchRequestsForStaleDistricts(
      { query },
      { districts: [district], triggerSource: "me_address_update", cooldownDays: 180 }
    );

    expect(result).toEqual({
      checked: 1,
      enqueued: [district.id],
      bumped: [],
      skipped_fresh: 0,
      failed: 0,
    });

    // Staleness query binds the district ids and the cooldown.
    expect(query.mock.calls[0]?.[1]).toEqual([[district.id], 180]);

    // Upsert binds district id, snapshots, trigger source, freshness-at-request.
    const upsertParams = query.mock.calls[1]?.[1] as unknown[];
    expect(upsertParams).toEqual([
      district.id,
      district.name,
      district.district_type,
      district.state,
      "me_address_update",
      null,
    ]);
  });

  it("counts a repeat request for an open row as bumped, not enqueued", async () => {
    const district = makeDistrict();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: district.id, last_elections_searched_at: null }] })
      .mockResolvedValueOnce({ rows: [{ request_count: 4 }] });
    vi.spyOn(console, "log").mockImplementation(() => {});

    const result = await enqueueManualDistrictResearchRequestsForStaleDistricts(
      { query },
      { districts: [district], triggerSource: "address_resolve", cooldownDays: 180 }
    );

    expect(result.enqueued).toEqual([]);
    expect(result.bumped).toEqual([district.id]);
  });

  it("skips fresh districts without inserting", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    const result = await enqueueManualDistrictResearchRequestsForStaleDistricts(
      { query },
      { districts: [makeDistrict()], triggerSource: "address_resolve", cooldownDays: 180 }
    );

    expect(result).toEqual({ checked: 1, enqueued: [], bumped: [], skipped_fresh: 1, failed: 0 });
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("counts a failed insert without throwing and continues with other districts", async () => {
    const districtA = makeDistrict();
    const districtB = makeDistrict({ id: OTHER_DISTRICT_ID, name: "Cook County", state: "IL" });
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          { id: districtA.id, last_elections_searched_at: null },
          { id: districtB.id, last_elections_searched_at: null },
        ],
      })
      .mockRejectedValueOnce(new Error("insert exploded"))
      .mockResolvedValueOnce({ rows: [{ request_count: 1 }] });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});

    const result = await enqueueManualDistrictResearchRequestsForStaleDistricts(
      { query },
      { districts: [districtA, districtB], triggerSource: "address_resolve", cooldownDays: 180 }
    );

    expect(result.failed).toBe(1);
    expect(result.enqueued).toEqual([districtB.id]);
  });

  it("never throws when the staleness query itself fails", async () => {
    const query = vi.fn().mockRejectedValueOnce(new Error("db down"));
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const result = await enqueueManualDistrictResearchRequestsForStaleDistricts(
      { query },
      { districts: [makeDistrict()], triggerSource: "address_resolve", cooldownDays: 180 }
    );

    expect(result).toEqual({ checked: 1, enqueued: [], bumped: [], skipped_fresh: 0, failed: 1 });
  });
});

describe("claimNextManualDistrictResearchRequest", () => {
  it("returns null when no queued rows remain", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    const claimed = await claimNextManualDistrictResearchRequest(
      { query },
      { claimedBy: "claude-session", agentKind: "claude", cooldownDays: 180 }
    );

    expect(claimed).toBeNull();
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]).toEqual(["claude-session", "claude"]);
  });

  it("returns a claimable request for a still-stale district", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [makeClaimRow()] });

    const claimed = await claimNextManualDistrictResearchRequest(
      { query },
      { claimedBy: "claude-session", agentKind: "claude", cooldownDays: 180 }
    );

    expect(claimed).toEqual({
      request_id: REQUEST_ID,
      district_id: DISTRICT_ID,
      district_name: "Los Angeles County",
      district_type: "county",
      state: "CA",
      request_count: 3,
      trigger_source: "address_resolve",
      last_elections_searched_at_at_request: null,
    });
  });

  it("marks a now-fresh district's request skipped and claims the next stale one", async () => {
    const freshRow = makeClaimRow({ district_last_searched_at: new Date().toISOString() });
    const staleRow = makeClaimRow({
      id: "44444444-4444-4444-8444-444444444444",
      district_id: OTHER_DISTRICT_ID,
      district_last_searched_at: null,
    });
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [freshRow] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [] })
      .mockResolvedValueOnce({ rows: [staleRow] });

    const claimed = await claimNextManualDistrictResearchRequest(
      { query },
      { claimedBy: "codex-session", agentKind: "codex", cooldownDays: 180 }
    );

    expect(claimed?.district_id).toBe(OTHER_DISTRICT_ID);
    // Second call marks the fresh row skipped.
    const skipSql = query.mock.calls[1]?.[0] as string;
    expect(skipSql).toContain("'skipped'");
    expect(query.mock.calls[1]?.[1]).toEqual([REQUEST_ID]);
  });

  it("treats a district researched longer ago than the cooldown as still stale", async () => {
    const twoHundredDaysAgo = new Date(Date.now() - 200 * 24 * 60 * 60 * 1000).toISOString();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [makeClaimRow({ district_last_searched_at: twoHundredDaysAgo })] });

    const claimed = await claimNextManualDistrictResearchRequest(
      { query },
      { claimedBy: "claude-session", agentKind: "claude", cooldownDays: 180 }
    );

    expect(claimed?.request_id).toBe(REQUEST_ID);
  });

  it("stops scanning at maxSkipScan even if fresh rows keep coming", async () => {
    const query = vi
      .fn()
      .mockResolvedValue({ rows: [makeClaimRow({ district_last_searched_at: new Date().toISOString() })] });

    const claimed = await claimNextManualDistrictResearchRequest(
      { query },
      { claimedBy: "claude-session", agentKind: "claude", cooldownDays: 180, maxSkipScan: 3 }
    );

    expect(claimed).toBeNull();
    // 3 scans x (claim + skip) = 6 queries.
    expect(query).toHaveBeenCalledTimes(6);
  });
});

describe("status transitions", () => {
  it("markRunning increments attempt_count and requires claimed status", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1 });

    const ok = await markManualDistrictResearchRequestRunning({ query }, REQUEST_ID);

    expect(ok).toBe(true);
    const sql = query.mock.calls[0]?.[0] as string;
    expect(sql).toContain("attempt_count = attempt_count + 1");
    expect(sql).toContain("status = 'claimed'");
  });

  it("markSucceeded stores the manifest path and reports false when the row is not open", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 0 });

    const ok = await markManualDistrictResearchRequestSucceeded(
      { query },
      { requestId: REQUEST_ID, manifestPath: "~/runs/la-county/manifest.md" }
    );

    expect(ok).toBe(false);
    expect(query.mock.calls[0]?.[1]).toEqual([REQUEST_ID, "~/runs/la-county/manifest.md", null]);
  });

  it("markFailed truncates the error and targets open statuses", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1 });

    const ok = await markManualDistrictResearchRequestFailed(
      { query },
      { requestId: REQUEST_ID, error: "x".repeat(5000) }
    );

    expect(ok).toBe(true);
    const params = query.mock.calls[0]?.[1] as unknown[];
    expect((params[1] as string).length).toBe(4000);
  });

  it("release returns the request to queued and clears claim fields", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rowCount: 1 });

    const ok = await releaseManualDistrictResearchRequest(
      { query },
      { requestId: REQUEST_ID, note: "session ended" }
    );

    expect(ok).toBe(true);
    const sql = query.mock.calls[0]?.[0] as string;
    expect(sql).toContain("status = 'queued'");
    expect(sql).toContain("claimed_by = NULL");
  });

  it("stale-claim sweep parks max-attempt rows as failed and requeues the rest", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValueOnce({ rowCount: 2 });

    const result = await releaseStaleManualDistrictResearchClaims({ query }, { maxClaimHours: 6 });

    expect(result).toEqual({ requeued: 2, parked_failed: 1 });
    // Park pass runs first and binds the attempt ceiling.
    expect(query.mock.calls[0]?.[1]).toEqual([6, MANUAL_RESEARCH_MAX_ATTEMPTS]);
    expect(query.mock.calls[1]?.[1]).toEqual([6]);
  });
});
