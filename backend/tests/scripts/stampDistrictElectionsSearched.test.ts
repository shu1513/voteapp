import { describe, expect, it, vi } from "vitest";

import { runStampDistrictElectionsSearched } from "../../src/scripts/stampDistrictElectionsSearched.js";

const DISTRICT_ID = "11111111-1111-1111-1111-111111111111";

function districtRow(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: DISTRICT_ID,
    name: "Example CDP",
    district_type: "place",
    state: "WA",
    last_elections_searched_at: "2026-01-01 00:00:00+00",
    has_future_election: false,
    in_flight_staging_status: null,
    ...overrides,
  };
}

describe("runStampDistrictElectionsSearched", () => {
  it("stamps the district and reports the previous timestamp", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [districtRow()] })
      .mockResolvedValueOnce({ rowCount: 1, rows: [] });

    const result = await runStampDistrictElectionsSearched(
      { query },
      { districtId: DISTRICT_ID, dryRun: false }
    );

    expect(result.stamped).toBe(true);
    expect(result.previousLastElectionsSearchedAt).toBe("2026-01-01 00:00:00+00");
    expect(result.districtName).toBe("Example CDP");
    const updateCall = query.mock.calls[1];
    expect(String(updateCall?.[0])).toContain("SET last_elections_searched_at = now()");
    expect(updateCall?.[1]).toEqual([DISTRICT_ID]);
  });

  it("dry-run reports without updating", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [districtRow({ last_elections_searched_at: null })] });

    const result = await runStampDistrictElectionsSearched(
      { query },
      { districtId: DISTRICT_ID, dryRun: true }
    );

    expect(result.stamped).toBe(false);
    expect(result.dryRun).toBe(true);
    expect(result.previousLastElectionsSearchedAt).toBeNull();
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("refuses when the district has a future election row", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [districtRow({ has_future_election: true })] });

    await expect(
      runStampDistrictElectionsSearched({ query }, { districtId: DISTRICT_ID, dryRun: false })
    ).rejects.toThrow(/future election row/);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("refuses when an election staging row is still in flight", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [districtRow({ in_flight_staging_status: "pending" })] });

    await expect(
      runStampDistrictElectionsSearched({ query }, { districtId: DISTRICT_ID, dryRun: false })
    ).rejects.toThrow(/staging row in flight \(status=pending\)/);
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("throws for a missing district", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    await expect(
      runStampDistrictElectionsSearched({ query }, { districtId: DISTRICT_ID, dryRun: false })
    ).rejects.toThrow(/District not found/);
  });
});
