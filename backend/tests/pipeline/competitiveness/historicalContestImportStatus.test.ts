import { describe, expect, it, vi } from "vitest";

import { loadHistoricalContestImportStatus } from "../../../src/pipeline/competitiveness/historicalContestImportStatus.js";

describe("historicalContestImportStatus", () => {
  it("summarizes imported historical contest margins and verified source coverage", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          source: "MIT_2024",
          election_year: 2024,
          office_type: "US_PRESIDENT",
          district_type: "statewide",
          stale_after_redistricting: false,
          row_count: "51",
          latest_imported_at: new Date("2026-06-15T01:02:03.000Z"),
          source_urls: [
            "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-president-state.csv",
          ],
        },
        {
          source: "MIT_2024",
          election_year: 2024,
          office_type: "US_HOUSE",
          district_type: "us_house",
          stale_after_redistricting: false,
          row_count: "14",
          latest_imported_at: "2026-06-15T02:03:04.000Z",
          source_urls: ["https://github.com/MEDSL/2024-elections-official"],
        },
      ],
    });

    await expect(loadHistoricalContestImportStatus({ query })).resolves.toEqual({
      total_records: 65,
      groups: [
        {
          source: "MIT_2024",
          election_year: 2024,
          office_type: "US_PRESIDENT",
          district_type: "statewide",
          stale_after_redistricting: false,
          row_count: 51,
          latest_imported_at: "2026-06-15T01:02:03.000Z",
          source_urls: [
            "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-president-state.csv",
          ],
        },
        {
          source: "MIT_2024",
          election_year: 2024,
          office_type: "US_HOUSE",
          district_type: "us_house",
          stale_after_redistricting: false,
          row_count: 14,
          latest_imported_at: "2026-06-15T02:03:04.000Z",
          source_urls: ["https://github.com/MEDSL/2024-elections-official"],
        },
      ],
      verified_sources: [
        {
          preset: "medsl-2024-president-state",
          source: "MIT_2024",
          source_url: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-president-state.csv",
          format: "medsl_aggregate_csv",
          election_year: 2024,
          office_type: "US_PRESIDENT",
          imported: true,
          row_count: 51,
          latest_imported_at: "2026-06-15T01:02:03.000Z",
        },
        {
          preset: "medsl-2024-senate-state",
          source: "MIT_2024",
          source_url: "https://raw.githubusercontent.com/MEDSL/2024-elections-official/main/2024-senate-state.csv",
          format: "medsl_aggregate_csv",
          election_year: 2024,
          office_type: "US_SENATE",
          imported: false,
          row_count: 0,
          latest_imported_at: null,
        },
      ],
    });

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("FROM public.historical_contest_margins");
  });

  it("rejects invalid row counts", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          source: "MIT_2024",
          election_year: 2024,
          office_type: "US_PRESIDENT",
          district_type: "statewide",
          stale_after_redistricting: false,
          row_count: "not-a-number",
          latest_imported_at: null,
          source_urls: [],
        },
      ],
    });

    await expect(loadHistoricalContestImportStatus({ query })).rejects.toThrow(
      "Invalid historical contest import status row count: not-a-number"
    );
  });
});
