import { describe, expect, it, vi } from "vitest";

import { loadHistoricalContestImportStatus } from "../../../src/pipeline/competitiveness/historicalContestImportStatus.js";
import { VERIFIED_HISTORICAL_CONTEST_SOURCES } from "../../../src/pipeline/competitiveness/historicalContestSources.js";

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
        {
          source: "MIT_2022",
          election_year: 2022,
          office_type: "ATTORNEY_GENERAL",
          district_type: "statewide",
          stale_after_redistricting: false,
          row_count: "44",
          latest_imported_at: "2026-06-15T03:04:05.000Z",
          source_urls: ["https://doi.org/10.7910/DVN/UYQIEP"],
        },
        {
          source: "MIT_2022",
          election_year: 2022,
          office_type: "COUNTY_SHERIFF",
          district_type: "county",
          stale_after_redistricting: false,
          row_count: "7",
          latest_imported_at: "2026-06-15T04:05:06.000Z",
          source_urls: ["https://doi.org/10.7910/DVN/UYQIEP"],
        },
      ],
    });

    const status = await loadHistoricalContestImportStatus({ query });

    expect(status.total_records).toBe(116);
    expect(status.groups).toEqual([
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
      {
        source: "MIT_2022",
        election_year: 2022,
        office_type: "ATTORNEY_GENERAL",
        district_type: "statewide",
        stale_after_redistricting: false,
        row_count: 44,
        latest_imported_at: "2026-06-15T03:04:05.000Z",
        source_urls: ["https://doi.org/10.7910/DVN/UYQIEP"],
      },
      {
        source: "MIT_2022",
        election_year: 2022,
        office_type: "COUNTY_SHERIFF",
        district_type: "county",
        stale_after_redistricting: false,
        row_count: 7,
        latest_imported_at: "2026-06-15T04:05:06.000Z",
        source_urls: ["https://doi.org/10.7910/DVN/UYQIEP"],
      },
    ]);

    const expectedVerifiedSourceCount = VERIFIED_HISTORICAL_CONTEST_SOURCES.reduce(
      (count, source) => count + source.officeTypes.length,
      0
    );
    expect(status.verified_sources).toHaveLength(expectedVerifiedSourceCount);

    const verifiedByPresetAndOffice = new Map(
      status.verified_sources.map((source) => [`${source.preset}:${source.office_type}`, source])
    );

    expect(verifiedByPresetAndOffice.get("medsl-2024-president-state:US_PRESIDENT")).toMatchObject({
      imported: true,
      row_count: 51,
      latest_imported_at: "2026-06-15T01:02:03.000Z",
    });
    expect(verifiedByPresetAndOffice.get("medsl-2024-house-precinct:US_HOUSE")).toMatchObject({
      imported: true,
      row_count: 14,
      latest_imported_at: "2026-06-15T02:03:04.000Z",
    });
    expect(verifiedByPresetAndOffice.get("medsl-2022-precinct:GOVERNOR")).toMatchObject({
      source: "MIT_2022",
      source_url: "https://doi.org/10.7910/DVN/UYQIEP",
      format: "medsl_precinct_csv",
      election_year: 2022,
      imported: false,
      row_count: 0,
      latest_imported_at: null,
    });
    expect(verifiedByPresetAndOffice.get("medsl-2022-precinct:ATTORNEY_GENERAL")).toMatchObject({
      source: "MIT_2022",
      source_url: "https://doi.org/10.7910/DVN/UYQIEP",
      format: "medsl_precinct_csv",
      election_year: 2022,
      imported: true,
      row_count: 44,
      latest_imported_at: "2026-06-15T03:04:05.000Z",
    });
    expect(verifiedByPresetAndOffice.get("medsl-2022-precinct:COUNTY_SHERIFF")).toMatchObject({
      source: "MIT_2022",
      source_url: "https://doi.org/10.7910/DVN/UYQIEP",
      format: "medsl_precinct_csv",
      election_year: 2022,
      imported: true,
      row_count: 7,
      latest_imported_at: "2026-06-15T04:05:06.000Z",
    });
    expect(verifiedByPresetAndOffice.get("medsl-2024-state-precinct:STATE_TREASURER")).toMatchObject({
      source: "MIT_2024",
      source_url: "https://doi.org/10.7910/DVN/NYTPDU",
      format: "medsl_precinct_csv",
      election_year: 2024,
      imported: false,
      row_count: 0,
      latest_imported_at: null,
    });
    expect(verifiedByPresetAndOffice.get("medsl-2024-state-precinct:COUNTY_SHERIFF")).toMatchObject({
      source: "MIT_2024",
      source_url: "https://doi.org/10.7910/DVN/NYTPDU",
      format: "medsl_precinct_csv",
      election_year: 2024,
      imported: false,
      row_count: 0,
      latest_imported_at: null,
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
