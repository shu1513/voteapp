import { describe, expect, it, vi } from "vitest";

import { VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET } from "../../src/pipeline/competitiveness/historicalContestSources.js";
import { importVerifiedHistoricalContestSource } from "../../src/scripts/importVerifiedHistoricalContestMargins.js";

describe("importVerifiedHistoricalContestMargins", () => {
  it("imports newly supported statewide executive offices from state-office verified sources", async () => {
    const query = vi.fn();
    const csv = [
      "year,state_po,state_fips,office,district,candidate,votes,party_simplified,stage,mode",
      "2024,CA,06,STATE TREASURER,,Treasurer One,520,DEMOCRAT,GEN,TOTAL",
      "2024,CA,06,STATE TREASURER,,Treasurer Two,480,REPUBLICAN,GEN,TOTAL",
      "2024,CA,06,US PRESIDENT,,President One,600,DEMOCRAT,GEN,TOTAL",
      "2024,CA,06,US PRESIDENT,,President Two,400,REPUBLICAN,GEN,TOTAL",
    ].join("\n");

    await expect(
      importVerifiedHistoricalContestSource({
        db: { query } as never,
        source: VERIFIED_HISTORICAL_CONTEST_SOURCE_BY_PRESET["medsl-2024-state-precinct"],
        csv,
        sourceUrl: "https://dataverse.harvard.edu/api/access/datafile/13731163",
        dryRun: true,
        importedAt: new Date("2026-06-16T00:00:00.000Z"),
      })
    ).resolves.toMatchObject({
      preset: "medsl-2024-state-precinct",
      source: "MIT_2024",
      source_url: "https://doi.org/10.7910/DVN/NYTPDU",
      format: "medsl_precinct_csv",
      election_year: 2024,
      office_types: expect.arrayContaining(["STATE_TREASURER"]),
      stale_after_redistricting: false,
      parsed_rows: 4,
      aggregated_rows: 4,
      normalized_records: 1,
      skipped_rows: 2,
      rows_written: 0,
      skipped_reasons: {
        excluded_office: 2,
      },
    });
    expect(query).not.toHaveBeenCalled();
  });
});
