import { describe, expect, it } from "vitest";

import { parseZctaCountyRelationshipFile } from "../../src/scripts/importZctaCountyCrosswalk.js";

const HEADER =
  "OID_ZCTA5_20|GEOID_ZCTA5_20|NAMELSAD_ZCTA5_20|AREALAND_ZCTA5_20|AREAWATER_ZCTA5_20|MTFCC_ZCTA5_20|CLASSFP_ZCTA5_20|FUNCSTAT_ZCTA5_20|OID_COUNTY_20|GEOID_COUNTY_20|NAMELSAD_COUNTY_20|AREALAND_COUNTY_20|AREAWATER_COUNTY_20|MTFCC_COUNTY_20|CLASSFP_COUNTY_20|FUNCSTAT_COUNTY_20|AREALAND_PART|AREAWATER_PART";

function dataLine(zcta5: string, countyGeoid: string): string {
  // Only the two GEOID columns matter to the parser; the rest are dummies in
  // the real 18-column layout.
  const fields = new Array(18).fill("x");
  fields[1] = zcta5;
  fields[9] = countyGeoid;
  return fields.join("|");
}

describe("parseZctaCountyRelationshipFile", () => {
  it("strips the BOM, skips blank-ZCTA rows, and dedupes", () => {
    const text = [
      "\uFEFF" + HEADER,
      dataLine("78701", "48453"),
      dataLine("", "48453"),
      dataLine("78701", "48453"),
    ].join("\n");

    const result = parseZctaCountyRelationshipFile(text);

    expect(result.rows).toEqual([{ zcta5: "78701", county_geoid: "48453" }]);
    expect(result.skipped_blank_zcta).toBe(1);
  });

  it("rejects a header missing the GEOID columns", () => {
    expect(() => parseZctaCountyRelationshipFile("A|B|C\n1|2|3")).toThrowError(/missing column GEOID_ZCTA5_20/);
  });

  it("rejects malformed GEOIDs and wrong field counts", () => {
    expect(() => parseZctaCountyRelationshipFile([HEADER, dataLine("7870", "48453")].join("\n"))).toThrowError(
      /invalid ZCTA/
    );
    expect(() => parseZctaCountyRelationshipFile([HEADER, dataLine("78701", "4845")].join("\n"))).toThrowError(
      /invalid ZCTA "78701" or county GEOID "4845"/
    );
    expect(() => parseZctaCountyRelationshipFile([HEADER, "only|three|fields"].join("\n"))).toThrowError(
      /expected 18 fields, got 3/
    );
  });

  it("handles CRLF line endings and a trailing newline", () => {
    const text = [HEADER, dataLine("02861", "44007"), dataLine("02861", "25005")].join("\r\n") + "\r\n";

    const result = parseZctaCountyRelationshipFile(text);

    expect(result.rows).toEqual([
      { zcta5: "02861", county_geoid: "44007" },
      { zcta5: "02861", county_geoid: "25005" },
    ]);
  });
});
