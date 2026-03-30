import { describe, expect, it } from "vitest";

import { parseStateDistrictRows } from "../../../src/pipeline/loaders/districtsLoader.js";
import { STATE_ABBR_BY_FIPS } from "../../../src/constants/usStates.js";

describe("parseStateDistrictRows", () => {
  it("parses valid state rows and maps state abbreviation + us_senate type", () => {
    const data: unknown[] = [["NAME", "B01001_001E", "state"]];
    for (const fips of Object.keys(STATE_ABBR_BY_FIPS)) {
      if (fips === "01") {
        data.push(["Alabama", "5086768", "01"]);
        continue;
      }
      if (fips === "06") {
        data.push(["California", "39287377", "06"]);
        continue;
      }
      data.push([`State ${fips}`, "1000", fips]);
    }

    const rows = parseStateDistrictRows(data);
    expect(rows).toHaveLength(Object.keys(STATE_ABBR_BY_FIPS).length);

    const alabama = rows.find((row) => row.state_fips === "01");
    const california = rows.find((row) => row.state_fips === "06");
    expect(alabama).toMatchObject({
      geoid_compact: "01",
      state_fips: "01",
      state: "AL",
      district_type: "us_senate",
      population: 5086768,
    });
    expect(california).toMatchObject({
      geoid_compact: "06",
      state_fips: "06",
      state: "CA",
      district_type: "us_senate",
      population: 39287377,
    });
  });

  it("filters territories and enforces 50+DC completeness", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const completeRows: unknown[] = [["NAME", "B01001_001E", "state"]];
    for (const fips of allFips) {
      completeRows.push([`State ${fips}`, "1000", fips]);
    }
    completeRows.push(["Puerto Rico", "3200000", "72"]);

    const parsed = parseStateDistrictRows(completeRows);
    expect(parsed).toHaveLength(allFips.length);
    expect(parsed.some((row) => row.state_fips === "72")).toBe(false);
  });

  it("throws on invalid population values", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state"]];
    for (const fips of allFips) {
      data.push([`State ${fips}`, fips === "06" ? "not-a-number" : "1000", fips]);
    }

    expect(() => parseStateDistrictRows(data)).toThrow(/Invalid population value/);
  });
});
