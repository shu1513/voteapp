import { describe, expect, it } from "vitest";

import { parseCountyDistrictRows, parseStateDistrictRows, parseUsHouseDistrictRows } from "../../../src/pipeline/loaders/districtsLoader.js";
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

  it("throws when Census returns duplicate state rows", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state"]];
    for (const fips of allFips) {
      data.push([`State ${fips}`, "1000", fips]);
    }
    data.push(["Duplicate California", "2000", "06"]);

    expect(() => parseStateDistrictRows(data)).toThrow(/Duplicate state rows returned by Census: 06/);
  });
});

describe("parseUsHouseDistrictRows", () => {
  it("parses valid congressional rows, filters ZZ, and excludes territories", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "congressional district"]];

    for (const fips of allFips) {
      const districtCode = fips === "02" ? "00" : fips === "11" ? "98" : "01";
      data.push([`Congressional District ${districtCode}, State ${fips}`, "1000", fips, districtCode]);
    }

    data.push(["Congressional Districts not defined, California", "0", "06", "ZZ"]);
    data.push(["Resident Commissioner District, Puerto Rico", "3200000", "72", "98"]);

    const rows = parseUsHouseDistrictRows(data);
    expect(rows).toHaveLength(allFips.length);
    expect(rows.some((row) => row.state_fips === "72")).toBe(false);

    const alaska = rows.find((row) => row.state_fips === "02");
    const dc = rows.find((row) => row.state_fips === "11");
    const california = rows.find((row) => row.state_fips === "06");
    expect(alaska).toMatchObject({
      geoid_compact: "0200",
      district_type: "us_house",
    });
    expect(dc).toMatchObject({
      geoid_compact: "1198",
      district_type: "us_house",
    });
    expect(california).toMatchObject({
      geoid_compact: "0601",
      district_type: "us_house",
    });
  });

  it("throws on unexpected congressional district code", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "congressional district"]];
    for (const fips of allFips) {
      const districtCode = fips === "06" ? "AA" : "01";
      data.push([`Congressional District ${districtCode}, State ${fips}`, "1000", fips, districtCode]);
    }

    expect(() => parseUsHouseDistrictRows(data)).toThrow(/Unexpected congressional district code/);
  });

  it("throws when congressional rows are missing a supported state", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "congressional district"]];
    for (const fips of allFips) {
      if (fips === "56") {
        continue;
      }
      data.push([`Congressional District 01, State ${fips}`, "1000", fips, "01"]);
    }

    expect(() => parseUsHouseDistrictRows(data)).toThrow(/Missing: 56/);
  });

  it("throws on duplicate congressional geoid rows", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "congressional district"]];
    for (const fips of allFips) {
      data.push([`Congressional District 01, State ${fips}`, "1000", fips, "01"]);
    }
    data.push(["Duplicate California 01", "2000", "06", "01"]);

    expect(() => parseUsHouseDistrictRows(data)).toThrow(/Duplicate congressional district rows returned by Census: 0601/);
  });
});

describe("parseCountyDistrictRows", () => {
  it("parses valid county rows and excludes territories", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "county"]];

    for (const fips of allFips) {
      data.push([`County 001, State ${fips}`, "1000", fips, "001"]);
    }
    data.push(["Adjuntas Municipio, Puerto Rico", "17960", "72", "001"]);

    const rows = parseCountyDistrictRows(data);
    expect(rows).toHaveLength(allFips.length);
    expect(rows.some((row) => row.state_fips === "72")).toBe(false);

    const dc = rows.find((row) => row.state_fips === "11");
    const california = rows.find((row) => row.state_fips === "06");
    expect(dc).toMatchObject({
      geoid_compact: "11001",
      district_type: "county",
    });
    expect(california).toMatchObject({
      geoid_compact: "06001",
      district_type: "county",
    });
  });

  it("throws on unexpected county code", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "county"]];
    for (const fips of allFips) {
      const countyCode = fips === "06" ? "AA1" : "001";
      data.push([`County ${countyCode}, State ${fips}`, "1000", fips, countyCode]);
    }

    expect(() => parseCountyDistrictRows(data)).toThrow(/Unexpected county code/);
  });

  it("throws when county rows are missing a supported state", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "county"]];
    for (const fips of allFips) {
      if (fips === "56") {
        continue;
      }
      data.push([`County 001, State ${fips}`, "1000", fips, "001"]);
    }

    expect(() => parseCountyDistrictRows(data)).toThrow(/Missing: 56/);
  });

  it("throws on duplicate county geoid rows", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "county"]];
    for (const fips of allFips) {
      data.push([`County 001, State ${fips}`, "1000", fips, "001"]);
    }
    data.push(["Duplicate California County 001", "2000", "06", "001"]);

    expect(() => parseCountyDistrictRows(data)).toThrow(/Duplicate county rows returned by Census: 06001/);
  });
});
