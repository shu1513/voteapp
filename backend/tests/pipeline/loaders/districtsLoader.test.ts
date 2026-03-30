import { describe, expect, it } from "vitest";

import {
  EXPECTED_COUNTY_ROWS_50_PLUS_DC_2024,
  EXPECTED_PLACE_ROWS_50_PLUS_DC_2024,
  EXPECTED_SCHOOL_UNIFIED_ROWS_50_PLUS_DC_2024,
  parseCountyDistrictRows,
  parsePlaceDistrictRows,
  parseSchoolUnifiedDistrictRows,
  parseStateDistrictRows,
  parseUsHouseDistrictRows,
} from "../../../src/pipeline/loaders/districtsLoader.js";
import { COUNTY_GEOIDS_50_PLUS_DC_2024 } from "../../../src/constants/countyGeoids2024.js";
import { PLACE_GEOIDS_50_PLUS_DC_2024 } from "../../../src/constants/placeGeoids2024.js";
import { SCHOOL_UNIFIED_GEOIDS_50_PLUS_DC_2024 } from "../../../src/constants/schoolUnifiedGeoids2024.js";
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
  function buildCanonicalCountyData(): unknown[] {
    const data: unknown[] = [["NAME", "B01001_001E", "state", "county"]];
    for (const geoid of COUNTY_GEOIDS_50_PLUS_DC_2024) {
      const state = geoid.slice(0, 2);
      const county = geoid.slice(2);
      data.push([`County ${county}, State ${state}`, "1000", state, county]);
    }
    return data;
  }

  it("parses complete county rows and excludes territories", () => {
    const data = buildCanonicalCountyData();
    data.push(["Adjuntas Municipio, Puerto Rico", "17960", "72", "001"]);

    const rows = parseCountyDistrictRows(data);
    expect(rows).toHaveLength(EXPECTED_COUNTY_ROWS_50_PLUS_DC_2024);
    expect(rows.some((row) => row.state_fips === "72")).toBe(false);

    const dc = rows.find((row) => row.state_fips === "11" && row.geoid_compact === "11001");
    const california = rows.find((row) => row.geoid_compact === "06001");
    expect(dc).toBeTruthy();
    expect(california).toBeTruthy();
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

  it("throws when county payload is truncated even if all states are present", () => {
    const complete = buildCanonicalCountyData();
    const header = complete[0];
    const rows = complete.slice(1);
    const truncated = [header, ...rows.slice(1)];
    expect(() => parseCountyDistrictRows(truncated)).toThrow(
      new RegExp(`Expected ${EXPECTED_COUNTY_ROWS_50_PLUS_DC_2024} county rows`)
    );
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

  it("throws when county payload swaps one canonical geoid for an unexpected one", () => {
    const complete = buildCanonicalCountyData();
    const rows = complete.slice(1) as string[][];

    const targetIndex = rows.findIndex((row) => row[2] === "06" && row[3] === "001");
    expect(targetIndex).toBeGreaterThanOrEqual(0);
    rows[targetIndex] = ["County 999, State 06", "1000", "06", "999"];

    const mutated: unknown[] = [complete[0], ...rows];
    expect(() => parseCountyDistrictRows(mutated)).toThrow(/County GEOID set mismatch/);
    expect(() => parseCountyDistrictRows(mutated)).toThrow(/missing=1/);
    expect(() => parseCountyDistrictRows(mutated)).toThrow(/unexpected=1/);
  });
});

describe("parsePlaceDistrictRows", () => {
  function buildCanonicalPlaceData(): unknown[] {
    const data: unknown[] = [["NAME", "B01001_001E", "state", "place"]];
    for (const geoid of PLACE_GEOIDS_50_PLUS_DC_2024) {
      const state = geoid.slice(0, 2);
      const place = geoid.slice(2);
      data.push([`Place ${place}, State ${state}`, "1000", state, place]);
    }
    return data;
  }

  it("parses complete place rows and excludes territories", () => {
    const data = buildCanonicalPlaceData();
    data.push(["Aceitunas comunidad, Puerto Rico", "1369", "72", "00186"]);

    const rows = parsePlaceDistrictRows(data);
    expect(rows).toHaveLength(EXPECTED_PLACE_ROWS_50_PLUS_DC_2024);
    expect(rows.some((row) => row.state_fips === "72")).toBe(false);

    const dc = rows.find((row) => row.state_fips === "11" && row.geoid_compact === "1150000");
    const california = rows.find((row) => row.geoid_compact === "0600135");
    expect(dc).toBeTruthy();
    expect(california).toBeTruthy();
  });

  it("allows zero population values from official place payloads", () => {
    const data = buildCanonicalPlaceData();
    const rows = data.slice(1) as string[][];
    const targetIndex = rows.findIndex((row) => row[2] === "06" && row[3] === "00135");
    expect(targetIndex).toBeGreaterThanOrEqual(0);
    rows[targetIndex][1] = "0";

    const parsed = parsePlaceDistrictRows([data[0], ...rows]);
    const target = parsed.find((row) => row.geoid_compact === "0600135");
    expect(target?.population).toBe(0);
  });

  it("throws on unexpected place code", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "place"]];
    for (const fips of allFips) {
      const placeCode = fips === "06" ? "ABCDE" : "00001";
      data.push([`Place ${placeCode}, State ${fips}`, "1000", fips, placeCode]);
    }

    expect(() => parsePlaceDistrictRows(data)).toThrow(/Unexpected place code/);
  });

  it("throws when place rows are missing a supported state", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "place"]];
    for (const fips of allFips) {
      if (fips === "56") {
        continue;
      }
      data.push([`Place 00001, State ${fips}`, "1000", fips, "00001"]);
    }

    expect(() => parsePlaceDistrictRows(data)).toThrow(/Missing: 56/);
  });

  it("throws when place payload is truncated even if all states are present", () => {
    const complete = buildCanonicalPlaceData();
    const header = complete[0];
    const rows = complete.slice(1);
    const truncated = [header, ...rows.slice(1)];
    expect(() => parsePlaceDistrictRows(truncated)).toThrow(
      new RegExp(`Expected ${EXPECTED_PLACE_ROWS_50_PLUS_DC_2024} place rows`)
    );
  });

  it("throws on duplicate place geoid rows", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "place"]];
    for (const fips of allFips) {
      data.push([`Place 00001, State ${fips}`, "1000", fips, "00001"]);
    }
    data.push(["Duplicate California Place 00001", "2000", "06", "00001"]);

    expect(() => parsePlaceDistrictRows(data)).toThrow(/Duplicate place rows returned by Census: 0600001/);
  });

  it("throws when place payload swaps one canonical geoid for an unexpected one", () => {
    const complete = buildCanonicalPlaceData();
    const rows = complete.slice(1) as string[][];

    const targetIndex = rows.findIndex((row) => row[2] === "06" && row[3] === "00135");
    expect(targetIndex).toBeGreaterThanOrEqual(0);
    rows[targetIndex] = ["Place 99999, State 06", "1000", "06", "99999"];

    const mutated: unknown[] = [complete[0], ...rows];
    expect(() => parsePlaceDistrictRows(mutated)).toThrow(/Place GEOID set mismatch/);
    expect(() => parsePlaceDistrictRows(mutated)).toThrow(/missing=1/);
    expect(() => parsePlaceDistrictRows(mutated)).toThrow(/unexpected=1/);
  });
});

describe("parseSchoolUnifiedDistrictRows", () => {
  function buildCanonicalSchoolUnifiedData(): unknown[] {
    const data: unknown[] = [["NAME", "B01001_001E", "state", "school district (unified)"]];
    for (const geoid of SCHOOL_UNIFIED_GEOIDS_50_PLUS_DC_2024) {
      const state = geoid.slice(0, 2);
      const district = geoid.slice(2);
      data.push([`Unified School District ${district}, State ${state}`, "1000", state, district]);
    }
    return data;
  }

  it("parses complete school_unified rows, excludes territories, and skips remainder aggregates", () => {
    const data = buildCanonicalSchoolUnifiedData();
    data.push(["Remainder of Alabama, Alabama", "1234", "01", "99999"]);
    data.push(["Puerto Rico Department of Education, Puerto Rico", "3234309", "72", "00030"]);

    const rows = parseSchoolUnifiedDistrictRows(data);
    expect(rows).toHaveLength(EXPECTED_SCHOOL_UNIFIED_ROWS_50_PLUS_DC_2024);
    expect(rows.some((row) => row.state_fips === "72")).toBe(false);
    expect(rows.some((row) => row.geoid_compact.endsWith("99999"))).toBe(false);

    const dc = rows.find((row) => row.geoid_compact === "1100030");
    const california = rows.find((row) => row.geoid_compact === "0600001");
    expect(dc).toBeTruthy();
    expect(california).toBeTruthy();
  });

  it("allows zero population values from official payloads", () => {
    const data = buildCanonicalSchoolUnifiedData();
    const rows = data.slice(1) as string[][];
    const targetIndex = rows.findIndex((row) => row[2] === "23" && row[3] === "82004");
    expect(targetIndex).toBeGreaterThanOrEqual(0);
    rows[targetIndex][1] = "0";

    const parsed = parseSchoolUnifiedDistrictRows([data[0], ...rows]);
    const target = parsed.find((row) => row.geoid_compact === "2382004");
    expect(target?.population).toBe(0);
  });

  it("throws on unexpected unified district code", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "school district (unified)"]];
    for (const fips of allFips) {
      const districtCode = fips === "06" ? "ABCDE" : "00001";
      data.push([`Unified School District ${districtCode}, State ${fips}`, "1000", fips, districtCode]);
    }

    expect(() => parseSchoolUnifiedDistrictRows(data)).toThrow(/Unexpected unified school district code/);
  });

  it("throws when school_unified rows are missing a supported state", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "school district (unified)"]];
    for (const fips of allFips) {
      if (fips === "56") {
        continue;
      }
      data.push([`Unified School District 00001, State ${fips}`, "1000", fips, "00001"]);
    }

    expect(() => parseSchoolUnifiedDistrictRows(data)).toThrow(/Missing: 56/);
  });

  it("throws when school_unified payload is truncated even if all states are present", () => {
    const complete = buildCanonicalSchoolUnifiedData();
    const header = complete[0];
    const rows = complete.slice(1);
    const truncated = [header, ...rows.slice(1)];
    expect(() => parseSchoolUnifiedDistrictRows(truncated)).toThrow(
      new RegExp(`Expected ${EXPECTED_SCHOOL_UNIFIED_ROWS_50_PLUS_DC_2024} unified school district rows`)
    );
  });

  it("throws on duplicate school_unified geoid rows", () => {
    const allFips = Object.keys(STATE_ABBR_BY_FIPS);
    const data: unknown[] = [["NAME", "B01001_001E", "state", "school district (unified)"]];
    for (const fips of allFips) {
      data.push([`Unified School District 00001, State ${fips}`, "1000", fips, "00001"]);
    }
    data.push(["Duplicate California Unified 00001", "2000", "06", "00001"]);

    expect(() => parseSchoolUnifiedDistrictRows(data)).toThrow(/Duplicate unified school district rows returned by Census: 0600001/);
  });

  it("throws when school_unified payload swaps one canonical geoid for an unexpected one", () => {
    const complete = buildCanonicalSchoolUnifiedData();
    const rows = complete.slice(1) as string[][];

    const targetIndex = rows.findIndex((row) => row[2] === "06" && row[3] === "00001");
    expect(targetIndex).toBeGreaterThanOrEqual(0);
    rows[targetIndex] = ["Unified School District 99998, State 06", "1000", "06", "99998"];

    const mutated: unknown[] = [complete[0], ...rows];
    expect(() => parseSchoolUnifiedDistrictRows(mutated)).toThrow(/Unified school district GEOID set mismatch/);
    expect(() => parseSchoolUnifiedDistrictRows(mutated)).toThrow(/missing=1/);
    expect(() => parseSchoolUnifiedDistrictRows(mutated)).toThrow(/unexpected=1/);
  });
});
