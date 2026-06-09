import { describe, expect, it } from "vitest";

import {
  districtTypeFromLayerName,
  districtTypeFromMtfcc,
  extractAddressDistrictKeysFromGeographies,
  resolveAddressDistrictKeysFromGeographies,
} from "../../../src/pipeline/address/addressDistrictResolver.js";

describe("addressDistrictResolver", () => {
  it("maps supported Census MTFCC codes to district types", () => {
    expect(districtTypeFromMtfcc("G4000")).toBe("statewide");
    expect(districtTypeFromMtfcc("G5200")).toBe("us_house");
    expect(districtTypeFromMtfcc("G5210")).toBe("state_upper");
    expect(districtTypeFromMtfcc("G5220")).toBe("state_lower");
    expect(districtTypeFromMtfcc("G4020")).toBe("county");
    expect(districtTypeFromMtfcc("G4110")).toBe("place");
    expect(districtTypeFromMtfcc("G5420")).toBe("school_unified");
    expect(districtTypeFromMtfcc("G5410")).toBe("school_secondary");
    expect(districtTypeFromMtfcc("G5400")).toBe("school_elementary");
    expect(districtTypeFromMtfcc("G9999")).toBeNull();
  });

  it("maps known Census layer names as fallback", () => {
    expect(districtTypeFromLayerName("States")).toBe("statewide");
    expect(districtTypeFromLayerName("120th Congressional Districts")).toBe("us_house");
    expect(districtTypeFromLayerName("2024 State Legislative Districts - Upper")).toBe("state_upper");
    expect(districtTypeFromLayerName("2024 State Legislative Districts - Lower")).toBe("state_lower");
    expect(districtTypeFromLayerName("Counties")).toBe("county");
    expect(districtTypeFromLayerName("Incorporated Places")).toBe("place");
    expect(districtTypeFromLayerName("Unified School Districts")).toBe("school_unified");
    expect(districtTypeFromLayerName("Secondary School Districts")).toBe("school_secondary");
    expect(districtTypeFromLayerName("Elementary School Districts")).toBe("school_elementary");
    expect(districtTypeFromLayerName("2020 Census Blocks")).toBeNull();
  });

  it("extracts Baldwin Park district keys from Census geographies", () => {
    const geographies = {
      Counties: [{ GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G4020" }],
      "Incorporated Places": [{ GEOID: "0603666", NAME: "Baldwin Park city", MTFCC: "G4110" }],
      "Unified School Districts": [{ GEOID: "0603690", NAME: "Baldwin Park Unified School District", MTFCC: "G5420" }],
      "2024 State Legislative Districts - Upper": [{ GEOID: "06022", NAME: "State Senate District 22", MTFCC: "G5210" }],
      "2024 State Legislative Districts - Lower": [{ GEOID: "06048", NAME: "Assembly District 48", MTFCC: "G5220" }],
      "119th Congressional Districts": [{ GEOID: "0631", NAME: "Congressional District 31", MTFCC: "G5200" }],
      States: [{ GEOID: "06", NAME: "California", MTFCC: "G4000" }],
      "2020 Census Blocks": [{ GEOID: "060374049021006", NAME: "Block 1006", MTFCC: "G5040" }],
      "2020 Census ZIP Code Tabulation Areas": [{ GEOID: "91706", NAME: "ZCTA5 91706", MTFCC: "G6350" }],
    };

    expect(extractAddressDistrictKeysFromGeographies(geographies).map(({ district_type, geoid_compact }) => ({
      district_type,
      geoid_compact,
    }))).toEqual([
      { district_type: "statewide", geoid_compact: "06" },
      { district_type: "us_house", geoid_compact: "0631" },
      { district_type: "state_upper", geoid_compact: "06022" },
      { district_type: "state_lower", geoid_compact: "06048" },
      { district_type: "county", geoid_compact: "06037" },
      { district_type: "place", geoid_compact: "0603666" },
      { district_type: "school_unified", geoid_compact: "0603690" },
    ]);
  });

  it("uses layer-name fallback when MTFCC is missing", () => {
    const result = resolveAddressDistrictKeysFromGeographies({
      Counties: [{ GEOID: "06037", NAME: "Los Angeles County" }],
    });

    expect(result.warnings).toEqual([]);
    expect(result.district_keys).toEqual([
      {
        district_type: "county",
        geoid_compact: "06037",
        source: "layer_name",
        layer_name: "Counties",
        name: "Los Angeles County",
      },
    ]);
  });

  it("skips features when MTFCC and layer name disagree", () => {
    const result = resolveAddressDistrictKeysFromGeographies({
      Counties: [{ GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G5200" }],
    });

    expect(result.district_keys).toEqual([]);
    expect(result.warnings).toEqual([
      {
        layer_name: "Counties",
        geoid: "06037",
        mtfcc: "G5200",
        reason: "MTFCC maps to us_house but layer name maps to county",
      },
    ]);
  });

  it("dedupes identical keys and prefers MTFCC-derived rows", () => {
    const result = resolveAddressDistrictKeysFromGeographies({
      Counties: [
        { GEOID: "06037", NAME: "Los Angeles County" },
        { GEOID: "06037", NAME: "Los Angeles County", MTFCC: "G4020" },
      ],
    });

    expect(result.district_keys).toEqual([
      {
        district_type: "county",
        geoid_compact: "06037",
        source: "mtfcc",
        layer_name: "Counties",
        mtfcc: "G4020",
        name: "Los Angeles County",
      },
    ]);
  });

  it("returns warnings for malformed geography containers but ignores unsupported layers", () => {
    const result = resolveAddressDistrictKeysFromGeographies({
      Counties: "not-an-array",
      "2020 Census Blocks": [{ GEOID: "060374049021006", NAME: "Block 1006", MTFCC: "G5040" }],
      States: [{ NAME: "California", MTFCC: "G4000" }],
    });

    expect(result.district_keys).toEqual([]);
    expect(result.warnings).toEqual([
      { layer_name: "Counties", reason: "geography layer is not an array" },
      { layer_name: "States", mtfcc: "G4000", reason: "geography feature is missing GEOID" },
    ]);
  });
});
