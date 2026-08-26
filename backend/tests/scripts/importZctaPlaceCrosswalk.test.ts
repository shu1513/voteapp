import { describe, expect, it } from "vitest";

import { parseZctaPlaceRelationshipFile } from "../../src/scripts/importZctaPlaceCrosswalk.js";

const HEADER =
  "OID_ZCTA5_20|GEOID_ZCTA5_20|NAMELSAD_ZCTA5_20|AREALAND_ZCTA5_20|AREAWATER_ZCTA5_20|MTFCC_ZCTA5_20|CLASSFP_ZCTA5_20|FUNCSTAT_ZCTA5_20|OID_PLACE_20|GEOID_PLACE_20|NAMELSAD_PLACE_20|AREALAND_PLACE_20|AREAWATER_PLACE_20|MTFCC_PLACE_20|CLASSFP_PLACE_20|FUNCSTAT_PLACE_20|AREALAND_PART|AREAWATER_PART";

function dataLine(input: {
  zcta5: string;
  placeGeoid?: string;
  classFp?: string;
  landPart?: number;
  waterPart?: number;
}): string {
  // Only the columns the parser reads matter; the rest are dummies in the
  // real 18-column layout.
  const fields = new Array(18).fill("x");
  fields[1] = input.zcta5;
  fields[9] = input.placeGeoid ?? "";
  fields[14] = input.classFp ?? "";
  fields[16] = String(input.landPart ?? 0);
  fields[17] = String(input.waterPart ?? 0);
  return fields.join("|");
}

describe("parseZctaPlaceRelationshipFile", () => {
  it("keeps a ZCTA wholly inside one incorporated place, stripping the BOM", () => {
    const text = [
      "﻿" + HEADER,
      dataLine({ zcta5: "78701", placeGeoid: "4805000", classFp: "C1", landPart: 1000 }),
    ].join("\n");

    const result = parseZctaPlaceRelationshipFile(text);

    expect(result.rows).toEqual([{ zcta5: "78701", place_geoid: "4805000" }]);
    expect(result.zctas_seen).toBe(1);
  });

  it("rejects a ZCTA with land outside every place, but tolerates water-only remainders", () => {
    const text = [
      HEADER,
      // 91706-style: city overlap plus unincorporated land remainder.
      dataLine({ zcta5: "91706", placeGeoid: "0603666", classFp: "C1", landPart: 1000 }),
      dataLine({ zcta5: "91706", landPart: 5 }),
      // Coastal city: remainder is water only — nobody votes offshore.
      dataLine({ zcta5: "94110", placeGeoid: "0667000", classFp: "C1", landPart: 1000 }),
      dataLine({ zcta5: "94110", landPart: 0, waterPart: 400 }),
    ].join("\n");

    const result = parseZctaPlaceRelationshipFile(text);

    expect(result.rows).toEqual([{ zcta5: "94110", place_geoid: "0667000" }]);
  });

  it("rejects a ZCTA touching two places, and one whose only place is a CDP", () => {
    const text = [
      HEADER,
      // Split between two cities: neither contains it.
      dataLine({ zcta5: "60614", placeGeoid: "1714000", classFp: "C1", landPart: 800 }),
      dataLine({ zcta5: "60614", placeGeoid: "1714001", classFp: "C1", landPart: 200 }),
      // Wholly inside a CDP: not a government, no races to offer.
      dataLine({ zcta5: "20602", placeGeoid: "2490220", classFp: "U1", landPart: 1000 }),
      // City + CDP: part of the ZCTA is outside the city.
      dataLine({ zcta5: "30301", placeGeoid: "1304000", classFp: "C1", landPart: 700 }),
      dataLine({ zcta5: "30301", placeGeoid: "1390221", classFp: "U1", landPart: 300 }),
    ].join("\n");

    expect(parseZctaPlaceRelationshipFile(text).rows).toEqual([]);
  });

  it("keeps consolidated-government balances and DC's Washington city", () => {
    const text = [
      HEADER,
      dataLine({ zcta5: "37203", placeGeoid: "4752006", classFp: "C8", landPart: 1000 }),
      dataLine({ zcta5: "20001", placeGeoid: "1150000", classFp: "C5", landPart: 1000 }),
    ].join("\n");

    expect(parseZctaPlaceRelationshipFile(text).rows).toEqual([
      { zcta5: "20001", place_geoid: "1150000" },
      { zcta5: "37203", place_geoid: "4752006" },
    ]);
  });

  it("skips blank-ZCTA rows and validates shapes", () => {
    const blankOk = [HEADER, dataLine({ zcta5: "", placeGeoid: "4805000", classFp: "C1", landPart: 9 })].join("\n");
    expect(parseZctaPlaceRelationshipFile(blankOk).rows).toEqual([]);

    for (const bad of [
      dataLine({ zcta5: "7870", placeGeoid: "4805000", classFp: "C1" }),
      dataLine({ zcta5: "78701", placeGeoid: "480500", classFp: "C1" }),
      "x|78701|only-three-fields",
    ]) {
      expect(() => parseZctaPlaceRelationshipFile([HEADER, bad].join("\n"))).toThrow(/line 2/);
    }
    expect(() => parseZctaPlaceRelationshipFile("A|B|C\n")).toThrow(/missing column/);
  });
});
