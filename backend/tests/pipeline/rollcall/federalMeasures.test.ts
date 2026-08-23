import { describe, expect, it } from "vitest";

import {
  congressGovBillUrl,
  ordinalCongress,
  parseFederalMeasure,
} from "../../../src/pipeline/rollcall/federalMeasures.js";

describe("parseFederalMeasure", () => {
  it("reads both the Clerk and the LIS spellings", () => {
    expect(parseFederalMeasure("H R 1")).toEqual({ type: "hr", number: "1" });
    expect(parseFederalMeasure("H.R. 5371")).toEqual({ type: "hr", number: "5371" });
    expect(parseFederalMeasure("H. R. 3944")).toEqual({ type: "hr", number: "3944" });
    expect(parseFederalMeasure("H RES 5")).toEqual({ type: "hres", number: "5" });
    expect(parseFederalMeasure("H J RES 20")).toEqual({ type: "hjres", number: "20" });
    expect(parseFederalMeasure("H.J.Res. 104")).toEqual({ type: "hjres", number: "104" });
    expect(parseFederalMeasure("H CON RES 14")).toEqual({ type: "hconres", number: "14" });
    expect(parseFederalMeasure("S 5")).toEqual({ type: "s", number: "5" });
    expect(parseFederalMeasure("S. 2296")).toEqual({ type: "s", number: "2296" });
    expect(parseFederalMeasure("S.J.Res. 3")).toEqual({ type: "sjres", number: "3" });
    expect(parseFederalMeasure("S.Con.Res. 7")).toEqual({ type: "sconres", number: "7" });
    expect(parseFederalMeasure("S.Res. 12")).toEqual({ type: "sres", number: "12" });
    expect(parseFederalMeasure("PN373")).toEqual({ type: "pn", number: "373" });
    expect(parseFederalMeasure("PN12-31")).toEqual({ type: "pn", number: "12-31" });
  });

  it("returns null for votes with no measure", () => {
    expect(parseFederalMeasure(null)).toBeNull();
    expect(parseFederalMeasure("")).toBeNull();
    expect(parseFederalMeasure("QUORUM")).toBeNull();
    expect(parseFederalMeasure("Treaty Doc. 118-1")).toBeNull();
    expect(parseFederalMeasure("H R")).toBeNull();
  });
});

describe("congressGovBillUrl", () => {
  it("spells the ordinal and the bill-type slug", () => {
    expect(ordinalCongress(119)).toBe("119th");
    expect(ordinalCongress(111)).toBe("111th");
    expect(ordinalCongress(112)).toBe("112th");
    expect(ordinalCongress(113)).toBe("113th");
    expect(ordinalCongress(121)).toBe("121st");
    expect(ordinalCongress(122)).toBe("122nd");
    expect(ordinalCongress(123)).toBe("123rd");
    expect(congressGovBillUrl(119, { type: "hr", number: "1" })).toBe(
      "https://www.congress.gov/bill/119th-congress/house-bill/1"
    );
    expect(congressGovBillUrl(119, { type: "sjres", number: "3" })).toBe(
      "https://www.congress.gov/bill/119th-congress/senate-joint-resolution/3"
    );
  });

  it("has no page for nominations or measure-less votes", () => {
    expect(congressGovBillUrl(119, { type: "pn", number: "373" })).toBeNull();
    expect(congressGovBillUrl(119, null)).toBeNull();
  });
});
