import { describe, expect, it } from "vitest";
import {
  matchSanFranciscoPublicFunds,
  sanFranciscoPublicFundsDistrictForContest,
} from "../../src/pipeline/sanFranciscoFinance/sanFranciscoPublicFundsMatcher.js";
import type { SanFranciscoPublicFundsRow } from "../../src/pipeline/sanFranciscoFinance/sanFranciscoOpenDataClient.js";

function row(
  candidateName: string,
  district: string,
  fundsApprovedCents: number,
): SanFranciscoPublicFundsRow {
  return { candidateName, district, pendingCompleted: null, fundsApprovedCents };
}

describe("sanFranciscoPublicFundsDistrictForContest", () => {
  it("maps the funded offices and rejects everything else", () => {
    expect(sanFranciscoPublicFundsDistrictForContest("myr")).toBe("Mayor");
    expect(sanFranciscoPublicFundsDistrictForContest("bos04")).toBe("4");
    expect(sanFranciscoPublicFundsDistrictForContest("bos11")).toBe("11");
    expect(sanFranciscoPublicFundsDistrictForContest("asr")).toBeNull();
    expect(sanFranciscoPublicFundsDistrictForContest("da")).toBeNull();
  });
});

describe("matchSanFranciscoPublicFunds", () => {
  const rows = [
    row("Chow, Albert", "4", 6_000_000),
    row("Chow, Albert", "4", 1_527_950),
    row("Lee, David", "4", 2_500_000),
    row("Chow, Albert", "1", 999_999),
  ];

  it("sums the matched candidate's rows within the district, keeping approval order", () => {
    const result = matchSanFranciscoPublicFunds({
      rows,
      candidateName: "ALBERT CHOW",
      district: "4",
    });
    expect(result).toEqual({
      status: "matched",
      publicFundsCents: 7_527_950,
      approvalCents: [6_000_000, 1_527_950],
      matchedNames: ["Chow, Albert"],
    });
  });

  it("returns a normal zero when the candidate has no rows", () => {
    expect(
      matchSanFranciscoPublicFunds({
        rows,
        candidateName: "NATALIE GEE",
        district: "4",
      }),
    ).toEqual({
      status: "none",
      publicFundsCents: 0,
      approvalCents: [],
      matchedNames: [],
    });
  });

  it("never sums another district's rows", () => {
    const result = matchSanFranciscoPublicFunds({
      rows,
      candidateName: "ALBERT CHOW",
      district: "1",
    });
    expect(result.publicFundsCents).toBe(999_999);
  });

  it("fails closed when two distinct disclosed names both match", () => {
    const result = matchSanFranciscoPublicFunds({
      rows: [
        row("Chow, Albert", "4", 6_000_000),
        row("Chow, Albert M.", "4", 1_000_000),
      ],
      candidateName: "ALBERT CHOW",
      district: "4",
    });
    expect(result.status).toBe("ambiguous");
    expect(result.publicFundsCents).toBe(0);
    expect(result.approvalCents).toEqual([]);
    expect(result.matchedNames.sort()).toEqual([
      "Chow, Albert",
      "Chow, Albert M.",
    ]);
  });

  it("vetoes a middle-name conflict instead of matching it", () => {
    const result = matchSanFranciscoPublicFunds({
      rows: [row("Chow, Albert M.", "4", 6_000_000)],
      candidateName: "ALBERT B. CHOW",
      district: "4",
    });
    expect(result.status).toBe("none");
  });
});
