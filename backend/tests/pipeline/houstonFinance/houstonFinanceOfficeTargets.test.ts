import { describe, expect, it } from "vitest";

import {
  parseHoustonDisclosureOfficeTarget,
  parseHoustonEfileOfficeTarget,
  parseStoredHoustonFinanceOfficeTarget,
  resolveHoustonElectionOfficeTarget,
} from "../../../src/pipeline/houstonFinance/houstonFinanceOfficeTargets.js";

describe("Houston finance office targets", () => {
  it("maps citywide election offices", () => {
    expect(resolveHoustonElectionOfficeTarget({ officeCanonicalName: "Mayor", officialBallotTitle: "Mayor" }))
      .toEqual({ officeName: "Mayor", seat: "Houston" });
    expect(resolveHoustonElectionOfficeTarget({ officeCanonicalName: "Municipal Controller", officialBallotTitle: "City Controller" }))
      .toEqual({ officeName: "Municipal Controller", seat: "Houston" });
  });

  it("requires an exact district or at-large council seat", () => {
    expect(resolveHoustonElectionOfficeTarget({
      officeCanonicalName: "City Council Member",
      officialBallotTitle: "City Council, District C",
    })).toEqual({ officeName: "City Council Member", seat: "District C" });
    expect(resolveHoustonElectionOfficeTarget({
      officeCanonicalName: "City Council Member",
      officialBallotTitle: "Council Member At-Large Position 2",
    })).toEqual({ officeName: "City Council Member", seat: "At-Large 2" });
    expect(resolveHoustonElectionOfficeTarget({
      officeCanonicalName: "City Council Member",
      officialBallotTitle: "Council Member At-Large Position No. 2",
    })).toEqual({ officeName: "City Council Member", seat: "At-Large 2" });
    expect(resolveHoustonElectionOfficeTarget({
      officeCanonicalName: "City Council Member",
      officialBallotTitle: "COH Council Member At Lg Pt 2",
    })).toEqual({ officeName: "City Council Member", seat: "At-Large 2" });
    expect(resolveHoustonElectionOfficeTarget({
      officeCanonicalName: "City Council Member",
      officialBallotTitle: "City Council Member",
    })).toBeNull();
    expect(resolveHoustonElectionOfficeTarget({
      officeCanonicalName: "City Council Member",
      officialBallotTitle: "City Council Member Position 3",
    })).toBeNull();
  });

  it("maps live disclosure and eFile office forms", () => {
    expect(parseHoustonDisclosureOfficeTarget("City Council - District C"))
      .toEqual({ officeName: "City Council Member", seat: "District C" });
    expect(parseHoustonEfileOfficeTarget("CCM_AL2"))
      .toEqual({ officeName: "City Council Member", seat: "At-Large 2" });
    expect(parseHoustonEfileOfficeTarget("CONTROLLER"))
      .toEqual({ officeName: "Municipal Controller", seat: "Houston" });
    expect(parseHoustonDisclosureOfficeTarget("Mayor Controller")).toBeNull();
    expect(parseStoredHoustonFinanceOfficeTarget({ officeName: "City Council Member", district: "District B" }))
      .toEqual({ officeName: "City Council Member", seat: "District B" });
  });
});
