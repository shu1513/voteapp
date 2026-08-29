import { describe, expect, it } from "vitest";

import {
  alabamaFcpaElectionCycleIdForYear,
  alabamaFcpaOfficeIdForLabel,
  alabamaFcpaOfficeLabelForRace,
  isAlabamaFinanceEligibleOffice,
} from "../../../src/pipeline/alabamaFinance/alabamaFinanceEligibleOffices.js";

describe("Alabama finance eligible offices", () => {
  it("gates offices to the v1 allowlist", () => {
    expect(
      isAlabamaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })
    ).toBe(true);
    expect(
      isAlabamaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);
    // Federal races are FEC-filed; county offices are out of v1 scope.
    expect(
      isAlabamaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "United States Senator",
      })
    ).toBe(false);
    expect(
      isAlabamaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })
    ).toBe(false);
    expect(isAlabamaFinanceEligibleOffice({ officeScope: null, officeCanonicalName: "Governor" })).toBe(
      false
    );
  });

  it("maps VoteApp offices to FCPA race-search labels", () => {
    expect(
      alabamaFcpaOfficeLabelForRace({
        officeScope: "statewide",
        officeCanonicalName: "Lieutenant Governor",
        ballotTitle: "Lieutenant Governor",
      })
    ).toBe("Lt. Governor");
    expect(
      alabamaFcpaOfficeLabelForRace({
        officeScope: "statewide",
        officeCanonicalName: "Commissioner of Agriculture",
        ballotTitle: "Commissioner of Agriculture and Industries",
      })
    ).toBe("Commissioner of Agriculture & Industries");
    expect(
      alabamaFcpaOfficeLabelForRace({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        ballotTitle: "State Representative, District 68",
      })
    ).toBe("State Representative");
  });

  it("routes State Level Judge races by ballot title", () => {
    const judge = (ballotTitle: string) =>
      alabamaFcpaOfficeLabelForRace({
        officeScope: "statewide",
        officeCanonicalName: "State Level Judge",
        ballotTitle,
      });
    expect(judge("Associate Justice of the Alabama Supreme Court, Place 7")).toBe(
      "Supreme Court Associate Justice"
    );
    expect(judge("Chief Justice of the Alabama Supreme Court")).toBe("Supreme Court Chief Justice");
    expect(judge("Alabama Court of Civil Appeals, Place 4")).toBe("Court of Civil Appeals Judge");
    expect(judge("Alabama Court of Criminal Appeals, Place 5")).toBe("Court of Criminal Appeals Judge");
    expect(judge("Alabama Circuit Court, Place 2")).toBeNull();
    expect(judge("")).toBeNull();
  });

  it("resolves dropdown ids, decoding the escaped ampersand", () => {
    const offices = [
      { id: "10", label: "Commissioner of Agriculture &amp; Industries" },
      { id: "23", label: "Governor" },
    ];
    expect(alabamaFcpaOfficeIdForLabel("Commissioner of Agriculture & Industries", offices)).toBe("10");
    expect(alabamaFcpaOfficeIdForLabel("Governor", offices)).toBe("23");
    expect(alabamaFcpaOfficeIdForLabel("Sheriff", offices)).toBeNull();
  });

  it("resolves the election-cycle dropdown id by year", () => {
    const elections = [
      { id: "160", label: "2026 ELECTION CYCLE" },
      { id: "167", label: "2026 2026 Municipal Election" },
    ];
    expect(alabamaFcpaElectionCycleIdForYear(2026, elections)).toBe("160");
    expect(alabamaFcpaElectionCycleIdForYear(2024, elections)).toBeNull();
  });
});
