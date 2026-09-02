import { describe, expect, it } from "vitest";

import {
  idahoCommissionerDistrictFromBallotTitle,
  idahoCountyFromDistrictName,
  idahoRegistrationRowName,
  resolveIdahoCandidateFiler,
  type IdahoCandidateFilerResolverInput,
} from "../../../src/pipeline/idahoFinance/idahoCandidateFilerResolver.js";
import { GUID_A, GUID_B, GUID_C, registration } from "./idahoTestFixtures.js";

const SENATE_16: Omit<IdahoCandidateFilerResolverInput, "registrations" | "candidateNames"> = {
  officeScope: "state_upper",
  officeName: "State Senator",
  district: "State Senate District 16 (2024); Idaho",
  legislativeDistrict: 16,
  ballotTitle: "State Senator District 16",
  electionYear: 2026,
};

const ADA_COUNTY: Omit<IdahoCandidateFilerResolverInput, "registrations" | "candidateNames" | "officeName"> = {
  officeScope: "county",
  district: "Ada County, Idaho",
  legislativeDistrict: null,
  ballotTitle: null,
  electionYear: 2026,
};

describe("resolveIdahoCandidateFiler", () => {
  it("links a State Senate candidate by exact name, office, and district", () => {
    const resolution = resolveIdahoCandidateFiler({
      ...SENATE_16,
      candidateNames: ["Todd Achilles"],
      registrations: [
        registration({ registrationGuid: GUID_A }),
        // Same person, other district: district evidence excludes it.
        registration({ registrationGuid: GUID_B, filerRegistrationId: 2, district: "Legislative District 17" }),
        // Same name, other office.
        registration({ registrationGuid: GUID_C, filerRegistrationId: 3, office: "State Representative", seatZone: "A" }),
      ],
    });
    expect(resolution).toEqual({
      status: "matched",
      match: {
        registrationGuid: GUID_A,
        filerEntityId: 257,
        filerRegistrationId: 1698,
        filerName: "Achilles, Todd Baker",
        status: "Active",
        officeName: "State Senator",
        district: "16",
        confidence: "name_exact",
        source: "sunshine_grid",
        sourceUrl: `https://sunshine.voteidaho.gov/public/cf/candidateprofile?guid=${GUID_A}&tabName=CAN&isLegacy=false`,
      },
    });
  });

  it("labels House matches with the seat and only links the race's election year", () => {
    const house = {
      ...SENATE_16,
      officeScope: "state_lower",
      officeName: "State Lower Chamber Legislator",
      district: "State House District 16 (2024); Idaho",
    };
    const rows = [
      registration({ registrationGuid: GUID_A, office: "State Representative", seatZone: "B" }),
      registration({ registrationGuid: GUID_B, filerRegistrationId: 254, office: "State Representative", seatZone: "B", electionYear: 2024, status: "Terminated", statusCode: "TERMN" }),
    ];
    const matched = resolveIdahoCandidateFiler({ ...house, candidateNames: ["Todd Achilles"], registrations: rows });
    expect(matched.status).toBe("matched");
    expect(matched.status === "matched" && matched.match.district).toBe("16B");

    expect(resolveIdahoCandidateFiler({ ...house, candidateNames: ["Todd Achilles"], registrations: [rows[1]!] })).toEqual({
      status: "unmatched",
      reason: "no_registration_match",
    });
  });

  it("requires the legislative district number", () => {
    expect(
      resolveIdahoCandidateFiler({
        ...SENATE_16,
        legislativeDistrict: null,
        candidateNames: ["Todd Achilles"],
        registrations: [registration({ registrationGuid: GUID_A })],
      })
    ).toEqual({ status: "unmatched", reason: "missing_required_district" });
  });

  it("rejects a middle-name conflict and a bare surname", () => {
    const rows = [registration({ registrationGuid: GUID_A })];
    expect(resolveIdahoCandidateFiler({ ...SENATE_16, candidateNames: ["Todd C. Achilles"], registrations: rows })).toEqual({
      status: "unmatched",
      reason: "no_registration_match",
    });
    expect(resolveIdahoCandidateFiler({ ...SENATE_16, candidateNames: ["Achilles"], registrations: rows })).toEqual({
      status: "unmatched",
      reason: "no_registration_match",
    });
  });

  it("uses the grid's quoted call name and one-sided nickname expansion", () => {
    const bertling = registration({
      registrationGuid: GUID_A,
      filerName: "Bertling, Timothy 'Tim' Paul",
      firstName: "Timothy",
      middleName: "Paul",
      lastName: "Bertling",
    });
    expect(idahoRegistrationRowName(bertling)).toBe("Bertling, Timothy (Tim) Paul");
    const byCallName = resolveIdahoCandidateFiler({ ...SENATE_16, candidateNames: ["Tim Bertling"], registrations: [bertling] });
    expect(byCallName.status === "matched" && byCallName.match.confidence).toBe("name_exact");

    const johnson = registration({
      registrationGuid: GUID_B,
      filerName: "Johnson, Steven",
      firstName: "Steven",
      middleName: null,
      lastName: "Johnson",
    });
    const byNickname = resolveIdahoCandidateFiler({ ...SENATE_16, candidateNames: ["Steve Johnson"], registrations: [johnson] });
    expect(byNickname.status === "matched" && byNickname.match.confidence).toBe("name_nickname");
  });

  it("stays unmatched when the roster uses a middle name as the first name", () => {
    // "Myricks II, William Eric" goes by Eric: manual link, never a guess.
    const myricks = registration({
      registrationGuid: GUID_A,
      filerName: "Myricks II, William Eric",
      firstName: "William",
      middleName: "Eric",
      lastName: "Myricks II",
      office: "Lieutenant Governor",
      districtType: "State",
      district: "Statewide",
    });
    expect(
      resolveIdahoCandidateFiler({
        officeScope: "statewide",
        officeName: "Lieutenant Governor",
        district: "Idaho",
        legislativeDistrict: null,
        ballotTitle: null,
        electionYear: 2026,
        candidateNames: ["Eric Myricks"],
        registrations: [myricks],
      })
    ).toEqual({ status: "unmatched", reason: "no_registration_match" });
  });

  it("links statewide races without a district and tries the structured name second", () => {
    const richardson = registration({
      registrationGuid: GUID_A,
      filerName: "Richardson, Marvin",
      firstName: "Marvin",
      middleName: null,
      lastName: "Richardson",
      office: "Governor",
      districtType: "State",
      district: "Statewide",
    });
    const resolution = resolveIdahoCandidateFiler({
      officeScope: "statewide",
      officeName: "Governor",
      district: "Idaho",
      legislativeDistrict: null,
      ballotTitle: null,
      electionYear: 2026,
      candidateNames: ["Pro-Life (A person formerly known as Marvin Richardson)", "Marvin Richardson"],
      registrations: [richardson],
    });
    expect(resolution.status).toBe("matched");
    expect(resolution.status === "matched" && resolution.match.district).toBeNull();
  });

  it("links county offices by jurisdiction and commissioners by the ballot-title seat", () => {
    const beck = registration({
      registrationGuid: GUID_A,
      filerName: "Beck, Rodney William",
      firstName: "Rodney",
      middleName: "William",
      lastName: "Beck",
      office: "County Commissioner",
      districtType: "County",
      district: "Ada County",
      jurisdiction: "Ada",
      seatZone: "2",
    });
    const davidson = registration({
      ...beck,
      registrationGuid: GUID_B,
      filerRegistrationId: 2,
      filerName: "Davidson, Ryan",
      firstName: "Ryan",
      middleName: null,
      lastName: "Davidson",
      seatZone: "1",
    });
    const kootenaiBeck = registration({ ...beck, registrationGuid: GUID_C, filerRegistrationId: 3, district: "Kootenai County", jurisdiction: "Kootenai" });
    const commissioner = {
      ...ADA_COUNTY,
      officeName: "County Commissioner",
      ballotTitle: "County Commissioner District 2",
      // The live roster spells him "Rod W. Beck"; ROD has no nickname family, so
      // that spelling is a manual link. The initial corroborates the middle name.
      candidateNames: ["Rodney W. Beck"],
      registrations: [beck, davidson, kootenaiBeck],
    };
    const resolution = resolveIdahoCandidateFiler(commissioner);
    expect(resolution.status === "matched" && resolution.match).toMatchObject({
      registrationGuid: GUID_A,
      district: "Ada 2",
      confidence: "name_exact",
    });
    expect(resolveIdahoCandidateFiler({ ...commissioner, ballotTitle: "County Commissioner" })).toEqual({
      status: "unmatched",
      reason: "missing_required_district",
    });

    const clerk = registration({
      registrationGuid: GUID_B,
      filerName: "Tripple, Trent",
      firstName: "Trent",
      middleName: null,
      lastName: "Tripple",
      office: "Clerk",
      districtType: "County",
      district: "Ada County",
      jurisdiction: "Ada",
    });
    for (const officeName of ["Clerk of Court", "County Clerk"]) {
      const clerkResolution = resolveIdahoCandidateFiler({
        ...ADA_COUNTY,
        officeName,
        candidateNames: ["Trent Tripple"],
        registrations: [clerk],
      });
      expect(clerkResolution.status === "matched" && clerkResolution.match.district).toBe("Ada");
    }
  });

  it("links only an Active registration: skips a terminated re-registration, reports lone non-Active and double-Active", () => {
    const terminated = registration({ registrationGuid: GUID_A, filerRegistrationId: 321, status: "Terminated", statusCode: "TERMN" });
    const active = registration({ registrationGuid: GUID_B, filerRegistrationId: 2748 });
    const resolved = resolveIdahoCandidateFiler({ ...SENATE_16, candidateNames: ["Todd Achilles"], registrations: [active, terminated] });
    expect(resolved.status === "matched" && resolved.match.registrationGuid).toBe(GUID_B);

    // A lone terminated or inactive registration never links automatically.
    for (const lone of [terminated, registration({ registrationGuid: GUID_C, status: "Inactive", statusCode: "INACT" })]) {
      expect(resolveIdahoCandidateFiler({ ...SENATE_16, candidateNames: ["Todd Achilles"], registrations: [lone] })).toEqual({
        status: "unmatched",
        reason: "no_active_registration",
      });
    }

    const secondActive = registration({ registrationGuid: GUID_C, filerRegistrationId: 2968 });
    const ambiguous = resolveIdahoCandidateFiler({
      ...SENATE_16,
      candidateNames: ["Todd Achilles"],
      registrations: [secondActive, terminated, active],
    });
    expect(ambiguous.status).toBe("ambiguous");
    expect(ambiguous.status === "ambiguous" && ambiguous.matches.map((match) => match.filerRegistrationId)).toEqual([
      2748, 2968,
    ]);
  });

  it("fails closed on unsupported offices and empty names", () => {
    const rows = [registration({ registrationGuid: GUID_A })];
    expect(
      resolveIdahoCandidateFiler({ ...ADA_COUNTY, officeName: "District Attorney", candidateNames: ["Todd Achilles"], registrations: rows })
    ).toEqual({ status: "unmatched", reason: "unsupported_office" });
    expect(resolveIdahoCandidateFiler({ ...SENATE_16, candidateNames: [" "], registrations: rows })).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
    });
  });

  it("parses county names and commissioner seats", () => {
    expect(idahoCountyFromDistrictName("Bear Lake County, Idaho")).toBe("Bear Lake");
    expect(idahoCountyFromDistrictName("Idaho")).toBeNull();
    expect(idahoCommissionerDistrictFromBallotTitle("Blaine County Commissioner District 2")).toBe(2);
    expect(idahoCommissionerDistrictFromBallotTitle("County Commissioner")).toBeNull();
    expect(idahoCommissionerDistrictFromBallotTitle(null)).toBeNull();
  });
});
