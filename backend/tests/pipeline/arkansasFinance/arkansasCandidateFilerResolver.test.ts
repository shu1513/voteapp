import { describe, expect, it } from "vitest";

import type { ArkansasFilerRegistrationRow } from "../../../src/pipeline/arkansasFinance/arkansasCfisClient.js";
import {
  arkansasDistrictNumberFromDistrictName,
  arkansasRegistrationRowNames,
  normalizeArkansasCandidateNameForStorage,
  resolveArkansasCandidateFiler,
} from "../../../src/pipeline/arkansasFinance/arkansasCandidateFilerResolver.js";

// Live 2026 registration shape (PublicFilerDetails/GetCandidateCommitteDetails),
// names sanitized. Money fields are illustrative.
function registration(overrides: Partial<ArkansasFilerRegistrationRow> = {}): ArkansasFilerRegistrationRow {
  return {
    registrationGuid: "0b27e93d-5e84-4cad-b859-6ae20a13782f",
    filerEntityId: 7817,
    filerEntityVersionId: 1,
    filerType: "Candidate",
    filerTypeCode: "CAN",
    filerStatus: "Active",
    filerName: "Doe, Robert S.",
    firstName: "Robert",
    lastName: "Doe",
    suffix: null,
    committeeName: null,
    office: "State Representative",
    officeDistrictName: "13",
    jurisdictionName: "Arkansas",
    politicalParty: "Republican Party",
    electionYear: 2026,
    filingYear: 2026,
    isPaperFiler: false,
    totalRaised: 27800,
    totalSpent: 18656.07,
    balanceOfFunds: 31854.07,
    ...overrides,
  };
}

const houseInput = {
  candidateName: "Robert Doe",
  candidateParty: "Republican",
  officeScope: "state_lower",
  officeName: "State Lower Chamber Legislator",
  district: "State House District 13 (2024); Arkansas",
  electionYear: 2026,
  sourceUrl: "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFilerDetails/GetCandidateCommitteDetails",
} as const;

describe("arkansasCandidateFilerResolver", () => {
  it("normalizes names and district numbers", () => {
    expect(normalizeArkansasCandidateNameForStorage("Joy C. Springer-Lopez")).toBe("JOY C SPRINGER LOPEZ");
    expect(arkansasDistrictNumberFromDistrictName("State Senate District 07 (2024); Arkansas")).toBe("7");
    expect(arkansasDistrictNumberFromDistrictName("Arkansas")).toBeNull();
  });

  it("derives row names from structured fields, the declared nickname, and the anchored comma form", () => {
    expect(
      arkansasRegistrationRowNames(
        registration({ filerName: 'Doe, State Representative. James "Jay" D.', firstName: 'James "Jay"', suffix: "Ph.D." })
      )
    ).toEqual(["James Doe", "Jay Doe", 'Doe, James "Jay" D.']);
    expect(
      arkansasRegistrationRowNames(registration({ filerName: "Hawk, Mr. . Robert J., II", lastName: "Hawk", suffix: "II" }))
    ).toEqual(["Robert Hawk II", "Hawk, Robert J., II"]);
    // First name absent from the comma form: no middle evidence, no error.
    expect(arkansasRegistrationRowNames(registration({ filerName: "Doe, Mr." }))).toEqual(["Robert Doe"]);
    expect(arkansasRegistrationRowNames(registration({ firstName: null }))).toEqual([]);
  });

  it("matches on exact office, district, cycle, and name", () => {
    const resolution = resolveArkansasCandidateFiler({ ...houseInput, registrationRows: [registration()] });
    expect(resolution).toEqual({
      status: "matched",
      filingEntityId: 7817,
      registrationGuid: "0b27e93d-5e84-4cad-b859-6ae20a13782f",
      filerName: "Robert Doe",
      officialName: "Robert Doe",
      officeName: "State Representative",
      district: "13",
      politicalParty: "Republican Party",
      electionYear: 2026,
      totalRaised: 27800,
      totalSpent: 18656.07,
      balanceOfFunds: 31854.07,
      confidence: "exact",
      source: "cfis_registration",
      sourceUrl: houseInput.sourceUrl,
    });
  });

  it("prefers the committee name as the link display name", () => {
    const resolution = resolveArkansasCandidateFiler({
      ...houseInput,
      registrationRows: [registration({ committeeName: "Doe for State Representative " })],
    });
    expect(resolution).toMatchObject({ status: "matched", filerName: "Doe for State Representative" });
  });

  it("accepts a roster nickname against the legal first name, and a declared nickname against the roster", () => {
    expect(
      resolveArkansasCandidateFiler({ ...houseInput, candidateName: "Bob Doe", registrationRows: [registration()] })
    ).toMatchObject({ status: "matched", filingEntityId: 7817 });
    expect(
      resolveArkansasCandidateFiler({
        ...houseInput,
        candidateName: "Justin Doe",
        registrationRows: [registration({ filerName: 'Doe, Phillip "Justin"', firstName: 'Phillip "Justin"' })],
      })
    ).toMatchObject({ status: "matched", officialName: "Phillip Doe" });
  });

  it("uses comma-form middle initials as evidence: corroboration matches, conflict rejects", () => {
    expect(
      resolveArkansasCandidateFiler({
        ...houseInput,
        candidateName: "Robert Scott Doe",
        registrationRows: [registration({ filerName: "Doe, Governor. Robert S." })],
      })
    ).toMatchObject({ status: "matched" });
    expect(
      resolveArkansasCandidateFiler({
        ...houseInput,
        candidateName: "Robert T. Doe",
        registrationRows: [registration({ filerName: "Doe, Robert S." })],
      })
    ).toEqual({ status: "unmatched", reason: "no_candidate_filer_match", candidateNameNormalized: "ROBERT T DOE" });
  });

  it("rejects a different first name, a generational-suffix conflict, and a party conflict", () => {
    expect(
      resolveArkansasCandidateFiler({ ...houseInput, candidateName: "Scott Doe", registrationRows: [registration()] })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
    expect(
      resolveArkansasCandidateFiler({
        ...houseInput,
        candidateName: "Robert Doe Jr.",
        registrationRows: [registration({ filerName: "Doe, Robert S., Sr.", suffix: "Sr." })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
    expect(
      resolveArkansasCandidateFiler({ ...houseInput, candidateParty: "Democratic", registrationRows: [registration()] })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
    // Party is corroboration only: an unknown roster party never blocks.
    expect(
      resolveArkansasCandidateFiler({ ...houseInput, candidateParty: "Nonpartisan", registrationRows: [registration()] })
    ).toMatchObject({ status: "matched" });
  });

  it("requires the exact district, cycle year, state jurisdiction, and candidate filer type", () => {
    const rows = [
      registration({ officeDistrictName: "31" }),
      registration({ filerEntityId: 2, electionYear: 2024 }),
      registration({ filerEntityId: 3, electionYear: null }),
      registration({ filerEntityId: 4, jurisdictionName: "Lonoke" }),
      registration({ filerEntityId: 5, filerTypeCode: "SFIFILER", filerType: "SFI Filer" }),
      registration({ filerEntityId: 6, filerTypeCode: "ECOMM", filerType: "Exploratory Committee" }),
      registration({ filerEntityId: 7, office: "State Senate" }),
    ];
    expect(resolveArkansasCandidateFiler({ ...houseInput, registrationRows: rows })).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_filer_match",
    });
    expect(
      resolveArkansasCandidateFiler({ ...houseInput, district: "Arkansas", registrationRows: [registration()] })
    ).toMatchObject({ status: "unmatched", reason: "missing_required_district" });
  });

  it("matches statewide offices only against undistricted rows", () => {
    const governor = registration({
      filerEntityId: 1004,
      filerName: "Sanders, Governor. Sarah H.",
      firstName: "Sarah",
      lastName: "Sanders",
      committeeName: "Sarah for Governor",
      office: "Governor",
      officeDistrictName: null,
    });
    const input = {
      ...houseInput,
      candidateName: "Sarah Huckabee Sanders",
      officeScope: "statewide",
      officeName: "Governor",
      district: "Arkansas",
    };
    expect(resolveArkansasCandidateFiler({ ...input, registrationRows: [governor] })).toMatchObject({
      status: "matched",
      filingEntityId: 1004,
      filerName: "Sarah for Governor",
      district: null,
    });
    expect(
      resolveArkansasCandidateFiler({ ...input, registrationRows: [{ ...governor, officeDistrictName: "1" }] })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
  });

  it("fails closed on unsupported offices and blank names", () => {
    expect(
      resolveArkansasCandidateFiler({
        ...houseInput,
        officeScope: "county",
        officeName: "Justice of the Peace",
        registrationRows: [registration()],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });
    expect(resolveArkansasCandidateFiler({ ...houseInput, candidateName: " ", registrationRows: [] })).toMatchObject({
      status: "unmatched",
      reason: "missing_candidate_name",
    });
    expect(() => resolveArkansasCandidateFiler({ ...houseInput, electionYear: 0, registrationRows: [] })).toThrow(
      /election year/
    );
  });

  it("reports two matching filers as ambiguous even when only one carries money", () => {
    // Live 2026 shape: a paper-filer twin registration at $0/$0/$0. Money is
    // not identity, so the operator links by hand.
    const dormant = registration({
      filerEntityId: 7298,
      registrationGuid: "69b74574-f3e2-43fe-9c18-1305d73813c5",
      isPaperFiler: true,
      totalRaised: 0,
      totalSpent: 0,
      balanceOfFunds: 0,
    });
    const ambiguous = resolveArkansasCandidateFiler({ ...houseInput, registrationRows: [dormant, registration()] });
    expect(ambiguous).toMatchObject({ status: "ambiguous", reason: "multiple_matching_filers" });
    if (ambiguous.status !== "ambiguous") throw new Error("expected ambiguity");
    expect(ambiguous.matches.map((match) => [match.filingEntityId, match.totalRaised])).toEqual([
      [7298, 0],
      [7817, 27800],
    ]);
    expect(() =>
      resolveArkansasCandidateFiler({ ...houseInput, registrationRows: [registration(), registration()] })
    ).toThrow(/twice/);
  });
});
