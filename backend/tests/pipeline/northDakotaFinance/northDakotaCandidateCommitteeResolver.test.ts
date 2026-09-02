import { describe, expect, it } from "vitest";

import type { NorthDakotaCommitteeRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsClient.js";
import {
  normalizeNorthDakotaCandidateNameForStorage,
  northDakotaRegistryElectionLabel,
  resolveNorthDakotaCandidateCommittee,
  stripNorthDakotaHonorific,
} from "../../../src/pipeline/northDakotaFinance/northDakotaCandidateCommitteeResolver.js";

// Registry shapes pinned live 2026-09-01.
function committee(overrides: Partial<NorthDakotaCommitteeRow>): NorthDakotaCommitteeRow {
  return {
    orgID: 1478,
    entityId: "1010001478",
    orgName: "Friends of Jamie Selzler",
    candidateName: "Mr. Selzler, Jamie",
    orgType: "Candidate/Candidate Committee",
    orgTypeCode: "101",
    orgSubType: "Candidate Committee",
    orgSubTypeCode: "CNCM",
    election: "2026 Election - Statewide",
    office: "State Senator",
    district: "District 44",
    party: "North Dakota Democratic-NPL Party",
    orgStatus: "Active",
    registrationYear: "2026",
    ...overrides,
  };
}

const senate = { registryOffice: "State Senator" as const, districted: true };
const taxCommissioner = { registryOffice: "Tax Commissioner" as const, districted: false };
const base = { electionYear: 2026, office: senate, districtNumber: 44 };

describe("resolveNorthDakotaCandidateCommittee", () => {
  it("matches through an honorific and reports the committee identity", () => {
    expect(
      resolveNorthDakotaCandidateCommittee({ ...base, candidateName: "Jamie Selzler", committees: [committee({})] })
    ).toMatchObject({
      status: "matched",
      entityId: "1010001478",
      orgID: 1478,
      committeeName: "Friends of Jamie Selzler",
      registryCandidateName: "Mr. Selzler, Jamie",
      orgStatus: "Active",
      party: "North Dakota Democratic-NPL Party",
    });
  });

  it("matches a nickname roster name against the legal name and strips the honorific from the fallback committee name", () => {
    // CNDT filers (234 of 376 live) register without a committee name.
    const resolution = resolveNorthDakotaCandidateCommittee({
      ...base,
      candidateName: "Chris O'Riley",
      committees: [
        committee({
          orgName: null,
          orgSubType: "Candidate",
          orgSubTypeCode: "CNDT",
          candidateName: "Dr. O'Riley, Christine Ann",
          district: "District 44",
        }),
      ],
    });
    expect(resolution).toMatchObject({ status: "matched", committeeName: "O'Riley, Christine Ann" });
  });

  it("matches statewide races on office + election + name and ignores the district column", () => {
    const rows = [
      committee({ entityId: "1010002001", orgID: 2001, orgName: null, candidateName: "Nelson, Mark", office: "Tax Commissioner", district: null }),
      committee({ entityId: "1010002002", orgID: 2002, orgName: null, candidateName: "Nelson, Mark", office: "State Auditor", district: null }),
    ];
    expect(
      resolveNorthDakotaCandidateCommittee({ electionYear: 2026, office: taxCommissioner, districtNumber: null, candidateName: "Mark Nelson", committees: rows })
    ).toMatchObject({ status: "matched", entityId: "1010002001", committeeName: "Nelson, Mark" });
  });

  it("requires the exact office, seat, election cycle and org type", () => {
    const wrongSeat = committee({ district: "District 45" });
    const wrongOffice = committee({ office: "State Representative" });
    const wrongCycle = committee({ election: "2028 Election - Statewide" });
    const pac = committee({ orgType: "Committee/PAC", orgTypeCode: "102" });
    const noName = committee({ candidateName: null });
    for (const row of [wrongSeat, wrongOffice, wrongCycle, pac, noName]) {
      expect(resolveNorthDakotaCandidateCommittee({ ...base, candidateName: "Jamie Selzler", committees: [row] })).toEqual({
        status: "unmatched",
        reason: "no_matching_committee",
      });
    }
  });

  it("never links on a surname alone or across a middle-name or suffix conflict", () => {
    expect(resolveNorthDakotaCandidateCommittee({ ...base, candidateName: "Selzler", committees: [committee({})] })).toEqual({
      status: "unmatched",
      reason: "no_matching_committee",
    });
    expect(
      resolveNorthDakotaCandidateCommittee({
        ...base,
        candidateName: "Donald B. Lippert",
        committees: [committee({ candidateName: "Mr. Lippert, Donald A. Jr." })],
      })
    ).toEqual({ status: "unmatched", reason: "no_matching_committee" });
    expect(
      resolveNorthDakotaCandidateCommittee({
        ...base,
        candidateName: "Daniel Johnston Jr.",
        committees: [committee({ candidateName: "Johnston Sr, Daniel" })],
      })
    ).toEqual({ status: "unmatched", reason: "no_matching_committee" });
    expect(resolveNorthDakotaCandidateCommittee({ ...base, candidateName: "  ", committees: [committee({})] })).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
    });
  });

  it("accepts a one-sided suffix ('Johnston Sr, Daniel' for roster 'Daniel Johnston')", () => {
    expect(
      resolveNorthDakotaCandidateCommittee({
        ...base,
        candidateName: "Daniel Johnston",
        committees: [committee({ candidateName: "Johnston Sr, Daniel" })],
      })
    ).toMatchObject({ status: "matched" });
  });

  it("reports two committees for the same seat as ambiguous instead of picking one", () => {
    const resolution = resolveNorthDakotaCandidateCommittee({
      ...base,
      candidateName: "Jamie Selzler",
      committees: [committee({}), committee({ entityId: "1010000099", orgID: 99, orgStatus: "Inactive", registrationYear: "2022" })],
    });
    expect(resolution).toMatchObject({ status: "ambiguous", reason: "multiple_matching_committees" });
    expect((resolution as { matches: { entityId: string }[] }).matches.map((match) => match.entityId)).toEqual([
      "1010000099",
      "1010001478",
    ]);
  });

  it("refuses to resolve a districted office without a district number", () => {
    expect(() =>
      resolveNorthDakotaCandidateCommittee({ ...base, districtNumber: null, candidateName: "Jamie Selzler", committees: [] })
    ).toThrow(/requires a district number/);
  });

  it("normalizes stored names the Delaware way and strips honorifics", () => {
    expect(normalizeNorthDakotaCandidateNameForStorage("José M. O'Neill-Smith, Jr.")).toBe("JOSE M O NEILL SMITH JR");
    expect(stripNorthDakotaHonorific("Hon. Tufte, Jerod")).toBe("Tufte, Jerod");
    expect(stripNorthDakotaHonorific("Tuttle, Charles")).toBe("Tuttle, Charles");
    expect(northDakotaRegistryElectionLabel(2026)).toBe("2026 Election - Statewide");
  });
});
