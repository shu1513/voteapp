import { describe, expect, it } from "vitest";

import type { WestVirginiaCommitteeRow } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsClient.js";
import {
  normalizeWestVirginiaCandidateNameForStorage,
  resolveWestVirginiaCandidateCommittee,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaCandidateCommitteeResolver.js";

// Registry shapes pinned live 2026-09-01.
function committee(overrides: Partial<WestVirginiaCommitteeRow>): WestVirginiaCommitteeRow {
  return {
    orgID: 2929,
    entityId: "1010002929",
    orgName: "Friends of Mike Oliverio",
    candidateName: "Oliverio, Michael Angelo II",
    orgType: "State Candidate",
    orgTypeCode: "101",
    orgSubType: "Candidate",
    office: "State Senator",
    district: "13",
    party: "Republican",
    election: "2026 Election",
    registrationYear: "2025",
    orgStatus: "Active",
    ...overrides,
  };
}

const base = {
  electionYear: 2026,
  registryOffice: "State Senator" as const,
  districtNumber: 13,
};

describe("resolveWestVirginiaCandidateCommittee", () => {
  it("matches a nickname roster name against the legal registry name with a suffix", () => {
    const resolution = resolveWestVirginiaCandidateCommittee({
      ...base,
      candidateName: "Mike Oliverio",
      committees: [committee({})],
    });
    expect(resolution).toMatchObject({
      status: "matched",
      entityId: "1010002929",
      orgID: 2929,
      committeeName: "Friends of Mike Oliverio",
      registryCandidateName: "Oliverio, Michael Angelo II",
    });
  });

  it("falls back to the candidate name when the registration has no committee name", () => {
    const resolution = resolveWestVirginiaCandidateCommittee({
      ...base,
      candidateName: "Michael Oliverio",
      committees: [committee({ orgName: null })],
    });
    expect(resolution).toMatchObject({ status: "matched", committeeName: "Oliverio, Michael Angelo II" });
  });

  it("requires the exact office, seat, election cycle and org type", () => {
    const wrongSeat = committee({ district: "14" });
    const wrongOffice = committee({ office: "House of Delegates" });
    const wrongCycle = committee({ election: "2024 Election" });
    const pac = committee({ orgType: "State Political Action Committee", orgTypeCode: "102" });
    for (const row of [wrongSeat, wrongOffice, wrongCycle, pac]) {
      expect(
        resolveWestVirginiaCandidateCommittee({ ...base, candidateName: "Michael Oliverio", committees: [row] })
      ).toEqual({ status: "unmatched", reason: "no_matching_committee" });
    }
  });

  it("never links on a surname alone or across a middle-name conflict", () => {
    expect(
      resolveWestVirginiaCandidateCommittee({ ...base, candidateName: "Oliverio", committees: [committee({})] })
    ).toEqual({ status: "unmatched", reason: "no_matching_committee" });
    expect(
      resolveWestVirginiaCandidateCommittee({
        ...base,
        candidateName: "Michael B. Oliverio",
        committees: [committee({})],
      })
    ).toEqual({ status: "unmatched", reason: "no_matching_committee" });
    expect(
      resolveWestVirginiaCandidateCommittee({ ...base, candidateName: "  ", committees: [committee({})] })
    ).toEqual({ status: "unmatched", reason: "missing_candidate_name" });
  });

  it("reports a re-registered pair for the same seat as ambiguous instead of picking one", () => {
    // Live 2026 shape: Beck, Marta Maria | House of Delegates | 98 —
    // 1010003604 Terminated (2025) + 1010003840 Active (2026).
    const resolution = resolveWestVirginiaCandidateCommittee({
      electionYear: 2026,
      registryOffice: "House of Delegates",
      districtNumber: 98,
      candidateName: "Marta Beck",
      committees: [
        committee({
          orgID: 3840,
          entityId: "1010003840",
          orgName: "Beck for Delegate",
          candidateName: "Beck, Marta Maria",
          office: "House of Delegates",
          district: "98",
          registrationYear: "2026",
        }),
        committee({
          orgID: 3604,
          entityId: "1010003604",
          orgName: "Friends of Marta Beck",
          candidateName: "Beck, Marta Maria",
          office: "House of Delegates",
          district: "98",
          orgStatus: "Terminated",
        }),
      ],
    });
    expect(resolution).toMatchObject({ status: "ambiguous", reason: "multiple_matching_committees" });
    expect((resolution as { matches: { entityId: string }[] }).matches.map((match) => match.entityId)).toEqual([
      "1010003604",
      "1010003840",
    ]);
  });

  it("normalizes stored names the Delaware way", () => {
    expect(normalizeWestVirginiaCandidateNameForStorage("José M. O'Neill-Smith, Jr.")).toBe("JOSE M O NEILL SMITH JR");
  });
});
