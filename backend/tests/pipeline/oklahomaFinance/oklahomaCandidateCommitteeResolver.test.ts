import { describe, expect, it } from "vitest";

import {
  normalizeOklahomaCandidateNameKeys,
  resolveOklahomaCandidateCommittee,
} from "../../../src/pipeline/oklahomaFinance/oklahomaCandidateCommitteeResolver.js";
import type { OklahomaGuardianContributionRow } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionReader.js";

function contribution(overrides: Partial<OklahomaGuardianContributionRow> = {}): OklahomaGuardianContributionRow {
  return {
    "Receipt ID": "1001",
    "Org ID": "11954",
    "Receipt Type": "Contribution",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "250.00",
    Description: "",
    "Receipt Source Type": "Individual",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Oklahoma City",
    State: "OK",
    Zip: "73102",
    "Filed Date": "02/01/2026",
    "Committee Type": "Candidate Committee",
    "Committee Name": "Dishman for Senate",
    "Candidate Name": "C. BRENT DISHMAN",
    Amended: "",
    Employer: "Acme",
    Occupation: "Attorney",
    ...overrides,
  };
}

describe("oklahomaCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and initial-first candidate names without fuzzy matching", () => {
    expect([...normalizeOklahomaCandidateNameKeys("DISHMAN, C. Brent")]).toEqual([
      "DISHMAN C BRENT",
      "C BRENT DISHMAN",
      "C DISHMAN",
      "BRENT DISHMAN",
    ]);
    expect([...normalizeOklahomaCandidateNameKeys("C. Brent Dishman")]).toEqual([
      "C BRENT DISHMAN",
      "C DISHMAN",
      "BRENT DISHMAN",
    ]);
  });

  it("matches exactly one Oklahoma candidate committee by candidate and cycle", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2026,
        sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        contributionRows: [
          contribution(),
          contribution({ "Org ID": "99999", "Candidate Name": "OTHER CANDIDATE" }),
          contribution({ "Org ID": "88888", "Committee Type": "PAC" }),
          contribution({ "Org ID": "77777", "Receipt Date": "01/10/2024" }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "11954",
      committeeName: "Dishman for Senate",
      confidence: "exact",
      source: "guardian_bulk",
      sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
      matchedContributionRowCount: 1,
    });
  });

  it("falls back to candidate name when Guardian omits committee name", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2026,
        contributionRows: [contribution({ "Committee Name": "" })],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "11954",
      committeeName: "C. BRENT DISHMAN",
    });
  });

  it("accepts safe Oklahoma canonical and source-like office names", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "State Auditor and Inspector",
        electionYear: 2026,
        contributionRows: [
          contribution({
            "Org ID": "2001",
            "Committee Name": "Doe for Auditor",
            "Candidate Name": "JANE DOE",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "2001",
      committeeName: "Doe for Auditor",
    });
  });

  it("requires districts for Oklahoma legislative offices because Guardian contribution rows do not prove district", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        contributionRows: [contribution()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "BRENT DISHMAN",
      officeNameNormalized: "State Senator",
    });
  });

  it("does not guess when multiple candidate committees match", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2026,
        contributionRows: [
          contribution(),
          contribution({
            "Org ID": "11955",
            "Committee Name": "Friends of Brent Dishman",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "BRENT DISHMAN",
      officeNameNormalized: "State Senator",
      matches: [
        {
          committeeId: "11954",
          committeeName: "Dishman for Senate",
          confidence: "exact",
          source: "guardian_bulk",
          sourceUrl: null,
          matchedContributionRowCount: 1,
        },
        {
          committeeId: "11955",
          committeeName: "Friends of Brent Dishman",
          confidence: "exact",
          source: "guardian_bulk",
          sourceUrl: null,
          matchedContributionRowCount: 1,
        },
      ],
    });
  });

  it("returns unmatched for unsupported offices or missing names", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Corporation Commissioner",
        electionYear: 2026,
        contributionRows: [contribution({ "Candidate Name": "JANE DOE" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "CORPORATION COMMISSIONER",
    });

    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRows: [contribution()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Governor",
    });
  });

  it("returns unmatched when candidate, committee type, or cycle does not match", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2026,
        contributionRows: [contribution({ "Candidate Name": "Other Person" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2026,
        contributionRows: [contribution({ "Committee Type": "Political Action Committee" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2026,
        contributionRows: [contribution({ "Receipt Date": "01/10/2024" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("does not fuzzy-match candidate typos", () => {
    expect(
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishmann",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2026,
        contributionRows: [contribution()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: "BRENT DISHMANN",
      officeNameNormalized: "State Senator",
    });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveOklahomaCandidateCommittee({
        candidateName: "Brent Dishman",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
        electionYear: 2013,
        contributionRows: [],
      })
    ).toThrow("Invalid Oklahoma candidate committee election year");
  });
});
