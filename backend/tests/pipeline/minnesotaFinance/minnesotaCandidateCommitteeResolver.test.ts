import { describe, expect, it } from "vitest";

import {
  isMinnesotaFinanceEligibleOffice,
  mapMinnesotaFinanceOffice,
  normalizeMinnesotaFinanceDistrict,
  normalizeMinnesotaFinanceOfficeName,
} from "../../../src/pipeline/minnesotaFinance/minnesotaFinanceEligibleOffices.js";
import {
  normalizeMinnesotaCandidateNameKeys,
  resolveMinnesotaCandidateCommittee,
} from "../../../src/pipeline/minnesotaFinance/minnesotaCandidateCommitteeResolver.js";
import type { MinnesotaCampaignFinanceCsvRow } from "../../../src/pipeline/minnesotaFinance/minnesotaCampaignFinanceArtifactReader.js";

function record(overrides: Partial<MinnesotaCampaignFinanceCsvRow> = {}): MinnesotaCampaignFinanceCsvRow {
  return {
    "Committee ID": "1001",
    "Committee Name": "FRIENDS OF JANE DOE",
    Candidate: "Jane Doe",
    Office: "Governor",
    District: "",
    Status: "Active",
    Year: "2026",
    ...overrides,
  };
}

describe("Minnesota finance eligible offices", () => {
  it("normalizes office names and districts conservatively", () => {
    expect(normalizeMinnesotaFinanceOfficeName("State Representative")).toBe("State Lower Chamber Legislator");
    expect(normalizeMinnesotaFinanceDistrict(" 07 ")).toBe("7");
    expect(mapMinnesotaFinanceOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toEqual(
      expect.objectContaining({
        officeScope: "statewide",
        officeName: "Governor",
        requiresDistrict: false,
        district: null,
      })
    );
    expect(
      isMinnesotaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Representative",
        district: "7",
      })
    ).toBe(
      true
    );
  });
});

describe("Minnesota candidate committee resolver", () => {
  it("normalizes candidate names without broad fuzzy matching", () => {
    expect([...normalizeMinnesotaCandidateNameKeys("Doe, Jane A.")]).toEqual([
      "DOE JANE A",
      "JANE A DOE",
      "JANE DOE",
    ]);
  });

  it("matches exactly one candidate committee by candidate name and office", () => {
    expect(
      resolveMinnesotaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        sourceUrl: "https://register.cfb.mn.gov/reports-and-data/viewers/campaign-finance/candidates/0/2026/",
        candidateRows: [
          record(),
          record({
            "Committee ID": "9999",
            "Committee Name": "Other Committee",
            Candidate: "Other Candidate",
            Office: "Governor",
          }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "1001",
      committeeName: "FRIENDS OF JANE DOE",
      confidence: "exact",
      source: "mn_board_viewer",
      sourceUrl: "https://register.cfb.mn.gov/reports-and-data/viewers/campaign-finance/candidates/0/2026/",
      matchedCandidateRowCount: 1,
    });
  });

  it("requires legislative districts for state senate and house candidates", () => {
    expect(
      resolveMinnesotaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        candidateRows: [record({ Office: "State Senator", District: "7" })],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "missing_legislative_district",
    });

    expect(
      resolveMinnesotaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "07",
        electionYear: 2026,
        candidateRows: [record({ Office: "State Senator", District: "7" })],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "1001",
    });
  });

  it("skips other districts and unsupported offices", () => {
    expect(
      resolveMinnesotaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "8",
        electionYear: 2026,
        candidateRows: [record({ Office: "State Senator", District: "7" })],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });

    expect(
      resolveMinnesotaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "President",
        electionYear: 2026,
        candidateRows: [record({ Office: "Governor" })],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "unsupported_office",
    });
  });

  it("skips rows from the wrong election year when that year is present", () => {
    expect(
      resolveMinnesotaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRows: [record({ Year: "2024" })],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });

  it("does not guess when multiple committees match", () => {
    expect(
      resolveMinnesotaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        candidateRows: [
          record(),
          record({
            "Committee ID": "1002",
            "Committee Name": "JANE DOE FOR GOVERNOR TRANSITION",
            Candidate: "Jane Doe",
            Office: "Governor",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Governor",
      matches: [
        {
          committeeId: "1001",
          committeeName: "FRIENDS OF JANE DOE",
          confidence: "exact",
          source: "mn_board_viewer",
          sourceUrl: null,
          matchedCandidateRowCount: 1,
        },
        {
          committeeId: "1002",
          committeeName: "JANE DOE FOR GOVERNOR TRANSITION",
          confidence: "exact",
          source: "mn_board_viewer",
          sourceUrl: null,
          matchedCandidateRowCount: 1,
        },
      ],
    });
  });
});
