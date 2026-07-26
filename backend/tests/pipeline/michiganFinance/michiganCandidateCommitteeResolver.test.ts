import { describe, expect, it } from "vitest";

import {
  normalizeMichiganCandidateNameKeys,
  resolveMichiganCandidateCommittee,
} from "../../../src/pipeline/michiganFinance/michiganCandidateCommitteeResolver.js";
import type { MichiganMitnLegacyContributionRow } from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

function contributionRow(
  overrides: Partial<MichiganMitnLegacyContributionRow> = {}
): MichiganMitnLegacyContributionRow {
  return {
    doc_seq_no: "100",
    page_no: "1",
    contribution_id: "200",
    cont_detail_id: "300",
    doc_stmnt_year: "2022",
    doc_type_desc: "Post-General",
    com_legal_name: "WHITMER FOR GOVERNOR",
    common_name: "Whitmer for Governor",
    cfr_com_id: "514456",
    com_type: "CAN",
    can_first_name: "GRETCHEN",
    can_last_name: "WHITMER",
    contribtype: "IND",
    f_name: "JANE",
    l_name_or_org: "DOE",
    address: "",
    city: "",
    state: "MI",
    zip: "",
    occupation: "ATTORNEY",
    employer: "LAW FIRM",
    received_date: "10/01/2022",
    amount: "250.00",
    aggregate: "250.00",
    extra_desc: "",
    ...overrides,
  };
}

describe("michiganCandidateCommitteeResolver", () => {
  it("normalizes direct and comma-form candidate names without broad fuzzy matching", () => {
    expect([...normalizeMichiganCandidateNameKeys("WHITMER, Gretchen E.")]).toEqual([
      "WHITMER GRETCHEN E",
      "GRETCHEN E WHITMER",
      "GRETCHEN WHITMER",
    ]);
    expect([...normalizeMichiganCandidateNameKeys("Gretchen E. Whitmer")]).toEqual([
      "GRETCHEN E WHITMER",
      "GRETCHEN WHITMER",
    ]);
  });

  it("matches exactly one Michigan candidate committee by structured candidate name", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Gretchen Whitmer",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        sourceUrl: "https://www.michigan.gov/sos/example/2022_mi_cfr.7z",
        contributionRows: [
          contributionRow(),
          contributionRow({
            cfr_com_id: "999999",
            com_legal_name: "OTHER FOR GOVERNOR",
            can_first_name: "OTHER",
            can_last_name: "PERSON",
          }),
          contributionRow({
            cfr_com_id: "520012",
            com_legal_name: "GET MICHIGAN WORKING AGAIN (SUPERPAC)",
            common_name: "Get Michigan Working Again",
            com_type: "IND",
          }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "514456",
      committeeName: "WHITMER FOR GOVERNOR",
      commonName: "Whitmer for Governor",
      confidence: "exact",
      source: "mitn_legacy",
      sourceUrl: "https://www.michigan.gov/sos/example/2022_mi_cfr.7z",
      matchedContributionRowCount: 1,
    });
  });

  it("links a committee whose name carries no office text at all", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Angela M. Jones",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "37",
        electionYear: 2026,
        contributionRows: [
          contributionRow({
            cfr_com_id: "521649",
            com_legal_name: "ANGELA JONES COMMITTEE TO ELECT",
            common_name: "",
            can_first_name: "ANGELA",
            can_last_name: "JONES",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "521649",
    });
  });

  it("vetoes a same-name committee whose name claims a different office", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "40",
        electionYear: 2026,
        contributionRows: [
          contributionRow({
            cfr_com_id: "3001",
            com_legal_name: "JANE DOE FOR STATE SENATE",
            can_first_name: "JANE",
            can_last_name: "DOE",
          }),
        ],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });

  it("allows an office-mover's committee named for the candidate's current office", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jocelyn Benson",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        currentOffice: "Michigan Secretary of State",
        contributionRows: [
          contributionRow({
            cfr_com_id: "514336",
            com_legal_name: "JOCELYN BENSON FOR SECRETARY OF STATE",
            can_first_name: "JOCELYN",
            can_last_name: "BENSON",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "514336",
    });
  });

  it("vetoes a committee claiming a conflicting legislative district", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "7",
        electionYear: 2026,
        contributionRows: [
          contributionRow({
            cfr_com_id: "3001",
            com_legal_name: "JANE DOE FOR SENATE DISTRICT 12",
            can_first_name: "JANE",
            can_last_name: "DOE",
          }),
        ],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });

  it("accepts a district claim matching the race even when zero-padded", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "7",
        electionYear: 2026,
        contributionRows: [
          contributionRow({
            cfr_com_id: "3001",
            com_legal_name: "JANE DOE FOR SENATE DISTRICT 07",
            can_first_name: "JANE",
            can_last_name: "DOE",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "3001",
    });
  });

  it("accepts gubernatorial GUB committees and refuses IND rows carrying a candidate name", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Gretchen Whitmer",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        contributionRows: [
          contributionRow({ com_type: "GUB" }),
          contributionRow({
            cfr_com_id: "888888",
            com_legal_name: "SOME INDEPENDENT SPENDER",
            com_type: "IND",
            can_first_name: "GRETCHEN",
            can_last_name: "WHITMER",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "514456",
    });
  });

  it("requires valid districts for legislative offices before matching", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2022,
        contributionRows: [
          contributionRow({
            cfr_com_id: "3001",
            com_legal_name: "JANE DOE FOR SENATE DISTRICT 7",
            can_first_name: "JANE",
            can_last_name: "DOE",
          }),
        ],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "STATE SENATOR",
    });

    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "SD 7",
        electionYear: 2022,
        contributionRows: [
          contributionRow({
            cfr_com_id: "3001",
            com_legal_name: "JANE DOE FOR SENATE DISTRICT 7",
            can_first_name: "JANE",
            can_last_name: "DOE",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "3001",
    });
  });

  it("skips same-name legislative committees from other districts", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "SD 7",
        electionYear: 2022,
        contributionRows: [
          contributionRow({
            cfr_com_id: "3001",
            com_legal_name: "JANE DOE FOR SENATE DISTRICT 8",
            can_first_name: "JANE",
            can_last_name: "DOE",
          }),
        ],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });

  it("rejects unsupported offices without trying to infer from committee names", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "State Treasurer",
        electionYear: 2022,
        contributionRows: [
          contributionRow({
            cfr_com_id: "4001",
            com_legal_name: "JANE DOE FOR TREASURER",
            can_first_name: "JANE",
            can_last_name: "DOE",
          }),
        ],
      })
    ).toEqual({
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "STATE TREASURER",
    });
  });

  it("does not match rows without structured candidate names", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Gretchen Whitmer",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        contributionRows: [
          contributionRow({
            can_first_name: "",
            can_last_name: "",
            com_legal_name: "WHITMER FOR GOVERNOR",
          }),
        ],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: "GRETCHEN WHITMER",
      officeNameNormalized: "Governor",
    });
  });

  it("does not guess when multiple candidate committees match", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Gretchen Whitmer",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        contributionRows: [
          contributionRow(),
          contributionRow({
            cfr_com_id: "514457",
            com_legal_name: "GRETCHEN WHITMER FOR GOVERNOR TRANSITION",
            common_name: "Whitmer Transition",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "GRETCHEN WHITMER",
      officeNameNormalized: "Governor",
      matches: [
        {
          committeeId: "514456",
          committeeName: "WHITMER FOR GOVERNOR",
          commonName: "Whitmer for Governor",
          confidence: "exact",
          source: "mitn_legacy",
          sourceUrl: null,
          matchedContributionRowCount: 1,
        },
        {
          committeeId: "514457",
          committeeName: "GRETCHEN WHITMER FOR GOVERNOR TRANSITION",
          commonName: "Whitmer Transition",
          confidence: "exact",
          source: "mitn_legacy",
          sourceUrl: null,
          matchedContributionRowCount: 1,
        },
      ],
    });
  });

  it("allows future election years without blocking sync before rows are processed", () => {
    expect(
      resolveMichiganCandidateCommittee({
        candidateName: "Gretchen Whitmer",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        contributionRows: [],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });
});
