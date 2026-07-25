import { describe, expect, it } from "vitest";

import {
  normalizeTexasCandidateNameKeys,
  resolveTexasCandidateCommittee,
} from "../../../src/pipeline/texasFinance/texasCandidateCommitteeResolver.js";
import type { TexasTecFilerRow } from "../../../src/pipeline/texasFinance/texasTecCsvDatabaseReader.js";

function filer(overrides: Partial<TexasTecFilerRow> = {}): TexasTecFilerRow {
  return {
    recordType: "FILER",
    filerIdent: "00012345",
    filerTypeCd: "COH",
    filerName: "ABBOTT, GREG",
    committeeStatusCd: "ACTIVE",
    filerFilerpersStatusCd: "CURRENT",
    contestSeekOfficeCd: "GOVERNOR",
    contestSeekOfficeDistrict: "",
    contestSeekOfficePlace: "",
    contestSeekOfficeDescr: "Governor",
    contestSeekOfficeCountyCd: "",
    contestSeekOfficeCountyDescr: "",
    filerPersentTypeCd: "INDIVIDUAL",
    filerNameOrganization: "",
    filerNameLast: "ABBOTT",
    filerNameFirst: "GREG",
    filerNameShort: "",
    ...overrides,
  };
}

describe("texasCandidateCommitteeResolver", () => {
  it("normalizes direct and comma-form candidate names without fuzzy matching", () => {
    expect([...normalizeTexasCandidateNameKeys("ABBOTT, Greg W.")]).toEqual([
      "ABBOTT GREG W",
      "GREG W ABBOTT",
      "GREG ABBOTT",
    ]);
    expect([...normalizeTexasCandidateNameKeys("Greg Abbott")]).toEqual(["GREG ABBOTT"]);
  });

  it("matches exactly one Texas candidate committee by candidate and office", () => {
    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Greg Abbott",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
        filerRows: [
          filer(),
          filer({ filerIdent: "999", filerName: "OTHER, PERSON", filerNameFirst: "OTHER", filerNameLast: "PERSON" }),
          filer({ filerIdent: "888", filerTypeCd: "SPAC" }),
          filer({ filerIdent: "777", contestSeekOfficeCd: "ATTYGEN", contestSeekOfficeDescr: "Attorney General" }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "00012345",
      committeeName: "ABBOTT, GREG",
      receiptCommitteeIds: ["00012345"],
      receiptCommittees: [
        {
          committeeId: "00012345",
          committeeName: "ABBOTT, GREG",
          relationship: "candidate_filer",
        },
      ],
      confidence: "exact",
      source: "tec_bulk",
      sourceUrl: "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip",
      matchedFilerRowCount: 1,
    });
  });

  it("matches a campaign nickname against the formal TEC filer name", () => {
    // "Gene Wu" (VoteApp) must find the TEC filer "WU, EUGENE"; expansion is
    // one-sided, so the filer-side names stay literal.
    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Gene Wu",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        sourceUrl: null,
        filerRows: [
          filer({ filerName: "WU, EUGENE", filerNameFirst: "EUGENE", filerNameLast: "WU" }),
        ],
      })
    ).toMatchObject({ status: "matched", committeeName: "WU, EUGENE" });

    // Two distinct formal names must not meet at a shared nickname key.
    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Patrick Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        sourceUrl: null,
        filerRows: [
          filer({ filerName: "SMITH, PATRICIA", filerNameFirst: "PATRICIA", filerNameLast: "SMITH" }),
        ],
      })
    ).toMatchObject({ status: "unmatched" });
  });

  it("includes safe campaign-named receipt committees without including opposition committees", () => {
    const result = resolveTexasCandidateCommittee({
      candidateName: "Greg Abbott",
      officeScope: "statewide",
      officeName: "Governor",
      electionYear: 2026,
      filerRows: [
        filer(),
        filer({
          filerIdent: "00051153",
          filerTypeCd: "SPAC",
          filerName: "Texans for Greg Abbott",
          filerNameFirst: "",
          filerNameLast: "",
        }),
        filer({
          filerIdent: "00099991",
          filerTypeCd: "GPAC",
          filerName: "Texans Against Greg Abbott",
          filerNameFirst: "",
          filerNameLast: "",
        }),
        filer({
          filerIdent: "00099992",
          filerTypeCd: "SPAC",
          filerName: "Beat Greg Abbott PAC",
          filerNameFirst: "",
          filerNameLast: "",
        }),
      ],
    });

    expect(result).toMatchObject({
      status: "matched",
      committeeId: "00012345",
      committeeName: "ABBOTT, GREG",
      receiptCommitteeIds: ["00012345", "00051153"],
      receiptCommittees: [
        {
          committeeId: "00012345",
          committeeName: "ABBOTT, GREG",
          relationship: "candidate_filer",
        },
        {
          committeeId: "00051153",
          committeeName: "Texans for Greg Abbott",
          relationship: "campaign_named_committee",
        },
      ],
    });
  });

  it("matches safe Texas source-like office labels", () => {
    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Comptroller of Public Accounts",
        electionYear: 2026,
        filerRows: [
          filer({
            filerIdent: "2001",
            filerName: "DOE, JANE",
            filerNameFirst: "JANE",
            filerNameLast: "DOE",
            contestSeekOfficeCd: "COMPTROLLER",
            contestSeekOfficeDescr: "Comptroller",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "2001",
    });

    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Railroad Commissioner",
        electionYear: 2026,
        filerRows: [
          filer({
            filerIdent: "2002",
            filerName: "DOE, JANE",
            filerNameFirst: "JANE",
            filerNameLast: "DOE",
            contestSeekOfficeCd: "RRCOMM_UNEXPIRED",
            contestSeekOfficeDescr: "Railroad Commissioner, Unexpired Term",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "2002",
    });
  });

  it("requires districts for legislative offices and matches exact district when present", () => {
    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Representative",
        electionYear: 2026,
        filerRows: [
          filer({
            filerIdent: "3001",
            filerName: "DOE, JANE",
            filerNameFirst: "JANE",
            filerNameLast: "DOE",
            contestSeekOfficeCd: "STATEREP",
            contestSeekOfficeDistrict: "52",
            contestSeekOfficeDescr: "State Representative",
          }),
        ],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "State Lower Chamber Legislator",
    });

    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Representative",
        district: "052",
        electionYear: 2026,
        filerRows: [
          filer({
            filerIdent: "3001",
            filerName: "DOE, JANE",
            filerNameFirst: "JANE",
            filerNameLast: "DOE",
            contestSeekOfficeCd: "STATEREP",
            contestSeekOfficeDistrict: "52",
            contestSeekOfficeDescr: "State Representative",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "3001",
    });
  });

  it("does not guess when multiple candidate committees match", () => {
    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Greg Abbott",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filer(),
          filer({
            filerIdent: "00054321",
            filerName: "ABBOTT, GREG CAMPAIGN",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "GREG ABBOTT",
      officeNameNormalized: "Governor",
      matches: [
        {
          committeeId: "00012345",
          committeeName: "ABBOTT, GREG",
          receiptCommitteeIds: ["00012345"],
          receiptCommittees: [
            {
              committeeId: "00012345",
              committeeName: "ABBOTT, GREG",
              relationship: "candidate_filer",
            },
          ],
          confidence: "exact",
          source: "tec_bulk",
          sourceUrl: null,
          matchedFilerRowCount: 1,
        },
        {
          committeeId: "00054321",
          committeeName: "ABBOTT, GREG CAMPAIGN",
          receiptCommitteeIds: ["00054321"],
          receiptCommittees: [
            {
              committeeId: "00054321",
              committeeName: "ABBOTT, GREG CAMPAIGN",
              relationship: "candidate_filer",
            },
          ],
          confidence: "exact",
          source: "tec_bulk",
          sourceUrl: null,
          matchedFilerRowCount: 1,
        },
      ],
    });
  });

  it("returns unmatched for unsupported offices, missing names, typos, and non-candidate filers", () => {
    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "State Board of Education",
        electionYear: 2026,
        filerRows: [filer({ filerName: "DOE, JANE", filerNameFirst: "JANE", filerNameLast: "DOE" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveTexasCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [filer()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Governor",
    });

    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Greg Abbottt",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [filer()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: "GREG ABBOTTT",
      officeNameNormalized: "Governor",
    });

    expect(
      resolveTexasCandidateCommittee({
        candidateName: "Greg Abbott",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [filer({ filerTypeCd: "SPAC" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveTexasCandidateCommittee({
        candidateName: "Greg Abbott",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 1999,
        filerRows: [],
      })
    ).toThrow("Invalid Texas candidate committee election year");
  });
});
