import { describe, expect, it } from "vitest";

import {
  isLouisianaFinanceEligibleOffice,
  mapLouisianaFinanceOffice,
  normalizeLouisianaFinanceDistrict,
  normalizeLouisianaFinanceOfficeName,
} from "../../../src/pipeline/louisianaFinance/louisianaFinanceEligibleOffices.js";
import {
  normalizeLouisianaCandidateNameForStorage,
  normalizeLouisianaCandidateNameKeys,
  resolveLouisianaCandidateCommittee,
  type LouisianaCandidateCommitteeRow,
} from "../../../src/pipeline/louisianaFinance/louisianaCandidateCommitteeResolver.js";

function candidateRow(overrides: Partial<LouisianaCandidateCommitteeRow> = {}): LouisianaCandidateCommitteeRow {
  return {
    FilerNumber: "12345",
    FilerLastName: "Edwards",
    FilerFirstName: "John Bel",
    CandidateName: "John Bel Edwards",
    FilerName: "Edwards, John Bel",
    OfficeSought: "Governor",
    District: "",
    ElectionYear: "2027",
    ContributionDate: "10/15/2027",
    FilerType: "Candidate",
    ...overrides,
  };
}

describe("Louisiana finance eligible offices", () => {
  it("normalizes supported state offices and legislative districts conservatively", () => {
    expect(normalizeLouisianaFinanceOfficeName("Lt. Governor")).toBe("Lieutenant Governor");
    expect(normalizeLouisianaFinanceOfficeName("State Representative")).toBe("State Lower Chamber Legislator");
    expect(normalizeLouisianaFinanceDistrict(" 007 ")).toBe("7");
    expect(mapLouisianaFinanceOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toEqual({
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
      requiresDistrict: false,
    });
    expect(
      mapLouisianaFinanceOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Representative",
        district: "007",
      })
    ).toEqual({
      officeScope: "state_lower",
      officeName: "State Lower Chamber Legislator",
      district: "7",
      requiresDistrict: true,
    });
    expect(isLouisianaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(false);
  });
});

describe("Louisiana candidate committee resolver", () => {
  it("normalizes direct and comma-form candidate names without broad fuzzy matching", () => {
    expect([...normalizeLouisianaCandidateNameKeys("Edwards, John Bel")]).toEqual([
      "EDWARDS JOHN BEL",
      "JOHN BEL EDWARDS",
    ]);
    expect([...normalizeLouisianaCandidateNameKeys("John Bel Edwards")]).toEqual(["JOHN BEL EDWARDS"]);
    expect(normalizeLouisianaCandidateNameForStorage("Edwards, John Bel")).toBe("EDWARDS JOHN BEL");
  });

  it("matches exactly one Louisiana candidate filer by name, election cycle, and unique filer number", () => {
    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "John Bel Edwards",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2027,
        sourceUrl: "https://www.ethics.la.gov/campaignfinancesearch/SearchByName.aspx",
        candidateRows: [
          candidateRow(),
          candidateRow({
            FilerNumber: "99999",
            FilerLastName: "Other",
            FilerFirstName: "Candidate",
            CandidateName: "Other Candidate",
            FilerName: "Candidate, Other",
          }),
          candidateRow({
            FilerNumber: "88888",
            FilerType: "Political Action Committee",
          }),
          candidateRow({
            FilerNumber: "77777",
            ElectionYear: "2023",
            ContributionDate: "9/1/2023",
          }),
        ],
      })
    ).toEqual({
      status: "matched",
      filerNumber: "12345",
      filerName: "Edwards, John Bel",
      candidateName: "John Bel Edwards",
      officeName: "Governor",
      district: null,
      confidence: "exact",
      source: "la_ethics_search",
      sourceUrl: "https://www.ethics.la.gov/campaignfinancesearch/SearchByName.aspx",
      matchedCandidateRowCount: 1,
    });
  });

  it("uses legislative district only as an app-side eligibility gate", () => {
    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Representative",
        district: "7",
        electionYear: 2027,
        candidateRows: [
          candidateRow({
            FilerNumber: "2001",
            FilerLastName: "Doe",
            FilerFirstName: "Jane",
            CandidateName: "Jane Doe",
            FilerName: "Doe, Jane",
            OfficeSought: "Representative",
            District: "07",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerNumber: "2001",
      officeName: "State Lower Chamber Legislator",
      district: "7",
    });
  });

  it("links rows that only have phase-one bulk CSV filer fields when cycle evidence is present", () => {
    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "John Bel Edwards",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2027,
        candidateRows: [
          {
            FilerNumber: "12345",
            FilerLastName: "Edwards",
            FilerFirstName: "John Bel",
            ReportCode: "10-G",
            ReportType: "10-G",
            ReportNumber: "1",
            ContributionDate: "9/1/2027",
          },
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerNumber: "12345",
      filerName: "Edwards, John Bel",
      candidateName: "John Bel Edwards",
      officeName: "Governor",
      district: null,
    });
  });

  it("does not treat first-and-last-only collisions as exact matches", () => {
    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "John Edwards",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2027,
        candidateRows: [candidateRow()],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });

  it("does not guess when multiple Louisiana filer numbers match", () => {
    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Attorney General",
        electionYear: 2027,
        candidateRows: [
          candidateRow({
            FilerNumber: "2001",
            FilerLastName: "Doe",
            FilerFirstName: "Jane",
            CandidateName: "Jane Doe",
            FilerName: "Doe, Jane",
            OfficeSought: "Attorney General",
          }),
          candidateRow({
            FilerNumber: "2002",
            FilerLastName: "Doe",
            FilerFirstName: "Jane",
            CandidateName: "Jane Doe",
            FilerName: "Jane Doe",
            OfficeSought: "Attorney General",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Attorney General",
      matches: [
        {
          filerNumber: "2001",
          filerName: "Doe, Jane",
          candidateName: "Jane Doe",
          officeName: "Attorney General",
          district: null,
          confidence: "exact",
          source: "la_ethics_search",
          sourceUrl: null,
          matchedCandidateRowCount: 1,
        },
        {
          filerNumber: "2002",
          filerName: "Jane Doe",
          candidateName: "Jane Doe",
          officeName: "Attorney General",
          district: null,
          confidence: "exact",
          source: "la_ethics_search",
          sourceUrl: null,
          matchedCandidateRowCount: 1,
        },
      ],
    });
  });

  it("returns unmatched for unsupported offices, missing names, missing districts, and wrong years", () => {
    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "John Bel Edwards",
        officeScope: "county",
        officeName: "Sheriff",
        electionYear: 2027,
        candidateRows: [candidateRow()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: "JOHN BEL EDWARDS",
      officeNameNormalized: "SHERIFF",
    });

    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: " ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2027,
        candidateRows: [candidateRow()],
      })
    ).toMatchObject({ status: "unmatched", reason: "missing_candidate_name" });

    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2027,
        candidateRows: [candidateRow({ CandidateName: "Jane Doe", FilerFirstName: "Jane", FilerLastName: "Doe" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "missing_legislative_district" });

    expect(
      resolveLouisianaCandidateCommittee({
        candidateName: "John Bel Edwards",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2027,
        candidateRows: [candidateRow({ ElectionYear: "2023", ContributionDate: "9/1/2023" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveLouisianaCandidateCommittee({
        candidateName: "John Bel Edwards",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 1999,
        candidateRows: [],
      })
    ).toThrow("Invalid Louisiana candidate committee election year");
  });
});
