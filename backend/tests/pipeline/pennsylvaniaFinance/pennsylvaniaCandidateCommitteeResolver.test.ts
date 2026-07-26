import { describe, expect, it } from "vitest";

import {
  normalizePennsylvaniaCandidateNameKeys,
  resolvePennsylvaniaCandidateCommittee,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCandidateCommitteeResolver.js";
import type { PennsylvaniaCampaignFinanceFilerRow } from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceReader.js";

function filerRow(overrides: Partial<PennsylvaniaCampaignFinanceFilerRow> = {}): PennsylvaniaCampaignFinanceFilerRow {
  return {
    CampaignfinanceID: "100",
    FILERID: "12345",
    EYEAR: "2026",
    SubmittedDate: "20260501",
    CYCLE: "2",
    AMMEND: "",
    TERMINATE: "",
    FILERTYPE: "1",
    FILERNAME: "JANE DOE FOR GOVERNOR",
    OFFICE: "GOV",
    DISTRICT: "",
    PARTY: "DEM",
    ADDRESS1: "",
    ADDRESS2: "",
    CITY: "",
    STATE: "PA",
    ZIPCODE: "",
    COUNTY: "",
    PHONE: "",
    BEGINNING: "",
    MONETARY: "",
    INKIND: "",
    ...overrides,
  };
}

describe("pennsylvaniaCandidateCommitteeResolver", () => {
  it("normalizes direct and comma-form candidate names without broad fuzzy matching", () => {
    expect([...normalizePennsylvaniaCandidateNameKeys("DOE, Jane E.")]).toEqual([
      "DOE JANE E",
      "JANE E DOE",
      "JANE DOE",
    ]);
    expect([...normalizePennsylvaniaCandidateNameKeys("Jane E. Doe")]).toEqual(["JANE E DOE", "JANE DOE"]);
  });

  it("matches exactly one Pennsylvania candidate filer by office and filer name", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        sourceUrl: "https://www.pa.gov/example/2026.zip",
        filerRows: [
          filerRow(),
          filerRow({
            FILERID: "99999",
            FILERNAME: "OTHER PERSON FOR GOVERNOR",
          }),
          filerRow({
            FILERID: "77777",
            FILERNAME: "PENNSYLVANIA ACTION PAC",
          }),
        ],
      })
    ).toEqual({
      status: "matched",
      filerId: "12345",
      filerName: "JANE DOE FOR GOVERNOR",
      filerType: "1",
      confidence: "exact",
      source: "pa_bulk",
      sourceUrl: "https://www.pa.gov/example/2026.zip",
      matchedFilerRowCount: 1,
    });
  });

  it("matches common PA committee wrappers conservatively", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Pat Harkins",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "1",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "22222",
            FILERNAME: "FRIENDS OF PAT HARKINS C/O SUSAN M KOWALSKI TREASURER",
            OFFICE: "STH",
            DISTRICT: "1",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "22222",
      filerName: "FRIENDS OF PAT HARKINS C/O SUSAN M KOWALSKI TREASURER",
    });
  });

  it("requires valid districts for legislative offices before matching", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Pat Harkins",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "22222",
            FILERNAME: "FRIENDS OF PAT HARKINS",
            OFFICE: "STH",
            DISTRICT: "1",
          }),
        ],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "PAT HARKINS",
      officeNameNormalized: "STATE LOWER CHAMBER LEGISLATOR",
    });
  });

  it("skips same-name legislative filers from other districts", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Pat Harkins",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "1",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "22222",
            FILERNAME: "FRIENDS OF PAT HARKINS",
            OFFICE: "STH",
            DISTRICT: "2",
          }),
        ],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_filer_match",
    });
  });

  it("rejects unsupported offices without trying to infer from filer names", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "local",
        officeName: "Mayor",
        electionYear: 2026,
        filerRows: [filerRow({ FILERNAME: "JANE DOE FOR MAYOR" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "unsupported_office",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "MAYOR",
    });
  });

  it("resolves the sole committee filer over the candidate's own registration filer", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Abigail Salisbury",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "34",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0465",
            FILERNAME: "SALISBURY, ABIGAIL MARIE",
            FILERTYPE: "1",
            OFFICE: "STH",
            DISTRICT: "34",
          }),
          filerRow({
            FILERID: "20220025",
            FILERNAME: "PEOPLE FOR ABIGAIL SALISBURY",
            FILERTYPE: "2",
            OFFICE: "STH",
            DISTRICT: "34",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "20220025",
      filerName: "PEOPLE FOR ABIGAIL SALISBURY",
      filerType: "2",
    });
  });

  it("resolves the sole committee filer even against two candidate registration filers", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filerRow({ FILERID: "2026C0001", FILERNAME: "DOE, JANE", FILERTYPE: "1" }),
          filerRow({ FILERID: "2026C0900", FILERNAME: "DOE, JANE E", FILERTYPE: "1" }),
          filerRow({ FILERID: "20240100", FILERNAME: "FRIENDS OF JANE DOE", FILERTYPE: "2" }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "20240100",
      filerType: "2",
    });
  });

  it("recalls a blank-OFFICE committee corroborated by the registration row's ZIP", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Aaron Bernstine",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0279",
            FILERNAME: "AARON BERNSTINE",
            FILERTYPE: "1",
            OFFICE: "STH",
            DISTRICT: "8",
            ZIPCODE: "16141",
            PHONE: "4129773127",
          }),
          filerRow({
            FILERID: "20150221",
            FILERNAME: "FRIENDS OF AARON BERNSTINE",
            FILERTYPE: "2",
            OFFICE: "",
            DISTRICT: "",
            ZIPCODE: "16141",
            PHONE: "",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "20150221",
      filerType: "2",
    });
  });

  it("recalls a blank-OFFICE committee corroborated by the registration row's phone", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Marla Brown",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "9",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0832",
            FILERNAME: "MARLA BROWN",
            FILERTYPE: "1",
            OFFICE: "STH",
            DISTRICT: "9",
            ZIPCODE: "16102",
            PHONE: "7247300256",
          }),
          filerRow({
            FILERID: "20220071",
            FILERNAME: "MARLA BROWN FOR PA",
            FILERTYPE: "2",
            OFFICE: "",
            DISTRICT: "",
            ZIPCODE: "99999",
            PHONE: "724-730-0256",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "20220071",
      filerType: "2",
    });
  });

  it("recalls a same-office committee with a blank DISTRICT when the ZIP corroborates", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Rosemary Brown",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "40",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0183",
            FILERNAME: "ROSEMARY BROWN",
            FILERTYPE: "1",
            OFFICE: "STS",
            DISTRICT: "40",
            ZIPCODE: "18372",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            OFFICE: "STS",
            DISTRICT: "",
            ZIPCODE: "18372-0000",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2010237",
      filerType: "2",
    });
  });

  it("never recalls a committee naming a different office, even with a shared ZIP", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Rosemary Brown",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "40",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0183",
            FILERNAME: "ROSEMARY BROWN",
            FILERTYPE: "1",
            OFFICE: "STS",
            DISTRICT: "40",
            ZIPCODE: "18372",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            OFFICE: "STH",
            DISTRICT: "",
            ZIPCODE: "18372",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0183",
      filerType: "1",
    });
  });

  it("never recalls a same-office committee that carries its own district", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Rosemary Brown",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "40",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0183",
            FILERNAME: "ROSEMARY BROWN",
            FILERTYPE: "1",
            OFFICE: "STS",
            DISTRICT: "40",
            ZIPCODE: "18372",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            OFFICE: "STS",
            DISTRICT: "12",
            ZIPCODE: "18372",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0183",
      filerType: "1",
    });
  });

  it("vetoes recall when a sibling row of the same filer carries a conflicting district", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Rosemary Brown",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "40",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0183",
            FILERNAME: "ROSEMARY BROWN",
            FILERTYPE: "1",
            OFFICE: "STS",
            DISTRICT: "40",
            ZIPCODE: "18372",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            OFFICE: "STS",
            DISTRICT: "",
            ZIPCODE: "18372",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            OFFICE: "STS",
            DISTRICT: "12",
            ZIPCODE: "18372",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0183",
      filerType: "1",
    });
  });

  it("does not let a corroborating sibling district veto the recall", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Aaron Bernstine",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0279",
            FILERNAME: "AARON BERNSTINE",
            FILERTYPE: "1",
            OFFICE: "STH",
            DISTRICT: "8",
            ZIPCODE: "16141",
          }),
          filerRow({
            FILERID: "20150221",
            FILERNAME: "FRIENDS OF AARON BERNSTINE",
            FILERTYPE: "2",
            OFFICE: "",
            DISTRICT: "",
            ZIPCODE: "16141",
          }),
          filerRow({
            FILERID: "20150221",
            FILERNAME: "FRIENDS OF AARON BERNSTINE",
            FILERTYPE: "2",
            OFFICE: "STH",
            DISTRICT: "08",
            ZIPCODE: "16141",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "20150221",
      filerType: "2",
    });
  });

  it("ignores a conflicting sibling district from another election year", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Rosemary Brown",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "40",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0183",
            FILERNAME: "ROSEMARY BROWN",
            FILERTYPE: "1",
            OFFICE: "STS",
            DISTRICT: "40",
            ZIPCODE: "18372",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            OFFICE: "STS",
            DISTRICT: "",
            ZIPCODE: "18372",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            EYEAR: "2022",
            OFFICE: "STH",
            DISTRICT: "189",
            ZIPCODE: "18372",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2010237",
      filerType: "2",
    });
  });

  it("keeps the registration when a same-office blank-district committee lacks corroboration", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Rosemary Brown",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "40",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0183",
            FILERNAME: "ROSEMARY BROWN",
            FILERTYPE: "1",
            OFFICE: "STS",
            DISTRICT: "40",
            ZIPCODE: "18372",
            PHONE: "5705551234",
          }),
          filerRow({
            FILERID: "2010237",
            FILERNAME: "FRIENDS OF ROSEMARY BROWN",
            FILERTYPE: "2",
            OFFICE: "STS",
            DISTRICT: "",
            ZIPCODE: "17108",
            PHONE: "7175559999",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0183",
      filerType: "1",
    });
  });

  it("rejects a recalled committee whose DISTRICT conflicts with the registration", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Aaron Bernstine",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0279",
            FILERNAME: "AARON BERNSTINE",
            FILERTYPE: "1",
            OFFICE: "STH",
            DISTRICT: "8",
            ZIPCODE: "16141",
          }),
          filerRow({
            FILERID: "20150221",
            FILERNAME: "FRIENDS OF AARON BERNSTINE",
            FILERTYPE: "2",
            OFFICE: "",
            DISTRICT: "9",
            ZIPCODE: "16141",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0279",
      filerType: "1",
    });
  });

  it("admits a recalled committee whose padded DISTRICT normalizes to the registration's", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Aaron Bernstine",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "8",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0279",
            FILERNAME: "AARON BERNSTINE",
            FILERTYPE: "1",
            OFFICE: "STH",
            DISTRICT: "8",
            ZIPCODE: "16141",
          }),
          filerRow({
            FILERID: "20150221",
            FILERNAME: "FRIENDS OF AARON BERNSTINE",
            FILERTYPE: "2",
            OFFICE: "",
            DISTRICT: "08",
            ZIPCODE: "16141",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "20150221",
      filerType: "2",
    });
  });

  it("rejects a district-bearing recalled committee for a statewide race", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0001",
            FILERNAME: "DOE, JANE",
            FILERTYPE: "1",
            OFFICE: "GOV",
            ZIPCODE: "15001",
          }),
          filerRow({
            FILERID: "20240500",
            FILERNAME: "FRIENDS OF JANE DOE",
            FILERTYPE: "2",
            OFFICE: "",
            DISTRICT: "12",
            ZIPCODE: "15001",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0001",
      filerType: "1",
    });
  });

  it("keeps the registration filer when a blank-OFFICE committee matches by name only", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0001",
            FILERNAME: "DOE, JANE",
            FILERTYPE: "1",
            OFFICE: "GOV",
            ZIPCODE: "15001",
            PHONE: "4125550001",
          }),
          filerRow({
            FILERID: "20240500",
            FILERNAME: "FRIENDS OF JANE DOE",
            FILERTYPE: "2",
            OFFICE: "",
            ZIPCODE: "19999",
            PHONE: "2155559999",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0001",
      filerType: "1",
    });
  });

  it("never admits a committee whose OFFICE names a different race, even with a shared ZIP", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "2026C0001",
            FILERNAME: "DOE, JANE",
            FILERTYPE: "1",
            OFFICE: "GOV",
            ZIPCODE: "15001",
          }),
          filerRow({
            FILERID: "20240500",
            FILERNAME: "FRIENDS OF JANE DOE",
            FILERTYPE: "2",
            OFFICE: "STH",
            DISTRICT: "5",
            ZIPCODE: "15001",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "2026C0001",
      filerType: "1",
    });
  });

  it("does not recall committees without an office-matched registration row", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filerRow({
            FILERID: "20240500",
            FILERNAME: "FRIENDS OF JANE DOE",
            FILERTYPE: "2",
            OFFICE: "",
            ZIPCODE: "15001",
            PHONE: "4125550001",
          }),
        ],
      })
    ).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_filer_match",
    });
  });

  it("stays ambiguous when two committee filers match", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filerRow({ FILERID: "20240100", FILERNAME: "FRIENDS OF JANE DOE", FILERTYPE: "2" }),
          filerRow({ FILERID: "20260200", FILERNAME: "JANE DOE FOR PA", FILERTYPE: "2" }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_filers",
    });
  });

  it("does not guess when multiple candidate filers match", () => {
    expect(
      resolvePennsylvaniaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filerRows: [
          filerRow(),
          filerRow({
            FILERID: "12346",
            FILERNAME: "FRIENDS OF JANE DOE",
          }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "GOV",
      matches: [
        {
          filerId: "12345",
          filerName: "JANE DOE FOR GOVERNOR",
          filerType: "1",
          confidence: "exact",
          source: "pa_bulk",
          sourceUrl: null,
          matchedFilerRowCount: 1,
        },
        {
          filerId: "12346",
          filerName: "FRIENDS OF JANE DOE",
          filerType: "1",
          confidence: "exact",
          source: "pa_bulk",
          sourceUrl: null,
          matchedFilerRowCount: 1,
        },
      ],
    });
  });
});
