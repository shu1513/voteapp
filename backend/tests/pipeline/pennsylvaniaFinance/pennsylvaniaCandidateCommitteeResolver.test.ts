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
