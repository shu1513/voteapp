import { describe, expect, it } from "vitest";

import {
  normalizeNewHampshireCandidateNameForStorage,
  normalizeNewHampshireCandidateNameKeys,
  resolveNewHampshireCandidateFiler,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFilerResolver.js";
import type { NewHampshireFilingEntityRow } from "../../../src/pipeline/newHampshireFinance/newHampshireCfsClient.js";

// Sanitized from the live 2026 CFS candidate-committee search shape. The
// official person fields identify Cindy Rosenwald; filer/committee text does not.
function filingEntity(
  overrides: Partial<NewHampshireFilingEntityRow> = {}
): NewHampshireFilingEntityRow {
  return {
    registrationGuid: "bc20d4a3-6458-47f9-ad86-075f1231ca36",
    filingEntityId: 207787,
    filerName: "Friends of Cindy Rosenwald",
    candidateName: "Cindy Rosenwald",
    firstName: "Cindy",
    lastName: "Rosenwald",
    committeeName: "Friends of Cindy Rosenwald",
    filerTypeCode: "PAC",
    filerSubTypeCode: "PACCC",
    filerSubTypeName: "Candidate Committee",
    officeName: "State Senate",
    county: null,
    district: "13",
    electionCycleId: 110,
    electionYear: 2026,
    electionCycle: "2026 Election Cycle",
    status: "Active",
    ...overrides,
  };
}

const senateInput = {
  candidateName: "Cindy Rosenwald",
  officeScope: "state_upper",
  officeName: "State Senator",
  district: "District 13",
  electionCycleId: 110,
} as const;

describe("newHampshireCandidateFilerResolver", () => {
  it("normalizes direct and comma-form names without fuzzy expansion", () => {
    expect([...normalizeNewHampshireCandidateNameKeys("ROSENWALD, Cindy Q.")]).toEqual([
      "ROSENWALD CINDY Q",
      "CINDY Q ROSENWALD",
    ]);
    expect(normalizeNewHampshireCandidateNameForStorage("ROSENWALD, Cindy Q.")).toBe(
      "CINDY Q ROSENWALD"
    );
  });

  it("resolves a live-shaped candidate committee from official person, office, and district fields", () => {
    const sourceUrl =
      "https://cfsapi.sos.nh.gov/api/PublicFilerDetails/GetFilingEntityDetails";
    expect(
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        sourceUrl,
        filingEntityRows: [filingEntity()],
      })
    ).toEqual({
      status: "matched",
      filingEntityId: 207787,
      filerName: "Friends of Cindy Rosenwald",
      candidateAliases: ["Cindy Rosenwald"],
      officeName: "State Senate",
      district: "13",
      confidence: "exact",
      source: "cfs_registration",
      sourceUrl,
      matchedRegistrationRowCount: 1,
    });
  });

  it("requires the exact official cycle, office, and district", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        filingEntityRows: [
          filingEntity({ filingEntityId: 1, electionCycleId: 111 }),
          filingEntity({ filingEntityId: 2, officeName: "State Representative" }),
          filingEntity({ filingEntityId: 3, district: "12" }),
          filingEntity({ filingEntityId: 4 }),
        ],
      })
    ).toMatchObject({ status: "matched", filingEntityId: 4 });
  });

  it("rejects candidate committees whose official registration omits the required district", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Daryl Abbas",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "22",
        electionCycleId: 110,
        filingEntityRows: [
          filingEntity({
            filingEntityId: 208786,
            filerName: "Abbas for New Hampshire",
            committeeName: "Abbas for New Hampshire",
            candidateName: "Daryl Abbas",
            firstName: "Daryl",
            lastName: "Abbas",
            district: null,
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
  });

  it("never derives candidate identity from committee or filer display text", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        filingEntityRows: [
          filingEntity({
            filerName: "Friends of Cindy Rosenwald",
            committeeName: "Friends of Cindy Rosenwald",
            candidateName: null,
            firstName: null,
            lastName: null,
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
  });

  it("allows missing middle evidence but rejects conflicting middles and generations", () => {
    const resolve = (candidateName: string, officialName: string) =>
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        candidateName,
        filingEntityRows: [
          filingEntity({
            candidateName: officialName,
          }),
        ],
      });

    expect(resolve("Cindy Rosenwald", "Rosenwald, Cindy Q.")).toMatchObject({ status: "matched" });
    expect(resolve("Cindy Q. Rosenwald", "Rosenwald, Cindy Quinn")).toMatchObject({
      status: "matched",
    });
    expect(resolve("Cindy A. Rosenwald", "Rosenwald, Cindy B.")).toMatchObject({
      status: "unmatched",
    });
    expect(resolve("John Doe Jr.", "Doe, John III")).toMatchObject({ status: "unmatched" });
  });

  it("groups duplicate official rows by filing entity ID and retains trusted person aliases", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        candidateName: "Cindy Q. Rosenwald",
        filingEntityRows: [
          filingEntity({ candidateName: "Cindy Rosenwald" }),
          filingEntity({ candidateName: "Rosenwald, Cindy Q." }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filingEntityId: 207787,
      candidateAliases: ["Cindy Q. Rosenwald", "Cindy Rosenwald"],
      matchedRegistrationRowCount: 2,
    });
  });

  it("returns multiple exact official registrations as ambiguous instead of guessing", () => {
    const result = resolveNewHampshireCandidateFiler({
      ...senateInput,
      filingEntityRows: [
        filingEntity({ filingEntityId: 100, filerName: "Committee One" }),
        filingEntity({ filingEntityId: 200, filerName: "Committee Two" }),
      ],
    });

    expect(result).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized: "CINDY ROSENWALD",
      officeNameNormalized: "State Senate",
    });
    expect(result.status === "ambiguous" ? result.matches.map((match) => match.filingEntityId) : []).toEqual([
      100,
      200,
    ]);
  });

  it("supports direct-candidate registration codes without weakening office evidence", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Patrick Abrami",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "State House District Rockingham 12 (2024); New Hampshire",
        electionCycleId: 110,
        filingEntityRows: [
          filingEntity({
            filingEntityId: 244398,
            filerName: "Abrami, Patrick",
            committeeName: null,
            candidateName: "Patrick Abrami",
            firstName: "Patrick",
            lastName: "Abrami",
            filerTypeCode: "CAN",
            filerSubTypeCode: null,
            filerSubTypeName: null,
            officeName: "State Representative",
            county: "Rockingham",
            district: "12",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", filingEntityId: 244398 });
  });

  it("requires the county-qualified NH House district and rejects the same number in another county", () => {
    const houseInput = {
      candidateName: "Patrick Abrami",
      officeScope: "state_lower",
      officeName: "State Lower Chamber Legislator",
      electionCycleId: 110,
    } as const;
    const houseRegistration = (filingEntityId: number, county: string) =>
      filingEntity({
        filingEntityId,
        candidateName: "Patrick Abrami",
        firstName: "Patrick",
        lastName: "Abrami",
        filerTypeCode: "CAN",
        filerSubTypeCode: null,
        officeName: "State Representative",
        county,
        district: "12",
      });

    expect(
      resolveNewHampshireCandidateFiler({
        ...houseInput,
        district: "State House District Rockingham 12 (2024); New Hampshire",
        filingEntityRows: [
          houseRegistration(1, "Cheshire"),
          houseRegistration(2, "Rockingham"),
        ],
      })
    ).toMatchObject({ status: "matched", filingEntityId: 2, district: "Rockingham 12" });
    expect(
      resolveNewHampshireCandidateFiler({
        ...houseInput,
        district: "12",
        filingEntityRows: [houseRegistration(2, "Rockingham")],
      })
    ).toMatchObject({ status: "unmatched", reason: "missing_required_district" });
  });

  it("requires official county jurisdiction for county offices", () => {
    const sheriff = (filingEntityId: number, county: string) =>
      filingEntity({
        filingEntityId,
        candidateName: "Chris Connelly",
        firstName: "Chris",
        lastName: "Connelly",
        filerTypeCode: "CAN",
        filerSubTypeCode: null,
        officeName: "Sheriff",
        county,
        district: null,
      });

    expect(
      resolveNewHampshireCandidateFiler({
        candidateName: "Chris Connelly",
        officeScope: "county",
        officeName: "Sheriff",
        district: "Hillsborough County, New Hampshire",
        electionCycleId: 110,
        filingEntityRows: [sheriff(1, "Merrimack"), sheriff(2, "Hillsborough")],
      })
    ).toMatchObject({ status: "matched", filingEntityId: 2, district: "Hillsborough" });
  });

  it("fails closed on missing identity context, unsupported offices, and invalid cycle IDs", () => {
    expect(
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        candidateName: " ",
        filingEntityRows: [],
      })
    ).toMatchObject({ status: "unmatched", reason: "missing_candidate_name" });
    expect(
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        officeScope: "statewide",
        officeName: "President",
        filingEntityRows: [],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });
    expect(
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        district: null,
        filingEntityRows: [],
      })
    ).toMatchObject({ status: "unmatched", reason: "missing_required_district" });
    expect(() =>
      resolveNewHampshireCandidateFiler({
        ...senateInput,
        electionCycleId: 0,
        filingEntityRows: [],
      })
    ).toThrow("Invalid New Hampshire candidate filer election-cycle ID");
  });
});
