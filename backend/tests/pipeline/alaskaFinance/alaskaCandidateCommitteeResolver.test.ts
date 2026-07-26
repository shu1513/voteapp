import { describe, expect, it } from "vitest";

import {
  normalizeAlaskaCandidateNameForStorage,
  normalizeAlaskaCandidateNameKeys,
  resolveAlaskaCandidateCommittee,
} from "../../../src/pipeline/alaskaFinance/alaskaCandidateCommitteeResolver.js";
import type { AlaskaApocCampaignIncomeRow } from "../../../src/pipeline/alaskaFinance/alaskaApocClient.js";

function income(overrides: Partial<AlaskaApocCampaignIncomeRow> = {}): AlaskaApocCampaignIncomeRow {
  return {
    reportYear: 2026,
    filerId: "1001",
    filerName: "Doe, Jane",
    filerType: "Candidate",
    office: "",
    name: "Doe, Jane",
    date: "10/01/2026",
    type: "Income",
    contributor: "Smith, Pat",
    address: "1 Main",
    city: "Juneau",
    state: "AK",
    zip: "99801",
    country: "USA",
    paymentType: "Check",
    paymentDetail: "1001",
    occupation: "Attorney",
    employer: "Law Firm",
    purpose: "Contribution",
    amount: 250,
    submitted: "10/02/2026",
    status: "Complete",
    sourceUrl: null,
    ...overrides,
  };
}

describe("alaskaCandidateCommitteeResolver", () => {
  it("normalizes candidate names for storage and matching", () => {
    expect(normalizeAlaskaCandidateNameForStorage(" Jane   Doe ")).toBe("JANE DOE");
    expect([...normalizeAlaskaCandidateNameKeys("Doe, Jane")]).toEqual(
      expect.arrayContaining(["DOE JANE", "JANE DOE"])
    );
  });

  it("keys first and last names around middles, suffixes, and quoted call names", () => {
    expect(normalizeAlaskaCandidateNameKeys("Louise B. Stutes")).toContain("LOUISE STUTES");
    expect(normalizeAlaskaCandidateNameKeys("Ruffridge, Justin M.")).toContain("JUSTIN RUFFRIDGE");
    expect(normalizeAlaskaCandidateNameKeys("Bauer, Paul A. Jr.")).toContain("PAUL BAUER");
    expect(normalizeAlaskaCandidateNameKeys('Glenn M. “Mike” Prax')).toContain("MIKE PRAX");
    expect(normalizeAlaskaCandidateNameKeys('Kennedy, Kathleen M. "Kit"')).toContain("KIT KENNEDY");
    // No lone-surname key and no nickname keys without opting in.
    expect(normalizeAlaskaCandidateNameKeys("Louise B. Stutes")).not.toContain("STUTES");
    expect(normalizeAlaskaCandidateNameKeys("Simpler, Kathy C.")).not.toContain("KATHERINE SIMPLER");
    expect(normalizeAlaskaCandidateNameKeys("Simpler, Kathy C.", { expandNicknames: true })).toContain(
      "KATHERINE SIMPLER"
    );
  });

  it("resolves one clear APOC candidate filer from income rows", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
        incomeRows: [
          income({ amount: 100 }),
          income({ amount: 200, contributor: "Roe, Alex" }),
          income({ filerId: "9999", filerName: "Other Candidate", name: "Other Candidate" }),
        ],
      })
    ).toEqual({
      status: "matched",
      candidateFilerId: "1001",
      candidateFilerName: "Doe, Jane",
      confidence: "exact",
      source: "apoc_csv",
      sourceUrl: "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
      matchedRowCount: 2,
    });
  });

  it("returns ambiguous when multiple candidate filers match", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        incomeRows: [
          income({ filerId: "1001", filerName: "Doe, Jane" }),
          income({ filerId: "1002", filerName: "Jane Doe for Alaska" }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_filers",
      candidateNameNormalized: "JANE DOE",
      candidateFilerIds: ["1001", "1002"],
    });
  });

  it("matches a filer whose name carries middle tokens the VoteApp side lacks", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        incomeRows: [income({ filerName: "Jane Marie Doe", name: "Jane Marie Doe" })],
      })
    ).toMatchObject({ status: "matched", candidateFilerName: "Jane Marie Doe" });
  });

  it("matches a campaign nickname against the formal APOC filing name one-sidedly", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Becky Schwanke",
        electionYear: 2026,
        incomeRows: [income({ filerName: "Rebecca A Schwanke", name: "Rebecca A Schwanke" })],
      })
    ).toMatchObject({ status: "matched", candidateFilerName: "Rebecca A Schwanke" });

    // Two distinct formal names must not meet at a shared nickname key.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Patrick Smith",
        electionYear: 2026,
        incomeRows: [income({ filerName: "Patricia Smith", name: "Patricia Smith" })],
      })
    ).toMatchObject({ status: "unmatched" });

    // A shared nickname with both formal families filed stays ambiguous.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Pat Smith",
        electionYear: 2026,
        incomeRows: [
          income({ filerId: "3001", filerName: "Patricia Smith", name: "Patricia Smith" }),
          income({ filerId: "3002", filerName: "Patrick Smith", name: "Patrick Smith" }),
        ],
      })
    ).toMatchObject({ status: "ambiguous", reason: "multiple_matching_filers" });
  });

  it("does not match a key across the filerName/name field seam", () => {
    // filerName ends with the surname and name starts with the first name;
    // joining the fields would fabricate "DOE JANE" across the seam.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Doe, Jane",
        electionYear: 2026,
        incomeRows: [income({ filerName: "Friends of Doe", name: "Jane Smith" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
  });

  it("requires key tokens in order within one field", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        incomeRows: [income({ filerName: "Doerr, Janet", name: "Doerr, Janet" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });
  });

  it("collapses a standalone filer plus its governor-ticket filer to the standalone", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Begich, Tom",
        electionYear: 2026,
        officeName: "Governor",
        incomeRows: [
          income({ filerId: "2001", filerName: "Tom Begich", name: "Tom Begich" }),
          income({ filerId: "2002", filerName: "Tom Begich/Julia Hnilicka", name: "Tom Begich/Julia Hnilicka" }),
        ],
      })
    ).toMatchObject({ status: "matched", candidateFilerId: "2001", candidateFilerName: "Tom Begich" });
  });

  it("only collapses tickets for the governor race and only with the joint delimiter", () => {
    // Committee-style extension in a House race: two filers stay ambiguous.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "State Lower Chamber Legislator",
        incomeRows: [
          income({ filerId: "2001", filerName: "Jane Doe", name: "Jane Doe" }),
          income({ filerId: "2002", filerName: "Jane Doe for State House", name: "Jane Doe for State House" }),
        ],
      })
    ).toMatchObject({ status: "ambiguous", reason: "multiple_matching_filers" });

    // Even in a governor race, an extension without APOC's "/" or "\" joint
    // delimiter is not a ticket filer.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        incomeRows: [
          income({ filerId: "2001", filerName: "Jane Doe", name: "Jane Doe" }),
          income({ filerId: "2002", filerName: "Jane Doe for Alaska", name: "Jane Doe for Alaska" }),
        ],
      })
    ).toMatchObject({ status: "ambiguous", reason: "multiple_matching_filers" });

    // No office context means no collapse.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Begich, Tom",
        electionYear: 2026,
        incomeRows: [
          income({ filerId: "2001", filerName: "Tom Begich", name: "Tom Begich" }),
          income({ filerId: "2002", filerName: "Tom Begich/Julia Hnilicka", name: "Tom Begich/Julia Hnilicka" }),
        ],
      })
    ).toMatchObject({ status: "ambiguous", reason: "multiple_matching_filers" });
  });

  it("does not collapse a one-token extension that could be another person", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Mark Smith",
        electionYear: 2026,
        officeName: "Governor",
        incomeRows: [
          income({ filerId: "2001", filerName: "Mark Smith", name: "Mark Smith" }),
          income({ filerId: "2002", filerName: "Mark Smith / Jr", name: "Mark Smith / Jr" }),
        ],
      })
    ).toMatchObject({ status: "ambiguous", reason: "multiple_matching_filers" });
  });

  it("filters rows by office class when both sides carry one", () => {
    // A same-name filer in a different race must not link.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "State Lower Chamber Legislator",
        incomeRows: [income({ office: "Senate" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });

    // Municipal filings never satisfy a state-office candidate.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "State Lower Chamber Legislator",
        incomeRows: [income({ office: "Assembly" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_filer_match" });

    // Blank or unrecognized row office text never blocks a match.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "State Lower Chamber Legislator",
        incomeRows: [income({ office: "" }), income({ office: "Statewide Ballot" })],
      })
    ).toMatchObject({ status: "matched", candidateFilerId: "1001" });

    // Governor and Lieutenant Governor rows are one ticket class.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
        incomeRows: [income({ office: "Lt. Governor" })],
      })
    ).toMatchObject({ status: "matched", candidateFilerId: "1001" });

    // The right-office filer wins when a wrong-office namesake also filed.
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "State Senator",
        incomeRows: [
          income({ filerId: "4001", office: "Senate" }),
          income({ filerId: "4002", filerName: "Jane Doe for Anchorage", name: "Jane Doe for Anchorage", office: "Assembly" }),
        ],
      })
    ).toMatchObject({ status: "matched", candidateFilerId: "4001" });
  });

  it("ignores out-of-cycle rows", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        incomeRows: [income({ reportYear: 2024, date: "10/01/2024" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized: "JANE DOE",
    });
  });

  it("ignores matching non-candidate filer rows", () => {
    expect(
      resolveAlaskaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        incomeRows: [
          income({
            filerId: "8001",
            filerName: "Jane Doe Support PAC",
            filerType: "Group",
            office: "",
            name: "Jane Doe",
          }),
        ],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_candidate_filer_match",
      candidateNameNormalized: "JANE DOE",
    });
  });
});
