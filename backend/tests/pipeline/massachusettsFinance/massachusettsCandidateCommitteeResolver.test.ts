import { describe, expect, it, vi } from "vitest";

import {
  normalizeMassachusettsCandidateNameKeys,
  resolveMassachusettsCandidateCommittee,
  searchAndResolveMassachusettsCandidateCommittee,
} from "../../../src/pipeline/massachusettsFinance/massachusettsCandidateCommitteeResolver.js";
import type { MassachusettsOcpfCandidateFiler } from "../../../src/pipeline/massachusettsFinance/massachusettsOcpfClient.js";

function filer(overrides: Partial<MassachusettsOcpfCandidateFiler> = {}): MassachusettsOcpfCandidateFiler {
  return {
    cpfId: "15710",
    filerName: "Maura T. Healey",
    filerNameReverse: "Healey, Maura T.",
    committeeName: "Healey Committee",
    officeSought: "Statewide, Governor",
    accountTypeCode: "D",
    accountTypeDescription: "Depository Candidate",
    isCandidate: true,
    isActive: true,
    ...overrides,
  };
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200, statusText: "OK" });
}

describe("massachusettsCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and parenthetical candidate names", () => {
    expect([...normalizeMassachusettsCandidateNameKeys("HEALEY, Maura T. (Maura Healey)")]).toEqual([
      "HEALEY MAURA T",
      "MAURA T HEALEY",
      "MAURA HEALEY",
    ]);
  });

  it("matches exactly one active Massachusetts candidate CPF by candidate name and office", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Maura Healey",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        filers: [
          filer({ cpfId: "99999", filerName: "Other Person", filerNameReverse: "Person, Other" }),
          filer(),
          filer({ cpfId: "12345", accountTypeCode: "PAC", accountTypeDescription: "Political Committee" }),
        ],
      })
    ).toEqual({
      status: "matched",
      candidateCpfId: "15710",
      filerName: "Maura T. Healey",
      committeeName: "Healey Committee",
      officeSought: "Statewide, Governor",
      confidence: "exact",
      source: "ocpf_api",
      sourceUrl: null,
      matchedFilerRowCount: 1,
    });
  });

  it("matches using OCPF comma-form filer names", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Maura Healey",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        filers: [filer({ filerName: "Healey, Maura T.", filerNameReverse: undefined })],
      })
    ).toMatchObject({ status: "matched", candidateCpfId: "15710" });
  });

  it("rejects a same-race filer whose middle name contradicts the candidate", () => {
    // Same office and account type — only the middle evidence differs. Without
    // the middle gate this filer linked as an "exact" match and attached the
    // other John Smith's OCPF reports.
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filers: [
          filer({
            cpfId: "30001",
            filerName: "John B. Smith",
            filerNameReverse: "Smith, John B.",
            committeeName: "Smith Committee",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("accepts an initial that corroborates the full middle name", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filers: [
          filer({
            cpfId: "30001",
            filerName: "John Andrew Smith",
            filerNameReverse: "Smith, John Andrew",
            committeeName: "Smith Committee",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", candidateCpfId: "30001" });
  });

  it("still falls back to first+last when a side lacks middle info", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "John Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filers: [
          filer({
            cpfId: "30001",
            filerName: "John B. Smith",
            filerNameReverse: "Smith, John B.",
            committeeName: "Smith Committee",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", candidateCpfId: "30001" });
  });

  it("lets a middle conflict veto a middle-less sibling name on the same filer", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        filers: [
          filer({
            cpfId: "30001",
            filerName: "John Smith",
            filerNameReverse: "Smith, John B.",
            committeeName: "Smith Committee",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("matches legislative filers only when expected district matches", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        electionYear: 2024,
        district: "3rd Suffolk",
        filers: [
          filer({
            cpfId: "20001",
            filerName: "Doe, Jane",
            filerNameReverse: "Doe, Jane",
            committeeName: "Jane Doe Committee",
            officeSought: "House, 3rd Suffolk",
          }),
          filer({
            cpfId: "20002",
            filerName: "Doe, Jane",
            filerNameReverse: "Doe, Jane",
            committeeName: "Jane Doe Committee",
            officeSought: "House, 4th Suffolk",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      candidateCpfId: "20001",
      officeSought: "House, 3rd Suffolk",
    });
  });

  it("requires districts for legislative offices", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2024,
        filers: [
          filer({
            cpfId: "20001",
            filerName: "Doe, Jane",
            officeSought: "Senate, 2nd Bristol & Plymouth",
          }),
        ],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "STATE SENATOR",
    });
  });

  it("does not guess when multiple candidate CPFs match", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Attorney General",
        electionYear: 2026,
        filers: [
          filer({
            cpfId: "20001",
            filerName: "Jane Doe",
            committeeName: "Jane Doe Committee",
            officeSought: "Statewide, Attorney General",
          }),
          filer({
            cpfId: "20002",
            filerName: "Jane Doe",
            committeeName: "Friends of Jane Doe",
            officeSought: "Statewide, Attorney General",
          }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Statewide, Attorney General",
      matches: [{ candidateCpfId: "20001" }, { candidateCpfId: "20002" }],
    });
  });

  it("groups duplicate filer rows with the same CPF ID", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "State Treasurer",
        electionYear: 2026,
        filers: [
          filer({ cpfId: "20001", filerName: "Jane Doe", officeSought: "Statewide, Treasurer" }),
          filer({ cpfId: "20001", filerName: "Doe, Jane", officeSought: "Statewide, Treasurer" }),
        ],
      })
    ).toMatchObject({ status: "matched", candidateCpfId: "20001", matchedFilerRowCount: 2 });
  });

  it("returns unmatched for unsupported offices, missing names, inactive rows, and typos", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "county",
        officeName: "Sheriff",
        electionYear: 2026,
        filers: [filer({ filerName: "Jane Doe" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        filers: [filer()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Statewide, Governor",
    });

    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Maura Healy",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        filers: [filer()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Maura Healey",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        filers: [filer({ isActive: false })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveMassachusettsCandidateCommittee({
        candidateName: "Maura Healey",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 1999,
        filers: [],
      })
    ).toThrow("Invalid Massachusetts candidate committee election year");
  });

  it("can search OCPF filers and resolve through the async wrapper", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          cpfId: 15710,
          filerName: "Maura T. Healey",
          filerNameReverse: "Healey, Maura T.",
          committeeName: "Healey Committee",
          officeSought: "Statewide, Governor",
          accountTypeCode: "D",
          accountTypeDescription: "Depository Candidate",
          isCandidate: true,
          isActive: true,
        },
      ])
    ) as unknown as typeof fetch;

    await expect(
      searchAndResolveMassachusettsCandidateCommittee(
        {
          candidateName: "Maura Healey",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2022,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      candidateCpfId: "15710",
    });

    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.origin + requestUrl.pathname).toBe("https://api.ocpf.us/filers/listings/A");
    expect(requestUrl.searchParams.get("searchPhrase")).toBe("Maura Healey");
  });
});
