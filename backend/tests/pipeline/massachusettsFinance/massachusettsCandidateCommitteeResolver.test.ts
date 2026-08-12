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

  it("matches legislative filers from catalog district names (auto-link shape)", () => {
    // Auto-link passes the catalog district NAME ("Middlesex and Norfolk
    // District (2024); Massachusetts"); OCPF labels the same district
    // "Senate, Middlesex and Norfolk". Word-ordinal catalog names must also
    // meet numeric OCPF labels ("Third Suffolk" vs "3rd Suffolk").
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Karen Spilka",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        district: "Middlesex and Norfolk District (2024); Massachusetts",
        filers: [
          filer({
            cpfId: "13758",
            filerName: "Karen Spilka",
            filerNameReverse: "Spilka, Karen",
            committeeName: "Spilka Committee",
            officeSought: "Senate, Middlesex and Norfolk",
            // Live legislative account type (plural — regression guard for
            // the \bCANDIDATE\b filter that rejected every legislative filer).
            accountTypeCode: undefined,
            accountTypeDescription: "Legislative Candidates",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", candidateCpfId: "13758" });
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Aaron Michlewitz",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        electionYear: 2026,
        district: "Third Suffolk District (2024); Massachusetts",
        filers: [
          filer({
            cpfId: "14100",
            filerName: "Aaron Michlewitz",
            filerNameReverse: "Michlewitz, Aaron",
            committeeName: "Michlewitz Committee",
            officeSought: "House, 3rd Suffolk",
            accountTypeCode: undefined,
            accountTypeDescription: "Legislative Candidates",
          }),
        ],
      })
    ).toMatchObject({ status: "matched", candidateCpfId: "14100" });
    // A filer still carrying a pre-redistricting district label stays
    // unmatched rather than guessing.
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        district: "Norfolk-Plymouth-Bristol District (2024); Massachusetts",
        filers: [
          filer({
            cpfId: "20003",
            filerName: "Doe, Jane",
            officeSought: "Senate, Norfolk, Bristol & Middlesex",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
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

  it("falls back to a surname search when the full-name phrase returns nothing", async () => {
    // OCPF requires every phrase token to match: "Karen E. Spilka" (roster
    // middle initial) returns zero rows, "Spilka" finds the filer. The strict
    // name-key/office/district gates still decide after the broad recall.
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(
        jsonResponse([
          {
            cpfId: 13758,
            filerName: "Karen Spilka",
            filerNameReverse: "Spilka, Karen",
            committeeName: "Spilka Committee",
            officeSought: "Senate, Middlesex and Norfolk",
            accountTypeDescription: "Legislative Candidates",
            isCandidate: true,
            isActive: true,
          },
        ])
      ) as unknown as typeof fetch;

    await expect(
      searchAndResolveMassachusettsCandidateCommittee(
        {
          candidateName: "Karen E. Spilka",
          officeScope: "state_upper",
          officeName: "State Senator",
          electionYear: 2026,
          district: "Middlesex and Norfolk District (2024); Massachusetts",
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      candidateCpfId: "13758",
    });

    const calls = vi.mocked(fetchImpl).mock.calls;
    expect(calls).toHaveLength(2);
    expect(new URL(String(calls[0]?.[0])).searchParams.get("searchPhrase")).toBe("Karen E. Spilka");
    expect(new URL(String(calls[1]?.[0])).searchParams.get("searchPhrase")).toBe("SPILKA");
  });

  it("falls back to a surname search when the full-name rows all fail the strict gates", async () => {
    // The full-name search can return rows (an inactive re-registered
    // committee) that the usability filters reject; the surname retry must
    // still fire on that no_candidate_committee_match, not only on zero rows.
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse([
          {
            cpfId: 20009,
            filerName: "Karen E. Spilka",
            filerNameReverse: "Spilka, Karen E.",
            committeeName: "Old Spilka Committee",
            officeSought: "Senate, Middlesex and Norfolk",
            accountTypeDescription: "Legislative Candidates",
            isCandidate: true,
            isActive: false,
          },
        ])
      )
      .mockResolvedValueOnce(
        jsonResponse([
          {
            cpfId: 13758,
            filerName: "Karen Spilka",
            filerNameReverse: "Spilka, Karen",
            committeeName: "Spilka Committee",
            officeSought: "Senate, Middlesex and Norfolk",
            accountTypeDescription: "Legislative Candidates",
            isCandidate: true,
            isActive: true,
          },
        ])
      ) as unknown as typeof fetch;

    await expect(
      searchAndResolveMassachusettsCandidateCommittee(
        {
          candidateName: "Karen E. Spilka",
          officeScope: "state_upper",
          officeName: "State Senator",
          electionYear: 2026,
          district: "Middlesex and Norfolk District (2024); Massachusetts",
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      candidateCpfId: "13758",
    });
    expect(vi.mocked(fetchImpl).mock.calls).toHaveLength(2);
  });

  it("does not widen the search after an ambiguous full-name pass", async () => {
    // Two usable same-name filers for the same seat = real ambiguity; the
    // surname retry must not run, and the result stays fail-closed.
    const ambiguousRows = [
      {
        cpfId: 20010,
        filerName: "Jane Doe",
        filerNameReverse: "Doe, Jane",
        committeeName: "Jane Doe Committee",
        officeSought: "Senate, Middlesex and Norfolk",
        accountTypeDescription: "Legislative Candidates",
        isCandidate: true,
        isActive: true,
      },
      {
        cpfId: 20011,
        filerName: "Jane Doe",
        filerNameReverse: "Doe, Jane",
        committeeName: "Committee to Elect Jane Doe",
        officeSought: "Senate, Middlesex and Norfolk",
        accountTypeDescription: "Legislative Candidates",
        isCandidate: true,
        isActive: true,
      },
    ];
    const fetchImpl = vi.fn().mockResolvedValueOnce(jsonResponse(ambiguousRows)) as unknown as typeof fetch;

    await expect(
      searchAndResolveMassachusettsCandidateCommittee(
        {
          candidateName: "Jane Doe",
          officeScope: "state_upper",
          officeName: "State Senator",
          electionYear: 2026,
          district: "Middlesex and Norfolk District (2024); Massachusetts",
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({ status: "ambiguous" });
    expect(vi.mocked(fetchImpl).mock.calls).toHaveLength(1);
  });

  // Fixtures mirror the live OCPF filer-search rows for "wu" (2026-08-10):
  // municipal committees carry "Mayoral, {City}" / "City Councilor, {City}"
  // labels and the "Depository Candidate" account type; inaugural funds are
  // inactive "Segregated Accounts" rows with "N/A, N/A" offices.
  it("resolves a Boston mayoral depository committee by name + city", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Michelle Wu",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2025,
        district: "BOSTON",
        filers: [
          filer({
            cpfId: "15563",
            filerName: "Wu, Michelle",
            filerNameReverse: "Wu, Michelle",
            committeeName: "Wu Committee",
            officeSought: "Mayoral, Boston",
          }),
          filer({
            cpfId: "50089",
            filerName: "Boston Inaugural Fund 2026 Michelle Wu",
            filerNameReverse: undefined,
            committeeName: "Boston Inaugural Fund 2026 Michelle Wu",
            officeSought: "N/A, N/A",
            accountTypeCode: undefined,
            accountTypeDescription: "Segregated Accounts",
            isCandidate: false,
            isActive: false,
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      candidateCpfId: "15563",
      committeeName: "Wu Committee",
      officeSought: "Mayoral, Boston",
    });
  });

  it("rejects a same-class mayoral filer from a different city", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "William Sarkodieh",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2025,
        district: "BOSTON",
        filers: [
          filer({
            cpfId: "17125",
            filerName: "Sarkodieh, William",
            filerNameReverse: undefined,
            committeeName: "Sarkodieh Committee",
            officeSought: "Mayoral, Worcester",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("refuses municipal cities outside the allowlist as unsupported offices", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "William Sarkodieh",
        officeScope: "place",
        officeName: "Mayor",
        electionYear: 2025,
        district: "WORCESTER",
        filers: [
          filer({
            cpfId: "17125",
            filerName: "Sarkodieh, William",
            filerNameReverse: undefined,
            committeeName: "Sarkodieh Committee",
            officeSought: "Mayoral, Worcester",
          }),
        ],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });
  });

  it("resolves a Boston city councilor and ignores mayoral filers for council races", () => {
    expect(
      resolveMassachusettsCandidateCommittee({
        candidateName: "Ruthzee Louijeune",
        officeScope: "place",
        officeName: "City Council Member",
        electionYear: 2025,
        district: "BOSTON",
        filers: [
          filer({
            cpfId: "17669",
            filerName: "Louijeune, Ruthzee",
            filerNameReverse: undefined,
            committeeName: "Louijeune Committee",
            officeSought: "City Councilor, Boston",
          }),
          filer({
            cpfId: "15563",
            filerName: "Wu, Michelle",
            filerNameReverse: undefined,
            committeeName: "Wu Committee",
            officeSought: "Mayoral, Boston",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      candidateCpfId: "17669",
      committeeName: "Louijeune Committee",
    });
  });
});
