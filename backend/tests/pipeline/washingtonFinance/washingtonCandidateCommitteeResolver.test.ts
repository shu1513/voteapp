import { describe, expect, it, vi } from "vitest";

import {
  normalizeWashingtonCandidateNameKeys,
  resolveWashingtonCandidateCommittee,
  searchAndResolveWashingtonCandidateCommittee,
} from "../../../src/pipeline/washingtonFinance/washingtonCandidateCommitteeResolver.js";
import type { WashingtonPdcCandidateSummary } from "../../../src/pipeline/washingtonFinance/washingtonPdcClient.js";

function summary(overrides: Partial<WashingtonPdcCandidateSummary> = {}): WashingtonPdcCandidateSummary {
  return {
    filerId: "FERGR *115",
    committeeId: "32311",
    candidacyId: "689556",
    filerName: "Robert W. Ferguson (Bob Ferguson)",
    committeeCategory: "Candidate",
    politicalCommitteeType: "Candidate",
    candidateCommitteeStatus: "Candidate declared",
    activeCandidate: true,
    hasReports: true,
    office: "GOVERNOR",
    jurisdiction: "State of Washington",
    jurisdictionType: "Statewide",
    electionYear: 2024,
    contributionsAmount: 14668288.73,
    sourceUrl: "https://my.pdc.wa.gov/registration/public/-/#/public/registration/59793",
    ...overrides,
  };
}

describe("washingtonCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and parenthetical candidate names", () => {
    expect([...normalizeWashingtonCandidateNameKeys("FERGUSON, Robert W. (Bob Ferguson)")]).toEqual([
      "FERGUSON ROBERT W",
      "ROBERT W FERGUSON",
      "ROBERT FERGUSON",
      "BOB FERGUSON",
    ]);
  });

  it("matches exactly one active Washington candidate committee by public parenthetical name", () => {
    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "Bob Ferguson",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2024,
        summaries: [
          summary({
            filerId: "FERGB--024",
            committeeId: "36704",
            filerName: "Bob Ferguson",
            candidateCommitteeStatus: "Candidate withdrew",
            activeCandidate: false,
            hasReports: false,
          }),
          summary({ filerId: "FERGR *115" }),
          summary({ filerId: "OTHER", committeeId: "999", filerName: "Other Person" }),
        ],
      })
    ).toEqual({
      status: "matched",
      filerId: "FERGR *115",
      committeeId: "32311",
      committeeName: "Robert W. Ferguson (Bob Ferguson)",
      candidacyId: "689556",
      contributionsAmount: 14668288.73,
      confidence: "exact",
      source: "pdc_api",
      sourceUrl: "https://my.pdc.wa.gov/registration/public/-/#/public/registration/59793",
      matchedSummaryRowCount: 1,
    });
  });

  it("matches legislative committees only when the expected district matches", () => {
    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        legislativeDistrict: "9",
        electionYear: 2024,
        summaries: [
          summary({
            filerId: "DOEJ--101",
            committeeId: "4001",
            filerName: "Jane Doe",
            office: "STATE REPRESENTATIVE",
            legislativeDistrict: "09",
          }),
          summary({
            filerId: "DOEJ--102",
            committeeId: "4002",
            filerName: "Jane Doe",
            office: "STATE REPRESENTATIVE",
            legislativeDistrict: "10",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      filerId: "DOEJ--101",
      committeeId: "4001",
    });
  });

  it("requires districts for legislative offices", () => {
    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2024,
        summaries: [summary({ filerName: "Jane Doe", office: "STATE SENATOR", legislativeDistrict: "01" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "STATE SENATOR",
    });
  });

  it("does not guess when multiple active committees match", () => {
    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Secretary of State",
        electionYear: 2024,
        summaries: [
          summary({ filerId: "DOEJ--101", committeeId: "4001", filerName: "Jane Doe", office: "SECRETARY OF STATE" }),
          summary({ filerId: "DOEJ--102", committeeId: "4002", filerName: "Jane Doe", office: "SECRETARY OF STATE" }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "SECRETARY OF STATE",
      matches: [
        { filerId: "DOEJ--101", committeeId: "4001" },
        { filerId: "DOEJ--102", committeeId: "4002" },
      ],
    });
  });

  it("returns unmatched for unsupported offices, missing names, inactive rows, and typos", () => {
    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "county",
        officeName: "County Sheriff",
        electionYear: 2024,
        summaries: [summary({ filerName: "Jane Doe", office: "COUNTY SHERIFF" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2024,
        summaries: [summary()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "GOVERNOR",
    });

    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "Bob Fergusson",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2024,
        summaries: [summary()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveWashingtonCandidateCommittee({
        candidateName: "Bob Ferguson",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2024,
        summaries: [summary({ candidateCommitteeStatus: "Candidate withdrew", activeCandidate: false })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveWashingtonCandidateCommittee({
        candidateName: "Bob Ferguson",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 1999,
        summaries: [],
      })
    ).toThrow("Invalid Washington candidate committee election year");
  });

  it("can search PDC summaries and resolve them through the async wrapper", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(JSON.stringify([
        {
          filer_id: "FERGR *115",
          committee_id: "32311",
          candidacy_id: "689556",
          filer_name: "Robert W. Ferguson (Bob Ferguson)",
          committee_category: "Candidate",
          political_committee_type: "Candidate",
          candidate_committee_status: "Candidate declared",
          active_candidate: "true",
          has_reports: "true",
          office: "GOVERNOR",
          jurisdiction: "State of Washington",
          jurisdiction_type: "Statewide",
          election_year: "2024",
          contributions_amount: "14668288.73",
          url: { url: "https://my.pdc.wa.gov/registration/public/-/#/public/registration/59793" },
        },
      ]), {
        status: 200,
        statusText: "OK",
      })
    ) as unknown as typeof fetch;

    await expect(
      searchAndResolveWashingtonCandidateCommittee(
        {
          candidateName: "Bob Ferguson",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2024,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      filerId: "FERGR *115",
      committeeId: "32311",
    });

    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.origin + requestUrl.pathname).toBe("https://data.wa.gov/resource/3h9x-7bvm.json");
    expect(requestUrl.searchParams.get("$where")).toContain("upper(office) = upper('GOVERNOR')");
  });
});
