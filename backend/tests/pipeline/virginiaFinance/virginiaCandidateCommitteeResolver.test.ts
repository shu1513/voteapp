import { describe, expect, it, vi } from "vitest";

import {
  normalizeVirginiaCandidateNameKeys,
  resolveVirginiaCandidateCommittee,
  searchAndResolveVirginiaCandidateCommittee,
} from "../../../src/pipeline/virginiaFinance/virginiaCandidateCommitteeResolver.js";
import type {
  VirginiaCommitteeSearchResult,
  VirginiaReportHeader,
} from "../../../src/pipeline/virginiaFinance/virginiaCampaignFinanceClient.js";

function committee(overrides: Partial<VirginiaCommitteeSearchResult> = {}): VirginiaCommitteeSearchResult {
  return {
    committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
    committeeName: "Spanberger for Governor",
    candidateName: "Abigail Spanberger",
    committeeType: "Candidate Campaign Committee",
    reportsUrl: "https://cfreports.elections.virginia.gov/Committee/Index/60e10dc7-c59e-4a79-afca-e688c1efed65",
    sourceUrl: "https://cfreports.elections.virginia.gov/?CommitteeName=Spanberger",
    ...overrides,
  };
}

function header(overrides: Partial<VirginiaReportHeader> = {}): VirginiaReportHeader {
  return {
    committeeCode: "CC-23-02436",
    committeeName: "Spanberger for Governor",
    reportYear: 2025,
    reportType: "Scheduled",
    filingDate: "2026-01-15",
    startDate: "2025-10-24",
    endDate: "2025-11-27",
    electionCycle: "11/2025",
    officeSought: "Governor",
    ...overrides,
  };
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200, statusText: "OK" });
}

describe("virginiaCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and parenthetical candidate names", () => {
    expect([...normalizeVirginiaCandidateNameKeys("SPANBERGER, Abigail (Abby Spanberger)")]).toEqual([
      "SPANBERGER ABIGAIL",
      "ABIGAIL SPANBERGER",
      "ABBY SPANBERGER",
    ]);
  });

  it("matches exactly one Virginia candidate committee by name and eligible office", () => {
    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Abigail Spanberger",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        committeeResults: [committee(), committee({ committeeId: "other", candidateName: "Other Person" })],
      })
    ).toEqual({
      status: "matched",
      committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
      committeeName: "Spanberger for Governor",
      committeeCode: null,
      candidateName: "Abigail Spanberger",
      confidence: "exact",
      source: "cfreports_search",
      sourceUrl: "https://cfreports.elections.virginia.gov/?CommitteeName=Spanberger",
      matchedReportHeaderCount: 0,
    });
  });

  it("uses report headers to verify office, election cycle, and committee code when provided", () => {
    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Abigail Spanberger",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        committeeResults: [committee()],
        reportHeaders: [
          header({ electionCycle: "11/2021" }),
          header({ officeSought: "Attorney General" }),
          header(),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
      committeeName: "Spanberger for Governor",
      committeeCode: "CC-23-02436",
      candidateName: "Abigail Spanberger",
      confidence: "exact",
      source: "cfreports_search",
      sourceUrl: "https://cfreports.elections.virginia.gov/?CommitteeName=Spanberger",
      matchedReportHeaderCount: 1,
    });
  });

  it("maps Virginia legislative report office labels to canonical app offices", () => {
    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        electionYear: 2025,
        committeeResults: [committee({ committeeName: "Jane Doe for Delegate", candidateName: "Jane Doe" })],
        reportHeaders: [
          header({
            committeeName: "Jane Doe for Delegate",
            officeSought: "House of Delegates",
            electionCycle: "11/2025",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeName: "Jane Doe for Delegate",
      matchedReportHeaderCount: 1,
    });

    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2025,
        committeeResults: [committee({ committeeName: "Jane Doe for Senate", candidateName: "Jane Doe" })],
        reportHeaders: [
          header({
            committeeName: "Jane Doe for Senate",
            officeSought: "Senate",
            electionCycle: "11/2025",
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeName: "Jane Doe for Senate",
      matchedReportHeaderCount: 1,
    });
  });

  it("does not guess when multiple candidate committees match", () => {
    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Attorney General",
        electionYear: 2025,
        committeeResults: [
          committee({
            committeeId: "a",
            committeeName: "Jane Doe for Attorney General",
            candidateName: "Jane Doe",
          }),
          committee({
            committeeId: "b",
            committeeName: "Friends of Jane Doe",
            candidateName: "Jane Doe",
          }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Attorney General",
      matches: [{ committeeId: "a" }, { committeeId: "b" }],
    });
  });

  it("returns unmatched for unsupported offices, missing names, typos, and report metadata mismatches", () => {
    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "county",
        officeName: "Sheriff",
        electionYear: 2025,
        committeeResults: [committee({ candidateName: "Jane Doe" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        committeeResults: [committee()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Governor",
    });

    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Abigail Spamburger",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        committeeResults: [committee()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveVirginiaCandidateCommittee({
        candidateName: "Abigail Spanberger",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2025,
        committeeResults: [committee()],
        reportHeaders: [header({ officeSought: "Attorney General" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveVirginiaCandidateCommittee({
        candidateName: "Abigail Spanberger",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 1999,
        committeeResults: [],
      })
    ).toThrow("Invalid Virginia candidate committee election year");
  });

  it("can search Virginia committees and resolve through the async wrapper", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(
        `
          <table>
            <tr>
              <td class="committeeName"><div> Spanberger for Governor </div></td>
              <td class="candidateName"><div> Abigail Spanberger </div></td>
              <td class="committeeType"><div> Candidate Campaign Committee </div></td>
              <td><a href="/Committee/Index/60e10dc7-c59e-4a79-afca-e688c1efed65">View Reports</a></td>
            </tr>
          </table>
        `,
        { status: 200, statusText: "OK" }
      )
    ) as unknown as typeof fetch;

    await expect(
      searchAndResolveVirginiaCandidateCommittee(
        {
          candidateName: "Abigail Spanberger",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2025,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      committeeId: "60e10dc7-c59e-4a79-afca-e688c1efed65",
    });

    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.origin + requestUrl.pathname).toBe("https://cfreports.elections.virginia.gov/");
    expect(requestUrl.searchParams.get("CommitteeName")).toBe("Abigail Spanberger");
  });

  it("does not search the network for unsupported offices", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse([])) as unknown as typeof fetch;

    await expect(
      searchAndResolveVirginiaCandidateCommittee(
        {
          candidateName: "Jane Doe",
          officeScope: "county",
          officeName: "Sheriff",
          electionYear: 2025,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({ status: "unmatched", reason: "unsupported_office" });
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});
