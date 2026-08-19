import { describe, expect, it, vi } from "vitest";

import {
  normalizeHawaiiCandidateNameKeys,
  resolveHawaiiCandidateCommittee,
  searchAndResolveHawaiiCandidateCommittee,
} from "../../../src/pipeline/hawaiiFinance/hawaiiCandidateCommitteeResolver.js";
import type { HawaiiCscCandidateCommitteeSummary } from "../../../src/pipeline/hawaiiFinance/hawaiiCscClient.js";

function summary(overrides: Partial<HawaiiCscCandidateCommitteeSummary> = {}): HawaiiCscCandidateCommitteeSummary {
  return {
    candidateName: "Green, Josh",
    committeeId: "CC10174",
    electionPeriod: "2018-2022",
    office: "Governor",
    district: undefined,
    county: "Statewide",
    party: "Democrat",
    totalAmount: 4_070_153.38,
    contributionCount: 1432,
    ...overrides,
  };
}

function jsonResponse(payload: unknown): Response {
  return new Response(JSON.stringify(payload), { status: 200, statusText: "OK" });
}

describe("hawaiiCandidateCommitteeResolver", () => {
  it("normalizes direct and comma-form candidate names", () => {
    expect([...normalizeHawaiiCandidateNameKeys("Green, Josh*")]).toEqual(["GREEN JOSH", "JOSH GREEN"]);
    expect(normalizeHawaiiCandidateNameKeys("Josh Green").has("JOSH GREEN")).toBe(true);
  });

  it("derives the first+last key from a comma-form name with a middle token", () => {
    // Real CSC summaries read "Brown, Robert J." / "Chock, Sr., Mason". Splitting
    // the raw form on spaces made the first+last key "BROWN J", so a "Robert
    // Brown" candidate never key-matched and the middle-name gate never ran.
    expect([...normalizeHawaiiCandidateNameKeys("Smith, John B.")]).toEqual([
      "SMITH JOHN B",
      "JOHN B SMITH",
      "JOHN SMITH",
    ]);
    expect(normalizeHawaiiCandidateNameKeys("Chock, Sr., Mason").has("MASON CHOCK")).toBe(true);
    expect(normalizeHawaiiCandidateNameKeys("Smith, John B.").has("SMITH B")).toBe(false);

    const resolve = (candidateName: string, summaryName: string) =>
      resolveHawaiiCandidateCommittee({
        candidateName,
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        summaries: [summary({ candidateName: summaryName, committeeId: "CC30001" })],
      });
    // Missing middle on the candidate side falls back to first+last.
    expect(resolve("John Smith", "Smith, John B.")).toMatchObject({ status: "matched", committeeId: "CC30001" });
    expect(resolve("John B. Smith", "Smith, John B.")).toMatchObject({ status: "matched", committeeId: "CC30001" });
    // The middle gate still rejects a contradicting middle initial.
    expect(resolve("John A. Smith", "Smith, John B.")).toMatchObject({
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
  });

  it("matches exactly one Hawaii candidate committee by candidate name, office, and election year", () => {
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "Josh Green",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        summaries: [
          summary(),
          summary({ candidateName: "Greene, Madeline", committeeId: "CC99999", totalAmount: 50_000 }),
          summary({ electionPeriod: "2022-2026", totalAmount: 2_917_141.36 }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "CC10174",
      committeeName: "Green, Josh",
      electionPeriod: "2018-2022",
      totalAmount: 4070153.38,
      confidence: "exact",
      source: "csc_api",
      sourceUrl: "https://hicscdata.hawaii.gov/dataset/Campaign-Contributions-Received-By-Hawaii-State-an/jexd-xbcg",
      matchedSummaryRowCount: 1,
    });
  });

  it("matches legislative committees only when the expected district matches", () => {
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        electionYear: 2024,
        district: "9",
        summaries: [
          summary({ candidateName: "Doe, Jane", committeeId: "CC20001", electionPeriod: "2022-2024", office: "House", district: "9" }),
          summary({ candidateName: "Doe, Jane", committeeId: "CC20002", electionPeriod: "2022-2024", office: "House", district: "10" }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "CC20001",
      electionPeriod: "2022-2024",
    });
  });

  it("rejects a same-race summary whose middle name contradicts the candidate", () => {
    // Same office and election period — only the middle evidence differs.
    // Without the middle gate this summary linked as an "exact" match and
    // attached the other John Smith's contributions.
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        summaries: [summary({ candidateName: "John B. Smith", committeeId: "CC30001" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("accepts an initial that corroborates the full middle name", () => {
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "John A. Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        summaries: [summary({ candidateName: "John Andrew Smith", committeeId: "CC30001" })],
      })
    ).toMatchObject({ status: "matched", committeeId: "CC30001" });
  });

  it("still falls back to first+last when a side lacks middle info", () => {
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "John Smith",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        summaries: [summary({ candidateName: "John B. Smith", committeeId: "CC30001" })],
      })
    ).toMatchObject({ status: "matched", committeeId: "CC30001" });
  });

  it("requires districts for legislative offices", () => {
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2024,
        summaries: [summary({ candidateName: "Doe, Jane", office: "Senate", district: "1", electionPeriod: "2022-2024" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "STATE SENATOR",
    });
  });

  it("does not guess when multiple committees or election periods match", () => {
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Lieutenant Governor",
        electionYear: 2022,
        summaries: [
          summary({ candidateName: "Doe, Jane", committeeId: "CC20001", electionPeriod: "2018-2022", office: "Lt. Governor" }),
          summary({ candidateName: "Doe, Jane", committeeId: "CC20002", electionPeriod: "2018-2022", office: "Lt. Governor" }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Lt. Governor",
      matches: [
        { committeeId: "CC20001", electionPeriod: "2018-2022" },
        { committeeId: "CC20002", electionPeriod: "2018-2022" },
      ],
    });
  });

  it("returns unmatched for unsupported offices, missing names, and typos", () => {
    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "city",
        officeName: "Mayor",
        electionYear: 2024,
        summaries: [summary({ candidateName: "Doe, Jane", office: "Mayor", electionPeriod: "2022-2024" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        summaries: [summary()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Governor",
    });

    expect(
      resolveHawaiiCandidateCommittee({
        candidateName: "Josh Greene",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        summaries: [summary()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveHawaiiCandidateCommittee({
        candidateName: "Josh Green",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 1999,
        summaries: [],
      })
    ).toThrow("Invalid Hawaii candidate committee election year");
  });

  it("can search CSC summaries and resolve them through the async wrapper", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse([
        {
          candidate_name: "Green, Josh",
          office: "Governor",
          county: "Statewide",
          party: "Democrat",
          reg_no: "CC10174",
          election_period: "2018-2022",
          total_amount: "4070153.38",
          total_count: "1432",
        },
      ])
    ) as unknown as typeof fetch;

    await expect(
      searchAndResolveHawaiiCandidateCommittee(
        {
          candidateName: "Josh Green",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2022,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      committeeId: "CC10174",
      electionPeriod: "2018-2022",
    });

    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.origin + requestUrl.pathname).toBe("https://hicscdata.hawaii.gov/resource/jexd-xbcg.json");
    expect(requestUrl.searchParams.get("$where")).toContain("upper(office) = upper('Governor')");
    expect(requestUrl.searchParams.get("$where")).toContain("lower(candidate_name) like '%green%'");
  });
});
