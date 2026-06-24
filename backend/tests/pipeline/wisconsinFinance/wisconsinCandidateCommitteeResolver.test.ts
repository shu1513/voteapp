import { describe, expect, it, vi } from "vitest";

import {
  normalizeWisconsinCandidateNameKeys,
  resolveWisconsinCandidateCommittee,
  searchAndResolveWisconsinCandidateCommittee,
} from "../../../src/pipeline/wisconsinFinance/wisconsinCandidateCommitteeResolver.js";
import type { WisconsinSunshineCommittee } from "../../../src/pipeline/wisconsinFinance/wisconsinSunshineClient.js";

function committee(overrides: Partial<WisconsinSunshineCommittee> = {}): WisconsinSunshineCommittee {
  return {
    entityId: "16621",
    committeeId: "407",
    assignedCommitteeId: "0104212",
    committeeName: "Tiffany for Wisconsin",
    committeeType: "State Candidate",
    committeeStatus: "Approved",
    committeeStatusSlug: "ACTIVE",
    candidateNames: ["Tom Tiffany", "James Koth", "Incredible Bank"],
    sourceUrl: "https://campaignfinance.wi.gov/browse-data/registrants/16621",
    ...overrides,
  };
}

function trpcResponse(payload: unknown): Response {
  return new Response(JSON.stringify([{ result: { data: { json: payload } } }]), {
    status: 200,
    statusText: "OK",
  });
}

describe("wisconsinCandidateCommitteeResolver", () => {
  it("normalizes direct, comma-form, and parenthetical candidate names", () => {
    expect([...normalizeWisconsinCandidateNameKeys("TIFFANY, Thomas P. (Tom Tiffany)")]).toEqual([
      "TIFFANY THOMAS P",
      "THOMAS P TIFFANY",
      "THOMAS TIFFANY",
      "TOM TIFFANY",
    ]);
  });

  it("matches exactly one active Wisconsin state candidate committee by candidate connection name", () => {
    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Tom Tiffany",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committees: [
          committee({
            entityId: "953328",
            committeeId: "13337",
            assignedCommitteeId: "0300132",
            committeeName: "Republican Party of Oneida County",
            committeeType: "Political Party",
            candidateNames: ["Tom (Chairman) Tiffany"],
          }),
          committee(),
          committee({ entityId: "999", committeeId: "999", committeeName: "Other Person for Wisconsin", candidateNames: ["Other Person"] }),
        ],
      })
    ).toEqual({
      status: "matched",
      entityId: "16621",
      committeeId: "407",
      assignedCommitteeId: "0104212",
      committeeName: "Tiffany for Wisconsin",
      confidence: "exact",
      source: "sunshine_api",
      sourceUrl: "https://campaignfinance.wi.gov/browse-data/registrants/16621",
      matchedCommitteeRowCount: 1,
    });
  });

  it("matches candidate committees by committee name only when no candidate connections are present", () => {
    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Attorney General",
        electionYear: 2026,
        committees: [
          committee({
            entityId: "200",
            committeeId: "300",
            assignedCommitteeId: "0100300",
            committeeName: "Jane Doe",
            candidateNames: [],
          }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      entityId: "200",
      committeeId: "300",
      committeeName: "Jane Doe",
    });
  });

  it("requires districts for legislative offices", () => {
    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_upper",
        officeName: "State Senator",
        electionYear: 2026,
        committees: [committee({ committeeName: "Jane Doe", candidateNames: ["Jane Doe"] })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_legislative_district",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "STATE SENATOR",
    });

    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "AD 4",
        electionYear: 2026,
        committees: [committee({ committeeName: "Jane Doe", candidateNames: ["Jane Doe"] })],
      })
    ).toMatchObject({ status: "matched" });
  });

  it("does not guess when multiple active state candidate committees match", () => {
    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Secretary of State",
        electionYear: 2026,
        committees: [
          committee({ entityId: "200", committeeId: "300", committeeName: "Jane Doe", candidateNames: ["Jane Doe"] }),
          committee({ entityId: "201", committeeId: "301", committeeName: "Friends of Jane Doe", candidateNames: ["Jane Doe"] }),
        ],
      })
    ).toMatchObject({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Secretary of State",
      matches: [
        { entityId: "200", committeeId: "300" },
        { entityId: "201", committeeId: "301" },
      ],
    });
  });

  it("returns unmatched for unsupported offices, missing names, typos, inactive rows, and non-candidate committees", () => {
    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Jane Doe",
        officeScope: "county",
        officeName: "District Attorney",
        electionYear: 2026,
        committees: [committee({ candidateNames: ["Jane Doe"] })],
      })
    ).toMatchObject({ status: "unmatched", reason: "unsupported_office" });

    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "   ",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committees: [committee()],
      })
    ).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
      candidateNameNormalized: "",
      officeNameNormalized: "Governor",
    });

    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Tom Tiffani",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committees: [committee()],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Tom Tiffany",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committees: [committee({ committeeStatusSlug: "TERMINATED", committeeStatus: "Terminated" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });

    expect(
      resolveWisconsinCandidateCommittee({
        candidateName: "Tom Tiffany",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        committees: [committee({ committeeType: "Political Party" })],
      })
    ).toMatchObject({ status: "unmatched", reason: "no_candidate_committee_match" });
  });

  it("rejects invalid election years", () => {
    expect(() =>
      resolveWisconsinCandidateCommittee({
        candidateName: "Tom Tiffany",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 1999,
        committees: [],
      })
    ).toThrow("Invalid Wisconsin candidate committee election year");
  });

  it("can search Wisconsin Sunshine committees and resolve through the async wrapper", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      trpcResponse({
        results: [
          {
            id: 407,
            assignedCommitteeId: "0104212",
            entity: { id: 16621, name: "Tiffany for Wisconsin" },
            committeeType: { name: "State Candidate" },
            committeeStatus: { name: "Approved", statusSlug: "ACTIVE" },
            entityConnections: [{ entity: { name: "Tom Tiffany" } }],
          },
          {
            id: 13337,
            assignedCommitteeId: "0300132",
            entity: { id: 953328, name: "Republican Party of Oneida County" },
            committeeType: { name: "Political Party" },
            committeeStatus: { name: "Approved", statusSlug: "ACTIVE" },
            entityConnections: [{ entity: { name: "Tom (Chairman) Tiffany" } }],
          },
        ],
      })
    ) as unknown as typeof fetch;

    await expect(
      searchAndResolveWisconsinCandidateCommittee(
        {
          candidateName: "Tom Tiffany",
          officeScope: "statewide",
          officeName: "Governor",
          electionYear: 2026,
        },
        { fetchImpl, timeoutMs: 1000 }
      )
    ).resolves.toMatchObject({
      status: "matched",
      entityId: "16621",
      committeeId: "407",
    });

    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.origin + requestUrl.pathname).toBe(
      "https://campaignfinance.wi.gov/api/trpc/publicFrontendApi.getCommittees"
    );
    expect(JSON.parse(requestUrl.searchParams.get("input") ?? "{}")).toMatchObject({
      "0": { json: { searchTerm: "Tom Tiffany", take: 50 } },
    });
  });
});
