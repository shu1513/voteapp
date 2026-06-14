import { describe, expect, it, vi } from "vitest";

import {
  loadCandidateElectionOfficeContext,
  loadCandidatePresidentialCycleOfficeContext,
} from "../../src/pipeline/candidates/candidateRecordOfficeContext.js";

describe("loadCandidateElectionOfficeContext", () => {
  it("returns context for linked candidate-election pair with office_id", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          candidateId: "cand-1",
          candidateDisplayName: "Jane Doe",
          electionId: "e-1",
          districtName: "California",
          districtType: "statewide",
          state: "CA",
          electionDate: "2026-11-03",
          officialBallotTitle: "Governor",
          electionStage: "general",
          senateClass: null,
          termEndYear: null,
          officeId: "office-1",
          discoveryContestFamily: "non_judicial_office",
          electionSources: ["https://example.org"],
        },
      ],
    });

    const result = await loadCandidateElectionOfficeContext({ query }, "cand-1", "e-1");

    expect(result?.candidateId).toBe("cand-1");
    expect(result?.officeId).toBe("office-1");
    expect(result?.discoveryContestFamily).toBe("non_judicial_office");
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("FROM public.candidate_elections ce");
    expect(query.mock.calls[0]?.[0]).not.toContain("e.office_id IS NOT NULL");
    expect(query.mock.calls[0]?.[1]).toEqual(["cand-1", "e-1"]);
  });

  it("returns context for linked candidate-election pair when office_id is null", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          candidateId: "cand-1",
          candidateDisplayName: "Jane Doe",
          electionId: "e-1",
          districtName: "California",
          districtType: "statewide",
          state: "CA",
          electionDate: "2026-11-03",
          officialBallotTitle: "Unknown Office",
          electionStage: "general",
          senateClass: null,
          termEndYear: null,
          officeId: null,
          discoveryContestFamily: null,
          electionSources: [],
        },
      ],
    });

    const result = await loadCandidateElectionOfficeContext({ query }, "cand-1", "e-1");

    expect(result?.candidateId).toBe("cand-1");
    expect(result?.officeId).toBeNull();
    expect(result?.discoveryContestFamily).toBeNull();
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).not.toContain("e.office_id IS NOT NULL");
  });

  it("returns null when no linked row is found", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });

    const result = await loadCandidateElectionOfficeContext({ query }, "cand-x", "e-x");

    expect(result).toBeNull();
  });
});

describe("loadCandidatePresidentialCycleOfficeContext", () => {
  it("returns President office context for presidential-cycle record drafts", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          candidateId: "cand-president",
          candidateDisplayName: "Jane President",
          presidentialCycleId: "cycle-2028",
          electionYear: 2028,
          stage: "primary",
          party: "Democratic",
          officeId: "office-president",
          sources: ["https://example.gov/cycle"],
        },
      ],
    });

    const result = await loadCandidatePresidentialCycleOfficeContext(
      { query },
      " cand-president ",
      " cycle-2028 ",
      "president"
    );

    expect(result).toEqual({
      candidateId: "cand-president",
      candidateDisplayName: "Jane President",
      electionId: "",
      presidentialCycleId: "cycle-2028",
      presidentialRole: "president",
      districtName: "United States",
      districtType: "presidential",
      state: "US",
      electionDate: "2028-11-07",
      officialBallotTitle: "President of the United States, 2028 Democratic primary",
      electionStage: "primary",
      senateClass: null,
      termEndYear: null,
      officeId: "office-president",
      discoveryContestFamily: null,
      electionSources: ["https://example.gov/cycle"],
    });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("JOIN public.offices AS office");
    expect(query.mock.calls[0]?.[0]).toContain("JOIN public.presidential_cycle_candidates AS cycle_candidate");
    expect(query.mock.calls[0]?.[0]).toContain("$4 = 'president'");
    expect(query.mock.calls[0]?.[0]).toContain("cycle_candidate.candidate_id = c.id");
    expect(query.mock.calls[0]?.[0]).toContain("office.scope = 'presidential'");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "cand-president",
      "cycle-2028",
      "President of the United States",
      "president",
    ]);
  });

  it("returns Vice President office context for running-mate record drafts", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          candidateId: "cand-vp",
          candidateDisplayName: "Pat Running Mate",
          presidentialCycleId: "cycle-2028",
          electionYear: 2028,
          stage: "general",
          party: null,
          officeId: "office-vp",
          sources: [],
        },
      ],
    });

    const result = await loadCandidatePresidentialCycleOfficeContext(
      { query },
      "cand-vp",
      "cycle-2028",
      "vice_president"
    );

    expect(result?.officeId).toBe("office-vp");
    expect(result?.officialBallotTitle).toBe(
      "Vice President of the United States, 2028 general election"
    );
    expect(query.mock.calls[0]?.[1]).toEqual([
      "cand-vp",
      "cycle-2028",
      "Vice President of the United States",
      "vice_president",
    ]);
    expect(query.mock.calls[0]?.[0]).toContain("$4 = 'vice_president'");
    expect(query.mock.calls[0]?.[0]).toContain("cycle_candidate.running_mate_candidate_id = c.id");
  });

  it("returns null for blank candidate or cycle ids", async () => {
    const query = vi.fn();

    await expect(
      loadCandidatePresidentialCycleOfficeContext({ query }, " ", "cycle-2028", "president")
    ).resolves.toBeNull();
    await expect(
      loadCandidatePresidentialCycleOfficeContext({ query }, "cand-1", " ", "president")
    ).resolves.toBeNull();

    expect(query).not.toHaveBeenCalled();
  });
});
