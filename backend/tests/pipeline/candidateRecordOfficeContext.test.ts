import { describe, expect, it, vi } from "vitest";

import { loadCandidateElectionOfficeContext } from "../../src/pipeline/candidates/candidateRecordOfficeContext.js";

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
