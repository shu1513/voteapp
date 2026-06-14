import { describe, expect, it, vi } from "vitest";

import {
  loadActivePresidentialCycleCandidatesForNomineeResolution,
  resolvePresidentialNomineeCandidate,
  type PresidentialNomineeCandidateForResolution,
} from "../../../src/pipeline/presidential/presidentialNomineeResolver.js";

const candidates: PresidentialNomineeCandidateForResolution[] = [
  {
    candidateId: "candidate-1",
    displayName: "Jane President",
    party: "Democratic",
    fecIds: ["P80000001"],
  },
  {
    candidateId: "candidate-2",
    displayName: "Pat Candidate",
    party: "Democratic",
    fecIds: [],
  },
];

describe("loadActivePresidentialCycleCandidatesForNomineeResolution", () => {
  it("loads active presidential cycle candidates and keeps rows without FEC IDs", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          candidate_id: "candidate-1",
          display_name: " Jane President ",
          first_name: "Jane",
          last_name: "President",
          party: " Democratic ",
          fec_ids: [" p80000001 ", "P80000001", "H0CA00001", "PABCDEFGH", "P80000002"],
        },
        {
          candidate_id: "candidate-2",
          display_name: "Pat Candidate",
          first_name: "Pat",
          last_name: "Candidate",
          party: "Democratic",
          fec_ids: null,
        },
      ],
    });

    await expect(
      loadActivePresidentialCycleCandidatesForNomineeResolution({ query } as never, " cycle-1 ")
    ).resolves.toEqual([
      {
        candidateId: "candidate-1",
        displayName: "Jane President",
        party: "Democratic",
        fecIds: ["P80000001", "P80000002"],
      },
      {
        candidateId: "candidate-2",
        displayName: "Pat Candidate",
        party: "Democratic",
        fecIds: [],
      },
    ]);

    expect(query).toHaveBeenCalledTimes(1);
    expect(String(query.mock.calls[0]?.[0])).toContain("FROM public.presidential_cycle_candidates");
    expect(String(query.mock.calls[0]?.[0])).toContain("cycle_candidate.status = 'active'");
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-1"]);
  });

  it("falls back to first and last name when display_name is blank", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          candidate_id: "candidate-1",
          display_name: " ",
          first_name: "Jane",
          last_name: "President",
          party: "Republican",
          fec_ids: ["P80000001"],
        },
      ],
    });

    await expect(
      loadActivePresidentialCycleCandidatesForNomineeResolution({ query } as never, "cycle-1")
    ).resolves.toEqual([
      {
        candidateId: "candidate-1",
        displayName: "Jane President",
        party: "Republican",
        fecIds: ["P80000001"],
      },
    ]);
  });

  it("rejects blank cycle IDs before querying", async () => {
    const query = vi.fn();

    await expect(
      loadActivePresidentialCycleCandidatesForNomineeResolution({ query } as never, "   ")
    ).rejects.toThrow("presidential cycle id is required");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("resolvePresidentialNomineeCandidate", () => {
  it("returns no_nominee_found when the AI did not find a nominee", () => {
    expect(
      resolvePresidentialNomineeCandidate({
        payload: {
          nominee_found: false,
          sources: ["https://example.org/no-nominee"],
        },
        candidates,
      })
    ).toEqual({
      status: "no_nominee_found",
      sources: ["https://example.org/no-nominee"],
    });
  });

  it("matches by exact FEC ID before name", () => {
    expect(
      resolvePresidentialNomineeCandidate({
        payload: {
          nominee_found: true,
          candidate_name: "Wrong Name",
          fec_candidate_id: "P80000001",
          sources: ["https://example.org/nominee"],
        },
        candidates,
      })
    ).toEqual({
      status: "matched",
      candidateId: "candidate-1",
      displayName: "Jane President",
      method: "exact_fec_id",
      candidateName: "Wrong Name",
      fecCandidateId: "P80000001",
      sources: ["https://example.org/nominee"],
    });
  });

  it("falls back to exact name when FEC is absent", () => {
    expect(
      resolvePresidentialNomineeCandidate({
        payload: {
          nominee_found: true,
          candidate_name: "Pat Candidate",
          sources: ["https://example.org/nominee"],
        },
        candidates,
      })
    ).toEqual({
      status: "matched",
      candidateId: "candidate-2",
      displayName: "Pat Candidate",
      method: "exact_name",
      candidateName: "Pat Candidate",
      sources: ["https://example.org/nominee"],
    });
  });

  it("falls back to exact name when a provided FEC ID is not linked yet", () => {
    expect(
      resolvePresidentialNomineeCandidate({
        payload: {
          nominee_found: true,
          candidate_name: "Pat Candidate",
          fec_candidate_id: "P80000999",
          sources: ["https://example.org/nominee"],
        },
        candidates,
      })
    ).toEqual({
      status: "matched",
      candidateId: "candidate-2",
      displayName: "Pat Candidate",
      method: "exact_name",
      candidateName: "Pat Candidate",
      fecCandidateId: "P80000999",
      sources: ["https://example.org/nominee"],
    });
  });

  it("returns ambiguous for duplicate FEC matches", () => {
    const result = resolvePresidentialNomineeCandidate({
      payload: {
        nominee_found: true,
        candidate_name: "Jane President",
        fec_candidate_id: "P80000001",
        sources: ["https://example.org/nominee"],
      },
      candidates: [
        candidates[0]!,
        {
          candidateId: "candidate-duplicate",
          displayName: "Jane Duplicate",
          party: "Democratic",
          fecIds: ["P80000001"],
        },
      ],
    });

    expect(result.status).toBe("ambiguous");
    expect(result).toMatchObject({
      reason: "multiple active cycle candidates share the nominee FEC ID",
      candidateName: "Jane President",
      fecCandidateId: "P80000001",
    });
  });

  it("returns ambiguous for duplicate exact-name matches", () => {
    const result = resolvePresidentialNomineeCandidate({
      payload: {
        nominee_found: true,
        candidate_name: "Jane President",
        sources: ["https://example.org/nominee"],
      },
      candidates: [
        candidates[0]!,
        {
          candidateId: "candidate-duplicate",
          displayName: "Jane President",
          party: "Democratic",
          fecIds: [],
        },
      ],
    });

    expect(result.status).toBe("ambiguous");
    expect(result).toMatchObject({
      reason: "multiple active cycle candidates match the nominee name",
      candidateName: "Jane President",
    });
  });

  it("returns unmatched when nominee identity matches no active cycle candidate", () => {
    expect(
      resolvePresidentialNomineeCandidate({
        payload: {
          nominee_found: true,
          candidate_name: "Unknown Nominee",
          fec_candidate_id: "P80000999",
          sources: ["https://example.org/nominee"],
        },
        candidates,
      })
    ).toEqual({
      status: "unmatched",
      reason: "nominee FEC ID and nominee name did not match any active cycle candidate",
      candidateName: "Unknown Nominee",
      fecCandidateId: "P80000999",
      sources: ["https://example.org/nominee"],
    });
  });
});
