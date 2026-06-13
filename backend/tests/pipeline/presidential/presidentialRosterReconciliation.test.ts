import { describe, expect, it, vi } from "vitest";

import { loadActivePresidentialCycleCandidatesForReconciliation } from "../../../src/pipeline/presidential/presidentialRosterReconciliation.js";

describe("loadActivePresidentialCycleCandidatesForReconciliation", () => {
  it("loads active presidential cycle candidates with normalized FEC IDs", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          candidate_id: "candidate-1",
          display_name: " Jane President ",
          first_name: "Jane",
          last_name: "President",
          party: "Democratic",
          fec_ids: [" p80000001 ", "P80000001", "H0CA00001", "PABCDEFGH", "P80000002"],
          cycle_sources: [" https://example.org/a ", "https://example.org/a", "https://example.org/b"],
        },
      ],
    });

    await expect(
      loadActivePresidentialCycleCandidatesForReconciliation({ query } as never, " cycle-1 ")
    ).resolves.toEqual([
      {
        candidateId: "candidate-1",
        displayName: "Jane President",
        party: "Democratic",
        fecIds: ["P80000001", "P80000002"],
        sources: ["https://example.org/a", "https://example.org/b"],
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
          cycle_sources: [],
        },
      ],
    });

    await expect(
      loadActivePresidentialCycleCandidatesForReconciliation({ query } as never, "cycle-1")
    ).resolves.toEqual([
      {
        candidateId: "candidate-1",
        displayName: "Jane President",
        party: "Republican",
        fecIds: ["P80000001"],
        sources: [],
      },
    ]);
  });

  it("skips rows without presidential FEC IDs", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          candidate_id: "candidate-1",
          display_name: "Jane President",
          first_name: "Jane",
          last_name: "President",
          party: "Democratic",
          fec_ids: ["H0CA00001", ""],
          cycle_sources: [],
        },
        {
          candidate_id: "candidate-2",
          display_name: "Pat President",
          first_name: "Pat",
          last_name: "President",
          party: "Democratic",
          fec_ids: null,
          cycle_sources: [],
        },
      ],
    });

    await expect(
      loadActivePresidentialCycleCandidatesForReconciliation({ query } as never, "cycle-1")
    ).resolves.toEqual([]);
  });

  it("rejects blank cycle IDs before querying", async () => {
    const query = vi.fn();

    await expect(
      loadActivePresidentialCycleCandidatesForReconciliation({ query } as never, "   ")
    ).rejects.toThrow("presidential cycle id is required");
    expect(query).not.toHaveBeenCalled();
  });
});
