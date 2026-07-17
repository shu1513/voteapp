import { describe, expect, it, vi } from "vitest";

import {
  findPresidentialCycleCandidateIdByFecId,
  findTicketLeadCandidateIdByDisplayName,
  markPresidentialCycleCandidateProfileResearched,
  markPresidentialCycleCandidateRunningMateProfileResearched,
  setCandidateElectionRunningMate,
  setPresidentialCycleCandidateRunningMate,
  upsertCandidateElection,
  upsertPresidentialCycleCandidate,
  withdrawPresidentialCycleCandidateByCandidateId,
  withdrawPresidentialCycleCandidateByFecId,
} from "../../src/pipeline/candidates/candidateProfileLinks.js";

describe("setCandidateElectionRunningMate", () => {
  it("sets the running mate on the ticket lead's candidate_elections row", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await expect(
      setCandidateElectionRunningMate({
        db: { query } as never,
        electionId: "election-1",
        candidateId: "lead-1",
        runningMateCandidateId: "mate-1",
      })
    ).resolves.toEqual({ updatedCount: 1 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("UPDATE public.candidate_elections");
    expect(sql).toContain("running_mate_candidate_id = $3::uuid");
    expect(sql).toContain("running_mate_candidate_id IS DISTINCT FROM $3::uuid");
    expect(query.mock.calls[0]?.[1]).toEqual(["election-1", "lead-1", "mate-1"]);
  });

  it("rejects a running mate equal to the ticket lead", async () => {
    const query = vi.fn();

    await expect(
      setCandidateElectionRunningMate({
        db: { query } as never,
        electionId: "election-1",
        candidateId: "same-1",
        runningMateCandidateId: "same-1",
      })
    ).rejects.toThrow("running mate candidate id must differ from the ticket lead candidate id");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("findTicketLeadCandidateIdByDisplayName", () => {
  it("returns the single matching lead candidate id", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ candidate_id: "lead-1" }] });

    await expect(
      findTicketLeadCandidateIdByDisplayName({
        db: { query } as never,
        electionId: "election-1",
        leadDisplayName: "Begich, Tom",
      })
    ).resolves.toEqual({ ok: true, candidateId: "lead-1" });
    expect(String(query.mock.calls[0]?.[0])).toContain("split_part($2, ',', 2)");
    expect(query.mock.calls[0]?.[1]).toEqual(["election-1", "Begich, Tom"]);
  });

  it("fails closed on not-found and ambiguous leads", async () => {
    const emptyQuery = vi.fn().mockResolvedValue({ rows: [] });
    await expect(
      findTicketLeadCandidateIdByDisplayName({
        db: { query: emptyQuery } as never,
        electionId: "election-1",
        leadDisplayName: "Begich, Tom",
      })
    ).resolves.toEqual({ ok: false, reason: "not_found" });

    const ambiguousQuery = vi
      .fn()
      .mockResolvedValue({ rows: [{ candidate_id: "lead-1" }, { candidate_id: "lead-2" }] });
    await expect(
      findTicketLeadCandidateIdByDisplayName({
        db: { query: ambiguousQuery } as never,
        electionId: "election-1",
        leadDisplayName: "Begich, Tom",
      })
    ).resolves.toEqual({ ok: false, reason: "ambiguous" });
  });
});

describe("upsertCandidateElection", () => {
  it("upserts a declared candidate election link", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [{ created: true }] });

    await expect(upsertCandidateElection({
      client: { query } as never,
      candidateId: "candidate-1",
      electionId: "election-1",
      isIncumbent: true,
    })).resolves.toEqual({ created: true });

    expect(query).toHaveBeenCalledTimes(1);
    expect(String(query.mock.calls[0]?.[0])).toContain("INSERT INTO public.candidate_elections");
    expect(String(query.mock.calls[0]?.[0])).toContain("status");
    expect(String(query.mock.calls[0]?.[0])).toContain("'declared'");
    expect(String(query.mock.calls[0]?.[0])).toContain("RETURNING (xmax = 0) AS created");
    expect(query.mock.calls[0]?.[1]).toEqual(["candidate-1", "election-1", true]);
  });

  it("preserves a withdrawn status on re-upsert", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [{ created: false }] });

    await upsertCandidateElection({
      client: { query } as never,
      candidateId: "candidate-1",
      electionId: "election-1",
      isIncumbent: false,
    });

    // A profile re-run replays this upsert for existing links; without the
    // CASE guard it would flip a manually recorded withdrawal back to
    // 'declared' because stale research sources keep listing the candidate.
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("WHEN candidate_elections.status = 'withdrawn' THEN candidate_elections.status");
    expect(sql).not.toContain("status = EXCLUDED.status,");
  });

  it("defaults unknown incumbent status to false", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [{ created: false }] });

    await expect(upsertCandidateElection({
      client: { query } as never,
      candidateId: "candidate-1",
      electionId: "election-1",
      isIncumbent: undefined,
    })).resolves.toEqual({ created: false });

    expect(query.mock.calls[0]?.[1]).toEqual(["candidate-1", "election-1", false]);
  });

  it("does not treat an upsert without a returning row as a new candidate-election link", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await expect(upsertCandidateElection({
      client: { query } as never,
      candidateId: "candidate-1",
      electionId: "election-1",
      isIncumbent: false,
    })).resolves.toEqual({ created: false });
  });
});

describe("upsertPresidentialCycleCandidate", () => {
  it("upserts an active presidential cycle candidate link", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await upsertPresidentialCycleCandidate({
      client: { query } as never,
      cycleId: "cycle-1",
      candidateId: "candidate-1",
      party: "Democratic",
      sources: [" https://example.com/a ", "https://example.com/a", "https://example.com/b"],
    });

    expect(query).toHaveBeenCalledTimes(1);
    expect(String(query.mock.calls[0]?.[0])).toContain("INSERT INTO public.presidential_cycle_candidates");
    expect(String(query.mock.calls[0]?.[0])).toContain("ON CONFLICT (cycle_id, candidate_id) DO UPDATE");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "cycle-1",
      "candidate-1",
      "Democratic",
      "active",
      JSON.stringify(["https://example.com/a", "https://example.com/b"]),
    ]);
  });

  it("supports withdrawn presidential cycle candidate links", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await upsertPresidentialCycleCandidate({
      client: { query } as never,
      cycleId: "cycle-1",
      candidateId: "candidate-1",
      party: "Republican",
      status: "withdrawn",
    });

    expect(query.mock.calls[0]?.[1]).toEqual([
      "cycle-1",
      "candidate-1",
      "Republican",
      "withdrawn",
      JSON.stringify([]),
    ]);
  });

  it("rejects blank presidential candidate party before writing", async () => {
    const query = vi.fn();

    await expect(
      upsertPresidentialCycleCandidate({
        client: { query } as never,
        cycleId: "cycle-1",
        candidateId: "candidate-1",
        party: "   ",
      })
    ).rejects.toThrow("presidential cycle candidate party is required");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("findPresidentialCycleCandidateIdByFecId", () => {
  it("finds a presidential cycle candidate link by candidate FEC ID", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [{ candidate_id: "candidate-1" }] });

    await expect(
      findPresidentialCycleCandidateIdByFecId({
        db: { query } as never,
        cycleId: " cycle-1 ",
        fecCandidateId: " p80000001 ",
      })
    ).resolves.toBe("candidate-1");

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.presidential_cycle_candidates AS cycle_candidate");
    expect(sql).toContain("jsonb_array_elements_text(candidate.fec_ids)");
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-1", "P80000001"]);
  });

  it("returns null when no presidential cycle candidate matches the FEC ID", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 0, rows: [] });

    await expect(
      findPresidentialCycleCandidateIdByFecId({
        db: { query } as never,
        cycleId: "cycle-1",
        fecCandidateId: "P80000001",
      })
    ).resolves.toBeNull();
  });

  it("rejects blank FEC IDs before finding a presidential cycle candidate", async () => {
    const query = vi.fn();

    await expect(
      findPresidentialCycleCandidateIdByFecId({
        db: { query } as never,
        cycleId: "cycle-1",
        fecCandidateId: " ",
      })
    ).rejects.toThrow("presidential FEC candidate id is required");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("markPresidentialCycleCandidateProfileResearched", () => {
  it("marks a presidential candidate profile as researched once", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await expect(
      markPresidentialCycleCandidateProfileResearched({
        db: { query } as never,
        cycleId: " cycle-1 ",
        candidateId: " candidate-1 ",
      })
    ).resolves.toEqual({ updatedCount: 1 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("UPDATE public.presidential_cycle_candidates");
    expect(sql).toContain("presidential_profile_researched = true");
    expect(sql).toContain("presidential_profile_researched = false");
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-1", "candidate-1"]);
  });

  it("rejects blank candidate IDs before marking president profile research", async () => {
    const query = vi.fn();

    await expect(
      markPresidentialCycleCandidateProfileResearched({
        db: { query } as never,
        cycleId: "cycle-1",
        candidateId: " ",
      })
    ).rejects.toThrow("candidate id is required");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("setPresidentialCycleCandidateRunningMate", () => {
  it("links a running mate and resets running mate profile research when the link changes", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await expect(
      setPresidentialCycleCandidateRunningMate({
        db: { query } as never,
        cycleId: "cycle-1",
        candidateId: "candidate-1",
        runningMateCandidateId: "running-mate-1",
      })
    ).resolves.toEqual({ updatedCount: 1 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("running_mate_candidate_id = $3::uuid");
    expect(sql).toContain("running_mate_profile_researched = false");
    expect(sql).toContain("running_mate_candidate_id IS DISTINCT FROM $3::uuid");
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-1", "candidate-1", "running-mate-1"]);
  });

  it("rejects blank running mate IDs before linking", async () => {
    const query = vi.fn();

    await expect(
      setPresidentialCycleCandidateRunningMate({
        db: { query } as never,
        cycleId: "cycle-1",
        candidateId: "candidate-1",
        runningMateCandidateId: " ",
      })
    ).rejects.toThrow("running mate candidate id is required");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("markPresidentialCycleCandidateRunningMateProfileResearched", () => {
  it("marks a linked running mate profile as researched once", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await expect(
      markPresidentialCycleCandidateRunningMateProfileResearched({
        db: { query } as never,
        cycleId: "cycle-1",
        candidateId: "candidate-1",
        runningMateCandidateId: "running-mate-1",
      })
    ).resolves.toEqual({ updatedCount: 1 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("running_mate_profile_researched = true");
    expect(sql).toContain("running_mate_candidate_id = $3::uuid");
    expect(sql).toContain("running_mate_profile_researched = false");
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-1", "candidate-1", "running-mate-1"]);
  });

  it("rejects blank running mate IDs before marking running mate profile research", async () => {
    const query = vi.fn();

    await expect(
      markPresidentialCycleCandidateRunningMateProfileResearched({
        db: { query } as never,
        cycleId: "cycle-1",
        candidateId: "candidate-1",
        runningMateCandidateId: " ",
      })
    ).rejects.toThrow("running mate candidate id is required");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("withdrawPresidentialCycleCandidateByFecId", () => {
  it("demotes an existing presidential cycle candidate matched by FEC ID", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await expect(
      withdrawPresidentialCycleCandidateByFecId({
        db: { query } as never,
        cycleId: "cycle-1",
        fecCandidateId: " p80000001 ",
      })
    ).resolves.toEqual({ updatedCount: 1 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("UPDATE public.presidential_cycle_candidates");
    expect(sql).toContain("jsonb_array_elements_text(candidate.fec_ids)");
    expect(sql).toContain("cycle_candidate.status <> 'withdrawn'");
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-1", "P80000001"]);
  });

  it("rejects blank FEC IDs before writing", async () => {
    const query = vi.fn();

    await expect(
      withdrawPresidentialCycleCandidateByFecId({
        db: { query } as never,
        cycleId: "cycle-1",
        fecCandidateId: " ",
      })
    ).rejects.toThrow("presidential FEC candidate id is required");
    expect(query).not.toHaveBeenCalled();
  });
});

describe("withdrawPresidentialCycleCandidateByCandidateId", () => {
  it("demotes an exact presidential cycle candidate link by candidate ID", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await expect(
      withdrawPresidentialCycleCandidateByCandidateId({
        db: { query } as never,
        cycleId: "cycle-1",
        candidateId: " candidate-1 ",
      })
    ).resolves.toEqual({ updatedCount: 1 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("UPDATE public.presidential_cycle_candidates");
    expect(sql).toContain("candidate_id = $2");
    expect(sql).toContain("status <> 'withdrawn'");
    expect(query.mock.calls[0]?.[1]).toEqual(["cycle-1", "candidate-1"]);
  });

  it("rejects blank candidate IDs before writing", async () => {
    const query = vi.fn();

    await expect(
      withdrawPresidentialCycleCandidateByCandidateId({
        db: { query } as never,
        cycleId: "cycle-1",
        candidateId: " ",
      })
    ).rejects.toThrow("candidate id is required");
    expect(query).not.toHaveBeenCalled();
  });
});
