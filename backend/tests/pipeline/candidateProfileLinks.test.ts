import { describe, expect, it, vi } from "vitest";

import {
  findPresidentialCycleCandidateIdByFecId,
  markPresidentialCycleCandidateProfileResearched,
  markPresidentialCycleCandidateRunningMateProfileResearched,
  setPresidentialCycleCandidateRunningMate,
  upsertCandidateElection,
  upsertPresidentialCycleCandidate,
  withdrawPresidentialCycleCandidateByCandidateId,
  withdrawPresidentialCycleCandidateByFecId,
} from "../../src/pipeline/candidates/candidateProfileLinks.js";

describe("upsertCandidateElection", () => {
  it("upserts a declared candidate election link", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await upsertCandidateElection({
      client: { query } as never,
      candidateId: "candidate-1",
      electionId: "election-1",
      isIncumbent: true,
    });

    expect(query).toHaveBeenCalledTimes(1);
    expect(String(query.mock.calls[0]?.[0])).toContain("INSERT INTO public.candidate_elections");
    expect(String(query.mock.calls[0]?.[0])).toContain("status");
    expect(String(query.mock.calls[0]?.[0])).toContain("'declared'");
    expect(query.mock.calls[0]?.[1]).toEqual(["candidate-1", "election-1", true]);
  });

  it("defaults unknown incumbent status to false", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1, rows: [] });

    await upsertCandidateElection({
      client: { query } as never,
      candidateId: "candidate-1",
      electionId: "election-1",
      isIncumbent: undefined,
    });

    expect(query.mock.calls[0]?.[1]).toEqual(["candidate-1", "election-1", false]);
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
