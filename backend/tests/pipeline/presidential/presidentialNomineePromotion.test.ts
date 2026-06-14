import { describe, expect, it, vi } from "vitest";

import {
  promoteMatchedPresidentialNominee,
  promotePresidentialNomineeFromResolution,
  PromotePresidentialNomineeError,
} from "../../../src/pipeline/presidential/presidentialNomineePromotion.js";

const PRIMARY_CYCLE_ID = "11111111-1111-4111-8111-111111111111";
const GENERAL_CYCLE_ID = "22222222-2222-4222-8222-222222222222";
const NOMINEE_CANDIDATE_ID = "33333333-3333-4333-8333-333333333333";
const RUNNING_MATE_CANDIDATE_ID = "44444444-4444-4444-8444-444444444444";

function makeDb(query: ReturnType<typeof vi.fn>) {
  const release = vi.fn();
  return {
    db: {
      connect: vi.fn().mockResolvedValue({ query, release }),
    },
    release,
  };
}

function mockSuccessfulPromotionQueries() {
  return vi
    .fn()
    .mockResolvedValueOnce({ rows: [], rowCount: 0 }) // BEGIN
    .mockResolvedValueOnce({
      rows: [{ id: PRIMARY_CYCLE_ID, nominee_candidate_id: null }],
      rowCount: 1,
    })
    .mockResolvedValueOnce({ rows: [{ running_mate_candidate_id: RUNNING_MATE_CANDIDATE_ID }], rowCount: 1 })
    .mockResolvedValueOnce({
      rows: [{ id: GENERAL_CYCLE_ID }],
      rowCount: 1,
    })
    .mockResolvedValueOnce({ rows: [], rowCount: 1 })
    .mockResolvedValueOnce({ rows: [], rowCount: 1 })
    .mockResolvedValueOnce({ rows: [], rowCount: 0 }); // COMMIT
}

describe("promoteMatchedPresidentialNominee", () => {
  it("completes the primary cycle and links the nominee into the general cycle atomically", async () => {
    const query = mockSuccessfulPromotionQueries();
    const { db, release } = makeDb(query);

    await expect(
      promoteMatchedPresidentialNominee({
        db: db as never,
        primaryCycleId: ` ${PRIMARY_CYCLE_ID} `,
        electionYear: 2028,
        party: " Democratic ",
        nomineeCandidateId: ` ${NOMINEE_CANDIDATE_ID} `,
        sources: [" https://example.org/nominee ", "https://example.org/nominee"],
        confirmedAt: new Date("2028-03-15T12:00:00.000Z"),
      })
    ).resolves.toEqual({
      status: "promoted",
      primaryCycleId: PRIMARY_CYCLE_ID,
      generalCycleId: GENERAL_CYCLE_ID,
      nomineeCandidateId: NOMINEE_CANDIDATE_ID,
      party: "Democratic",
      sources: ["https://example.org/nominee"],
    });

    expect(query).toHaveBeenCalledTimes(7);
    expect(query.mock.calls[0]?.[0]).toBe("BEGIN");
    expect(String(query.mock.calls[1]?.[0])).toContain("FROM public.presidential_cycles");
    expect(String(query.mock.calls[1]?.[0])).toContain("stage = 'primary'");
    expect(String(query.mock.calls[1]?.[0])).toContain("FOR UPDATE");
    expect(query.mock.calls[1]?.[1]).toEqual([PRIMARY_CYCLE_ID, 2028, "Democratic"]);

    expect(String(query.mock.calls[2]?.[0])).toContain("FROM public.presidential_cycle_candidates");
    expect(String(query.mock.calls[2]?.[0])).toContain("status = 'active'");
    expect(String(query.mock.calls[2]?.[0])).toContain("running_mate_candidate_id");
    expect(query.mock.calls[2]?.[1]).toEqual([PRIMARY_CYCLE_ID, NOMINEE_CANDIDATE_ID]);

    expect(String(query.mock.calls[3]?.[0])).toContain("stage = 'general'");
    expect(query.mock.calls[3]?.[1]).toEqual([2028]);

    expect(String(query.mock.calls[4]?.[0])).toContain("nominee_candidate_id = $2::uuid");
    expect(String(query.mock.calls[4]?.[0])).toContain("status = 'completed'");
    expect(query.mock.calls[4]?.[1]).toEqual([
      PRIMARY_CYCLE_ID,
      NOMINEE_CANDIDATE_ID,
      "2028-03-15T12:00:00.000Z",
      JSON.stringify(["https://example.org/nominee"]),
    ]);

    expect(String(query.mock.calls[5]?.[0])).toContain("INSERT INTO public.presidential_cycle_candidates");
    expect(String(query.mock.calls[5]?.[0])).toContain("running_mate_candidate_id");
    expect(String(query.mock.calls[5]?.[0])).toContain("running_mate_profile_researched = CASE");
    expect(String(query.mock.calls[5]?.[0])).toContain("ON CONFLICT (cycle_id, candidate_id) DO UPDATE");
    expect(query.mock.calls[5]?.[1]).toEqual([
      GENERAL_CYCLE_ID,
      NOMINEE_CANDIDATE_ID,
      "Democratic",
      RUNNING_MATE_CANDIDATE_ID,
      JSON.stringify(["https://example.org/nominee"]),
    ]);
    expect(query.mock.calls[6]?.[0]).toBe("COMMIT");
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("allows idempotent promotion when the primary already has the same nominee", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 0 }) // BEGIN
    .mockResolvedValueOnce({
      rows: [{ id: PRIMARY_CYCLE_ID, nominee_candidate_id: NOMINEE_CANDIDATE_ID.toUpperCase() }],
      rowCount: 1,
    })
      .mockResolvedValueOnce({ rows: [{ running_mate_candidate_id: null }], rowCount: 1 })
      .mockResolvedValueOnce({
        rows: [{ id: GENERAL_CYCLE_ID }],
        rowCount: 1,
      })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 }); // COMMIT
    const { db } = makeDb(query);

    await expect(
      promoteMatchedPresidentialNominee({
        db: db as never,
        primaryCycleId: PRIMARY_CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        nomineeCandidateId: NOMINEE_CANDIDATE_ID,
        sources: ["https://example.org/nominee"],
        confirmedAt: new Date("2028-03-15T12:00:00.000Z"),
      })
    ).resolves.toMatchObject({ status: "promoted" });
  });

  it("rolls back when the primary cycle is missing", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 });
    const { db, release } = makeDb(query);

    await expect(
      promoteMatchedPresidentialNominee({
        db: db as never,
        primaryCycleId: PRIMARY_CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        nomineeCandidateId: NOMINEE_CANDIDATE_ID,
        sources: ["https://example.org/nominee"],
      })
    ).rejects.toMatchObject({
      code: "primary_cycle_not_found",
    });

    expect(query.mock.calls[2]?.[0]).toBe("ROLLBACK");
    expect(release).toHaveBeenCalledTimes(1);
  });

  it("rejects changing a primary cycle to a different nominee", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({
        rows: [
          {
            id: PRIMARY_CYCLE_ID,
            nominee_candidate_id: "44444444-4444-4444-8444-444444444444",
          },
        ],
        rowCount: 1,
      })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 });
    const { db } = makeDb(query);

    await expect(
      promoteMatchedPresidentialNominee({
        db: db as never,
        primaryCycleId: PRIMARY_CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        nomineeCandidateId: NOMINEE_CANDIDATE_ID,
        sources: ["https://example.org/nominee"],
      })
    ).rejects.toMatchObject({
      code: "primary_cycle_already_has_different_nominee",
    });

    expect(query.mock.calls[2]?.[0]).toBe("ROLLBACK");
  });

  it("rejects when the nominee is not active in the primary cycle", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({
        rows: [{ id: PRIMARY_CYCLE_ID, nominee_candidate_id: null }],
        rowCount: 1,
      })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 })
      .mockResolvedValueOnce({ rows: [], rowCount: 0 });
    const { db } = makeDb(query);

    await expect(
      promoteMatchedPresidentialNominee({
        db: db as never,
        primaryCycleId: PRIMARY_CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        nomineeCandidateId: NOMINEE_CANDIDATE_ID,
        sources: ["https://example.org/nominee"],
      })
    ).rejects.toMatchObject({
      code: "nominee_not_active_primary_candidate",
    });

    expect(query.mock.calls[3]?.[0]).toBe("ROLLBACK");
  });

  it("rejects invalid input before opening a transaction", async () => {
    const query = vi.fn();
    const { db } = makeDb(query);

    await expect(
      promoteMatchedPresidentialNominee({
        db: db as never,
        primaryCycleId: "not-a-uuid",
        electionYear: 2028,
        party: "Democratic",
        nomineeCandidateId: NOMINEE_CANDIDATE_ID,
        sources: [],
      })
    ).rejects.toBeInstanceOf(PromotePresidentialNomineeError);

    expect(db.connect).not.toHaveBeenCalled();
  });
});

describe("promotePresidentialNomineeFromResolution", () => {
  it("skips non-matched nominee resolutions without opening a transaction", async () => {
    const query = vi.fn();
    const { db } = makeDb(query);

    await expect(
      promotePresidentialNomineeFromResolution({
        db: db as never,
        primaryCycleId: PRIMARY_CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        resolution: {
          status: "ambiguous",
          reason: "multiple active cycle candidates match the nominee name",
          candidateName: "Jane President",
          candidates: [],
          sources: ["https://example.org/nominee"],
        },
      })
    ).resolves.toEqual({
      status: "skipped",
      reason: "no_matched_nominee",
      resolutionStatus: "ambiguous",
    });

    expect(db.connect).not.toHaveBeenCalled();
  });

  it("promotes matched nominee resolutions", async () => {
    const query = mockSuccessfulPromotionQueries();
    const { db } = makeDb(query);

    await expect(
      promotePresidentialNomineeFromResolution({
        db: db as never,
        primaryCycleId: PRIMARY_CYCLE_ID,
        electionYear: 2028,
        party: "Democratic",
        confirmedAt: new Date("2028-03-15T12:00:00.000Z"),
        resolution: {
          status: "matched",
          candidateId: NOMINEE_CANDIDATE_ID,
          displayName: "Jane President",
          method: "exact_fec_id",
          candidateName: "Jane President",
          fecCandidateId: "P80000001",
          sources: ["https://example.org/nominee"],
        },
      })
    ).resolves.toMatchObject({
      status: "promoted",
      nomineeCandidateId: NOMINEE_CANDIDATE_ID,
      generalCycleId: GENERAL_CYCLE_ID,
    });
  });
});
