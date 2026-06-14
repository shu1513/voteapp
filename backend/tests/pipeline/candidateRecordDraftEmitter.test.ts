import { describe, expect, it, vi } from "vitest";

import { enqueueCandidateRecordDrafts } from "../../src/pipeline/candidates/candidateRecordDraftEmitter.js";

describe("enqueueCandidateRecordDrafts", () => {
  it("emits candidate-record draft with candidate keyed marker", async () => {
    const sendCommand = vi.fn().mockResolvedValueOnce(1);

    const result = await enqueueCandidateRecordDrafts(
      { sendCommand },
      [{ candidateId: "cand-1", electionId: "e-1", runId: "run-1" }]
    );

    expect(result).toEqual({ emittedCount: 1, skippedCount: 0 });
    expect(sendCommand).toHaveBeenCalledTimes(1);
    const args = sendCommand.mock.calls[0]?.[0] as string[];
    expect(args[0]).toBe("EVAL");
    expect(args[2]).toBe("2");
    expect(args[3]).toBe("staging:candidates:record:draft");
    expect(args[4]).toContain("staging:candidate_record_draft_emitted:cand-1");
    expect(args[5]).toBe("cand-1");
    expect(args[6]).toBe("e-1");
    expect(args[7]).toBe("candidate_record");
    expect(args[11]).toBe("election");
    expect(args[12]).toBe("");
    expect(args[13]).toBe("");
  });

  it("skips duplicate candidate id entries in the same batch", async () => {
    const sendCommand = vi.fn().mockResolvedValueOnce(1);

    const result = await enqueueCandidateRecordDrafts(
      { sendCommand },
      [
        { candidateId: "cand-1", electionId: "e-1", runId: "run-1" },
        { candidateId: "cand-1", electionId: "e-2", runId: "run-2" },
      ]
    );

    expect(result).toEqual({ emittedCount: 1, skippedCount: 1 });
    expect(sendCommand).toHaveBeenCalledTimes(1);
  });

  it("counts redis marker hits as skipped", async () => {
    const sendCommand = vi.fn().mockResolvedValueOnce(0);

    const result = await enqueueCandidateRecordDrafts(
      { sendCommand },
      [{ candidateId: "cand-2", electionId: "e-1", runId: null }]
    );

    expect(result).toEqual({ emittedCount: 0, skippedCount: 1 });
  });

  it("emits presidential-cycle candidate-record drafts with cycle and role marker keys", async () => {
    const sendCommand = vi.fn().mockResolvedValueOnce(1);

    const result = await enqueueCandidateRecordDrafts(
      { sendCommand },
      [
        {
          contextType: "presidential_cycle",
          candidateId: " candidate-president ",
          presidentialCycleId: " cycle-2028 ",
          presidentialRole: "president",
          runId: "run-1",
        },
      ]
    );

    expect(result).toEqual({ emittedCount: 1, skippedCount: 0 });
    const args = sendCommand.mock.calls[0]?.[0] as string[];
    expect(args[4]).toBe(
      "staging:candidate_record_draft_emitted:presidential_cycle:candidate-president:cycle-2028:president"
    );
    expect(args[5]).toBe("candidate-president");
    expect(args[6]).toBe("");
    expect(args[7]).toBe("candidate_record");
    expect(args[11]).toBe("presidential_cycle");
    expect(args[12]).toBe("cycle-2028");
    expect(args[13]).toBe("president");
  });

  it("does not collide presidential record draft markers across president and vice-president roles", async () => {
    const sendCommand = vi.fn().mockResolvedValue(1);

    const result = await enqueueCandidateRecordDrafts(
      { sendCommand },
      [
        {
          contextType: "presidential_cycle",
          candidateId: "cand-1",
          presidentialCycleId: "cycle-2028",
          presidentialRole: "president",
          runId: "run-president",
        },
        {
          contextType: "presidential_cycle",
          candidateId: "cand-1",
          presidentialCycleId: "cycle-2028",
          presidentialRole: "vice_president",
          runId: "run-vp",
        },
        {
          contextType: "presidential_cycle",
          candidateId: "cand-1",
          presidentialCycleId: "cycle-2028",
          presidentialRole: "president",
          runId: "run-president-duplicate",
        },
      ]
    );

    expect(result).toEqual({ emittedCount: 2, skippedCount: 1 });
    expect(sendCommand).toHaveBeenCalledTimes(2);
    expect((sendCommand.mock.calls[0]?.[0] as string[])[4]).toContain(":president");
    expect((sendCommand.mock.calls[1]?.[0] as string[])[4]).toContain(":vice_president");
  });
});
