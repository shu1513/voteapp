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
    expect(args[7]).toBe("candidate_record");
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
});
