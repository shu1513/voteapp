import { describe, expect, it, vi } from "vitest";

import {
  runCandidateRecordsSearchLifecycle,
  summarizeCandidateRecordsLifecycleResults,
} from "../../src/pipeline/candidates/candidateRecordsSearchLifecycle.js";

describe("runCandidateRecordsSearchLifecycle", () => {
  it("skips when claim is not granted", async () => {
    const query = vi.fn().mockResolvedValueOnce({ rows: [] });
    const executeSearch = vi.fn();

    const result = await runCandidateRecordsSearchLifecycle(
      { query },
      { candidateId: "cand-1", asOf: new Date("2026-05-31T00:00:00.000Z") },
      executeSearch
    );

    expect(result).toEqual({
      status: "skipped",
      reason: "cooldown_or_active_claim",
      metrics: {
        discovered_count: 0,
        inserted_count: 0,
        deduped_count: 0,
        tagged_specific_count: 0,
        tagged_general_count: 0,
      },
    });
    expect(executeSearch).not.toHaveBeenCalled();
  });

  it("claims, executes, and marks completion on success", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: "cand-2",
            last_records_searched_at: "2026-01-01T00:00:00.000Z",
            last_records_researched_through: "2026-01-15",
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 });

    const executeSearch = vi.fn().mockResolvedValue({
      discovered_count: 10,
      inserted_count: 7,
      deduped_count: 3,
      tagged_specific_count: 6,
      tagged_general_count: 1,
    });

    const result = await runCandidateRecordsSearchLifecycle(
      { query },
      {
        candidateId: "cand-2",
        asOf: new Date("2026-05-31T00:00:00.000Z"),
        overlapDays: 45,
      },
      executeSearch
    );

    expect(result.status).toBe("completed");
    if (result.status === "completed") {
      expect(result.window).toEqual({
        mode: "incremental",
        sinceDate: "2025-12-01",
      });
      expect(result.metrics.inserted_count).toBe(7);
    }

    expect(executeSearch).toHaveBeenCalledTimes(1);
    expect(executeSearch).toHaveBeenCalledWith({
      candidateId: "cand-2",
      window: {
        mode: "incremental",
        sinceDate: "2025-12-01",
      },
    });
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("last_records_searched_at = now()");
  });

  it("releases claim and rethrows when executeSearch fails", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: "cand-3",
            last_records_searched_at: null,
            last_records_researched_through: null,
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 });

    const executeSearch = vi.fn().mockRejectedValue(new Error("search failed"));

    await expect(
      runCandidateRecordsSearchLifecycle(
        { query },
        {
          candidateId: "cand-3",
          asOf: new Date("2026-05-31T00:00:00.000Z"),
          overlapDays: 45,
        },
        executeSearch
      )
    ).rejects.toThrow("search failed");

    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("records_search_claimed_at = NULL");
    expect(query.mock.calls[1]?.[1]).toEqual(["cand-3"]);
  });

  it("marks completion even when no new records are discovered", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: "cand-10",
            last_records_searched_at: "2026-01-01T00:00:00.000Z",
            last_records_researched_through: "2026-01-31",
          },
        ],
      })
      .mockResolvedValueOnce({ rowCount: 1 });

    const executeSearch = vi.fn().mockResolvedValue({
      discovered_count: 0,
      inserted_count: 0,
      deduped_count: 0,
      tagged_specific_count: 0,
      tagged_general_count: 0,
    });

    const result = await runCandidateRecordsSearchLifecycle(
      { query },
      {
        candidateId: "cand-10",
        asOf: new Date("2026-05-31T00:00:00.000Z"),
      },
      executeSearch
    );

    expect(result.status).toBe("completed");
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("last_records_searched_at = now()");
  });
});

describe("summarizeCandidateRecordsLifecycleResults", () => {
  it("aggregates claimed and skipped outcomes", () => {
    const summary = summarizeCandidateRecordsLifecycleResults([
      {
        status: "skipped",
        reason: "cooldown_or_active_claim",
        metrics: {
          discovered_count: 0,
          inserted_count: 0,
          deduped_count: 0,
          tagged_specific_count: 0,
          tagged_general_count: 0,
        },
      },
      {
        status: "completed",
        candidateId: "cand-9",
        window: { mode: "full", sinceDate: null },
        metrics: {
          discovered_count: 4,
          inserted_count: 3,
          deduped_count: 1,
          tagged_specific_count: 2,
          tagged_general_count: 1,
        },
      },
    ]);

    expect(summary).toEqual({
      claimed_count: 1,
      skipped_cooldown_or_claim_count: 1,
      discovered_count: 4,
      inserted_count: 3,
      deduped_count: 1,
      tagged_specific_count: 2,
      tagged_general_count: 1,
    });
  });
});
