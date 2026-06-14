import { describe, expect, it, vi } from "vitest";

import {
  buildCandidateRecordIdentityKey,
  loadRecentCandidateRecordsForDuplicateAvoidance,
  scoreCandidateRecordDescriptionSimilarity,
  upsertCandidateRecords,
} from "../../src/pipeline/candidates/candidateRecordStore.js";

describe("buildCandidateRecordIdentityKey", () => {
  it("normalizes casing, punctuation, spacing, and trailing slash", () => {
    const left = buildCandidateRecordIdentityKey({
      description: "  City-Council   Vote  ",
      sourceUrl: "HTTPS://Example.com/path///",
      eventDate: "2026-05-01",
    });

    const right = buildCandidateRecordIdentityKey({
      description: "city council vote",
      sourceUrl: "https://example.com/path",
      eventDate: new Date("2026-05-01T12:00:00.000Z"),
    });

    expect(left).toBe(right);
  });

  it("changes when event date changes", () => {
    const first = buildCandidateRecordIdentityKey({
      description: "City Council Vote",
      sourceUrl: "https://example.com/path",
      eventDate: "2026-05-01",
    });
    const second = buildCandidateRecordIdentityKey({
      description: "City Council Vote",
      sourceUrl: "https://example.com/path",
      eventDate: "2026-05-02",
    });

    expect(first).not.toBe(second);
  });
});

describe("scoreCandidateRecordDescriptionSimilarity", () => {
  it("scores normalized-equivalent descriptions as exact matches", () => {
    expect(
      scoreCandidateRecordDescriptionSimilarity(
        "Candidate sponsored Bill A.",
        " candidate sponsored bill a "
      )
    ).toBe(1);
  });

  it("scores unrelated descriptions below the update threshold", () => {
    expect(
      scoreCandidateRecordDescriptionSimilarity(
        "Candidate sponsored a transit funding bill.",
        "Candidate was listed on the primary ballot."
      )
    ).toBeLessThan(0.86);
  });
});

describe("upsertCandidateRecords", () => {
  it("counts inserted and updated rows from upsert RETURNING marker", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "record-1", inserted: true }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: "record-2", inserted: false }] });
    const client = { query };

    const result = await upsertCandidateRecords(client, [
      {
        candidateId: "cand-1",
        description: "Desc A",
        sourceUrl: "https://example.com/a",
        eventDate: "2026-04-01",
      },
      {
        candidateId: "cand-1",
        description: "Desc B",
        sourceUrl: "https://example.com/b",
        eventDate: "2026-04-02",
      },
    ]);

    expect(result.inserted).toBe(1);
    expect(result.updated).toBe(1);
    expect(result.processed).toBe(2);
    expect(result.recordIdsByIdentityKey.size).toBe(2);
    expect(query).toHaveBeenCalledTimes(4);
    expect(query.mock.calls[1]?.[0]).toContain("ON CONFLICT (candidate_id, record_identity_key)");
  });

  it("updates a highly similar existing record for the same candidate, source, and date", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          {
            id: "existing-record",
            description: "Candidate sponsored a transit funding bill.",
            record_identity_key: "v3_old",
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] });
    const client = { query };

    const result = await upsertCandidateRecords(client, [
      {
        candidateId: "cand-1",
        description: "Candidate sponsored transit funding bill",
        sourceUrl: "https://example.com/a",
        eventDate: "2026-04-01",
      },
    ]);

    expect(result.inserted).toBe(0);
    expect(result.updated).toBe(1);
    expect(result.recordIdsByIdentityKey.size).toBe(1);
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("UPDATE public.candidate_records");
  });
});

describe("loadRecentCandidateRecordsForDuplicateAvoidance", () => {
  it("loads bounded recent records for duplicate-avoidance prompt context", async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [
        {
          description: "Candidate signed a public safety bill.",
          sourceUrl: "https://example.gov/bill",
          eventDate: "2024-06-01",
        },
      ],
    });

    const result = await loadRecentCandidateRecordsForDuplicateAvoidance(
      { query },
      " candidate-1 ",
      100
    );

    expect(result).toEqual([
      {
        description: "Candidate signed a public safety bill.",
        sourceUrl: "https://example.gov/bill",
        eventDate: "2024-06-01",
      },
    ]);
    expect(query).toHaveBeenCalledWith(expect.stringContaining("FROM public.candidate_records"), [
      "candidate-1",
      40,
    ]);
  });

  it("returns no records without querying when candidate ID is blank", async () => {
    const query = vi.fn();

    await expect(loadRecentCandidateRecordsForDuplicateAvoidance({ query }, "   ")).resolves.toEqual([]);
    expect(query).not.toHaveBeenCalled();
  });
});
