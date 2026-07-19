import { describe, expect, it, vi } from "vitest";

import {
  buildCandidateRecordIdentityKey,
  deleteCandidateRecordsForReplacementRefresh,
  findWithinPayloadRecordCollisions,
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
    expect(result.insertedRecordIds).toEqual(["record-1"]);
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
    expect(result.insertedRecordIds).toEqual([]);
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[1]?.[0]).toContain("UPDATE public.candidate_records");
  });
});

describe("deleteCandidateRecordsForReplacementRefresh", () => {
  it("deletes all candidate records for a candidate and returns the deleted count", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 4 });

    await expect(
      deleteCandidateRecordsForReplacementRefresh({ query }, " candidate-1 ")
    ).resolves.toEqual({ deletedCount: 4 });

    expect(query).toHaveBeenCalledWith(expect.stringContaining("DELETE FROM public.candidate_records"), [
      "candidate-1",
    ]);
  });

  it("does not query when candidate ID is blank", async () => {
    const query = vi.fn();

    await expect(deleteCandidateRecordsForReplacementRefresh({ query }, "   ")).resolves.toEqual({
      deletedCount: 0,
    });
    expect(query).not.toHaveBeenCalled();
  });
});

describe("findWithinPayloadRecordCollisions", () => {
  it("flags same-date same-source rows with near-identical descriptions", () => {
    const collisions = findWithinPayloadRecordCollisions([
      {
        description:
          "Voted yes on House Bill 204 to expand the state income tax credit for families",
        sourceUrl: "https://example.gov/session/2025",
        eventDate: "2025-03-26",
      },
      {
        description:
          "Voted yes on House Bill 205 to expand the state income tax credit for families",
        sourceUrl: "https://example.gov/session/2025",
        eventDate: "2025-03-26",
      },
    ]);

    expect(collisions).toHaveLength(1);
    expect(collisions[0]).toMatchObject({
      firstIndex: 0,
      secondIndex: 1,
      eventDate: "2025-03-26",
      sourceUrl: "https://example.gov/session/2025",
    });
    expect(collisions[0]!.similarity).toBeGreaterThanOrEqual(0.86);
  });

  it("normalizes source URLs and date forms before grouping", () => {
    const collisions = findWithinPayloadRecordCollisions([
      {
        description: "Voted yes on Senate Bill 402 concurrence with House amendments",
        sourceUrl: "HTTPS://Example.gov/journal/",
        eventDate: new Date("1986-04-24T12:00:00.000Z"),
      },
      {
        description: "Voted yes on Senate Bill 402 concurrence with House changes",
        sourceUrl: "https://example.gov/journal",
        eventDate: "1986-04-24",
      },
    ]);

    expect(collisions).toHaveLength(1);
    expect(collisions[0]).toMatchObject({
      eventDate: "1986-04-24",
      sourceUrl: "https://example.gov/journal",
    });
  });

  it("ignores rows that differ in event date or source URL", () => {
    const description = "Voted yes on House Bill 204 to expand the tax credit";
    expect(
      findWithinPayloadRecordCollisions([
        { description, sourceUrl: "https://example.gov/a", eventDate: "2025-03-26" },
        { description, sourceUrl: "https://example.gov/b", eventDate: "2025-03-26" },
        { description, sourceUrl: "https://example.gov/a", eventDate: "2025-03-27" },
      ])
    ).toEqual([]);
  });

  it("ignores same-day same-source rows with clearly distinct descriptions", () => {
    expect(
      findWithinPayloadRecordCollisions([
        {
          description: "Voted yes on the fiscal year 2026 operating budget",
          sourceUrl: "https://example.gov/session/2025",
          eventDate: "2025-03-26",
        },
        {
          description: "Spoke against the proposed surveillance camera contract during public comment",
          sourceUrl: "https://example.gov/session/2025",
          eventDate: "2025-03-26",
        },
      ])
    ).toEqual([]);
  });
});
