import { describe, expect, it, vi } from "vitest";

import {
  buildCandidateRecordIdentityKey,
  upsertCandidateRecords,
} from "../../src/pipeline/candidates/candidateRecordStore.js";

describe("buildCandidateRecordIdentityKey", () => {
  it("normalizes casing, punctuation, spacing, and trailing slash", () => {
    const left = buildCandidateRecordIdentityKey({
      title: "  City-Council   Vote  ",
      sourceUrl: "HTTPS://Example.com/path///",
      eventDate: "2026-05-01",
    });

    const right = buildCandidateRecordIdentityKey({
      title: "city council vote",
      sourceUrl: "https://example.com/path",
      eventDate: new Date("2026-05-01T12:00:00.000Z"),
    });

    expect(left).toBe(right);
  });

  it("changes when event date changes", () => {
    const first = buildCandidateRecordIdentityKey({
      title: "City Council Vote",
      sourceUrl: "https://example.com/path",
      eventDate: "2026-05-01",
    });
    const second = buildCandidateRecordIdentityKey({
      title: "City Council Vote",
      sourceUrl: "https://example.com/path",
      eventDate: "2026-05-02",
    });

    expect(first).not.toBe(second);
  });
});

describe("upsertCandidateRecords", () => {
  it("counts inserted and updated rows from upsert RETURNING marker", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ inserted: true }] })
      .mockResolvedValueOnce({ rows: [{ inserted: false }] });
    const client = { query };

    const result = await upsertCandidateRecords(client, [
      {
        candidateId: "cand-1",
        title: "Record A",
        description: "Desc A",
        sourceUrl: "https://example.com/a",
        eventDate: "2026-04-01",
      },
      {
        candidateId: "cand-1",
        title: "Record B",
        description: "Desc B",
        sourceUrl: "https://example.com/b",
        eventDate: "2026-04-02",
      },
    ]);

    expect(result).toEqual({ inserted: 1, updated: 1, processed: 2 });
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[0]?.[0]).toContain("ON CONFLICT (candidate_id, record_identity_key)");
  });
});
