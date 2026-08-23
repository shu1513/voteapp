import { describe, expect, it, vi } from "vitest";

import {
  assertLegislativeVoteStillApproved,
  loadLegislativeVote,
  upsertLegislativeVoteSource,
  type LegislativeVoteSourceRow,
} from "../../../src/pipeline/rollcall/legislativeVoteStore.js";
import { migrationTableColumns } from "../../helpers/migrationTableColumns.js";

const ROW: LegislativeVoteSourceRow = {
  jurisdiction: "US",
  chamber: "house",
  session: "119-1",
  rollNumber: 145,
  voteDate: "2025-05-22",
  measureId: "H R 1",
  exactQuestion: "On Passage",
  isFloorVote: true,
  result: "Passed",
  yeas: 215,
  nays: 214,
  displayUrl: "https://clerk.house.gov/Votes/2025145",
  machineUrl: "https://clerk.house.gov/evs/2025/roll145.xml",
  billUrl: "https://www.congress.gov/bill/119th-congress/house-bill/1",
  sourceSha256: "a".repeat(64),
  fetchedAt: new Date("2026-08-22T12:00:00.000Z"),
  importerVersion: "rollcall-fetch-v1",
};

const EXISTING = {
  id: "row-1",
  review_status: "pending",
  vote_date: "2025-05-22",
  measure_id: "H R 1",
  exact_question: "On Passage",
  is_floor_vote: true,
  result: "Passed",
  yeas: 215,
  nays: 214,
  display_url: ROW.displayUrl,
  machine_url: ROW.machineUrl,
  bill_url: ROW.billUrl,
  source_sha256: ROW.sourceSha256,
};

function fakeDb(existingRows: unknown[]) {
  const query = vi
    .fn()
    .mockResolvedValueOnce({ rows: existingRows })
    .mockResolvedValue({ rows: [{ id: "row-new", review_status: "pending" }] });
  return { query };
}

describe("upsertLegislativeVoteSource", () => {
  it("inserts when no row exists", async () => {
    const db = fakeDb([]);
    await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toEqual({
      outcome: "inserted",
      id: "row-new",
      reviewStatus: "pending",
      judgmentCleared: false,
    });
    const [sql, params] = db.query.mock.calls[1] as [string, unknown[]];
    expect(sql).toMatch(/INSERT INTO legislative_votes/);
    expect(params).toHaveLength(17);
    expect(params[4]).toBe("2025-05-22");
    expect(params[15]).toBe("2026-08-22T12:00:00.000Z");
  });

  it("touches only the bookkeeping columns when nothing changed", async () => {
    const db = fakeDb([EXISTING]);
    await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toEqual({
      outcome: "unchanged",
      id: "row-1",
      reviewStatus: "pending",
      judgmentCleared: false,
    });
    const [sql, params] = db.query.mock.calls[1] as [string, unknown[]];
    expect(sql).toMatch(/SET fetched_at = \$2::timestamptz,\s+importer_version = \$3\s+WHERE id = \$1/);
    expect(sql).not.toMatch(/source_sha256/);
    expect(params).toEqual(["row-1", "2026-08-22T12:00:00.000Z", "rollcall-fetch-v1"]);
  });

  it("rewrites the source columns when the feed changed, keeping the judgment", async () => {
    const db = fakeDb([{ ...EXISTING, source_sha256: "b".repeat(64), yeas: 216 }]);
    await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toEqual({
      outcome: "updated",
      id: "row-1",
      reviewStatus: "pending",
      judgmentCleared: false,
    });
    const [sql, params] = db.query.mock.calls[1] as [string, unknown[]];
    expect(sql).toMatch(/UPDATE legislative_votes/);
    expect(sql).toMatch(/source_sha256 = \$12/);
    expect(sql).not.toMatch(/review_status|yea_description|nay_description|labels_json|reviewed_at/);
    expect(params[0]).toBe("row-1");
    expect(params[11]).toBe(ROW.sourceSha256);
  });

  it("clears the judgment and returns to pending when the question or measure changed", async () => {
    for (const existing of [
      { ...EXISTING, review_status: "rejected", exact_question: "On Motion to Recommit" },
      { ...EXISTING, review_status: "pending", measure_id: "H R 2" },
    ]) {
      const db = fakeDb([existing]);
      await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toEqual({
        outcome: "updated",
        id: "row-1",
        reviewStatus: "pending",
        judgmentCleared: true,
      });
      const [sql] = db.query.mock.calls[1] as [string];
      expect(sql).toMatch(/yea_description = NULL,\s+nay_description = NULL,\s+labels_json = NULL/);
      expect(sql).toMatch(/review_status = 'pending',\s+reviewed_at = NULL/);
    }
  });

  it("treats an is_floor_vote flip alone as a change", async () => {
    const db = fakeDb([{ ...EXISTING, is_floor_vote: false }]);
    await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toMatchObject({ outcome: "updated" });
  });

  it("never writes over an approved row whose source differs", async () => {
    const db = fakeDb([{ ...EXISTING, review_status: "approved", source_sha256: "b".repeat(64) }]);
    await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toEqual({
      outcome: "approved_conflict",
      id: "row-1",
      reviewStatus: "approved",
      judgmentCleared: false,
    });
    expect(db.query).toHaveBeenCalledTimes(1);
  });

  it("still advances fetched_at on an approved row whose source is identical", async () => {
    const db = fakeDb([{ ...EXISTING, review_status: "approved" }]);
    await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toMatchObject({ outcome: "unchanged" });
    expect(db.query).toHaveBeenCalledTimes(2);
  });

  it("only names columns the migrations build", async () => {
    const columns = migrationTableColumns("legislative_votes");
    const db = fakeDb([{ ...EXISTING, exact_question: "On Passage, as Amended" }]);
    await upsertLegislativeVoteSource(db, ROW);
    for (const [sql] of db.query.mock.calls as [string][]) {
      for (const match of sql.matchAll(/\b([a-z_]+) = (?:\$\d+|NULL|'pending')/g)) {
        expect(columns.has(match[1]!), match[1]).toBe(true);
      }
      const insertColumns = /INSERT INTO legislative_votes \(([\s\S]*?)\) VALUES/.exec(sql)?.[1];
      for (const column of insertColumns?.split(",").map((name) => name.trim()) ?? []) {
        expect(columns.has(column), column).toBe(true);
      }
    }
  });
});

describe("loadLegislativeVote", () => {
  it("returns the reviewed row the importer needs, or null, naming only migration columns", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        {
          id: "row-1",
          vote_date: "2025-05-22",
          measure_id: "H R 1",
          exact_question: "On Passage",
          is_floor_vote: true,
          yeas: 215,
          nays: 214,
          machine_url: ROW.machineUrl,
          source_sha256: "a".repeat(64),
          yea_description: "Voted to pass H.R. 1.",
          nay_description: "Voted against passing H.R. 1.",
          labels_json: [{ slug: "immigration", yea: "for" }],
          review_status: "approved",
        },
      ],
    });
    const key = { jurisdiction: "US", chamber: "house" as const, session: "119-1", rollNumber: 145 };
    const loaded = await loadLegislativeVote({ query }, key);
    expect(query.mock.calls[0]?.[1]).toEqual(["US", "house", "119-1", 145]);
    expect(loaded).toEqual({
      id: "row-1",
      voteDate: "2025-05-22",
      measureId: "H R 1",
      exactQuestion: "On Passage",
      isFloorVote: true,
      yeas: 215,
      nays: 214,
      machineUrl: ROW.machineUrl,
      sourceSha256: "a".repeat(64),
      yeaDescription: "Voted to pass H.R. 1.",
      nayDescription: "Voted against passing H.R. 1.",
      labelsJson: [{ slug: "immigration", yea: "for" }],
      reviewStatus: "approved",
    });
    const columns = migrationTableColumns("legislative_votes");
    const selected = /SELECT ([\s\S]*?)FROM legislative_votes/.exec(query.mock.calls[0]?.[0] as string)?.[1] ?? "";
    const names = [...selected.matchAll(/\b([a-z_]+)(?:::text AS [a-z_]+)?,?\s*$/gm)].map((match) => match[1]!);
    expect(names.length).toBeGreaterThan(10);
    for (const name of names) {
      expect(columns.has(name), name).toBe(true);
    }

    query.mockResolvedValueOnce({ rows: [] });
    expect(await loadLegislativeVote({ query }, key)).toBeNull();
  });
});

describe("assertLegislativeVoteStillApproved", () => {
  const vote = {
    id: "row-1",
    voteDate: "2025-05-22",
    measureId: "H R 1",
    exactQuestion: "On Passage",
    isFloorVote: true,
    yeas: 215,
    nays: 214,
    machineUrl: ROW.machineUrl,
    sourceSha256: "a".repeat(64),
    yeaDescription: "Voted to pass H.R. 1.",
    nayDescription: "Voted against passing H.R. 1.",
    labelsJson: [{ slug: "immigration", yea: "for" }],
    reviewStatus: "approved" as const,
  };
  const current = {
    review_status: "approved",
    source_sha256: vote.sourceSha256,
    yea_description: vote.yeaDescription,
    nay_description: vote.nayDescription,
    labels_json: [{ slug: "immigration", yea: "for" }],
  };

  it("locks the row with FOR SHARE and passes when the approved judgment is unchanged", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [current] });
    await expect(assertLegislativeVoteStillApproved({ query }, vote)).resolves.toBeUndefined();
    expect(query.mock.calls[0]?.[0]).toMatch(/FOR SHARE/);
    expect(query.mock.calls[0]?.[1]).toEqual(["row-1"]);
  });

  it("fails when approval was withdrawn, the content was re-approved differently, or the row is gone", async () => {
    const pending = vi.fn().mockResolvedValue({ rows: [{ ...current, review_status: "pending" }] });
    await expect(assertLegislativeVoteStillApproved({ query: pending }, vote)).rejects.toThrow(/is pending now/);
    const reworded = vi.fn().mockResolvedValue({ rows: [{ ...current, nay_description: "Voted no." }] });
    await expect(assertLegislativeVoteStillApproved({ query: reworded }, vote)).rejects.toThrow(/different content/);
    const relabeled = vi.fn().mockResolvedValue({ rows: [{ ...current, labels_json: [{ slug: "immigration", yea: "against" }] }] });
    await expect(assertLegislativeVoteStillApproved({ query: relabeled }, vote)).rejects.toThrow(/different content/);
    const refetched = vi.fn().mockResolvedValue({ rows: [{ ...current, source_sha256: "b".repeat(64) }] });
    await expect(assertLegislativeVoteStillApproved({ query: refetched }, vote)).rejects.toThrow(/different content/);
    const gone = vi.fn().mockResolvedValue({ rows: [] });
    await expect(assertLegislativeVoteStillApproved({ query: gone }, vote)).rejects.toThrow(/no longer exists/);
  });
});
