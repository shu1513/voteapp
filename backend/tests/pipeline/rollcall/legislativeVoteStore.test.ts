import { describe, expect, it, vi } from "vitest";

import {
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
    });
    const [sql, params] = db.query.mock.calls[1] as [string, unknown[]];
    expect(sql).toMatch(/SET fetched_at = \$2::timestamptz,\s+importer_version = \$3\s+WHERE id = \$1/);
    expect(sql).not.toMatch(/source_sha256/);
    expect(params).toEqual(["row-1", "2026-08-22T12:00:00.000Z", "rollcall-fetch-v1"]);
  });

  it("rewrites the source columns when the feed changed", async () => {
    const db = fakeDb([{ ...EXISTING, source_sha256: "b".repeat(64), yeas: 216 }]);
    await expect(upsertLegislativeVoteSource(db, ROW)).resolves.toMatchObject({ outcome: "updated", id: "row-1" });
    const [sql, params] = db.query.mock.calls[1] as [string, unknown[]];
    expect(sql).toMatch(/UPDATE legislative_votes/);
    expect(sql).toMatch(/source_sha256 = \$12/);
    expect(sql).not.toMatch(/review_status|yea_description|nay_description|labels_json/);
    expect(params[0]).toBe("row-1");
    expect(params[11]).toBe(ROW.sourceSha256);
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
    const db = fakeDb([{ ...EXISTING, source_sha256: "b".repeat(64) }]);
    await upsertLegislativeVoteSource(db, ROW);
    for (const [sql] of db.query.mock.calls as [string][]) {
      for (const match of sql.matchAll(/\b([a-z_]+) = \$\d+/g)) {
        expect(columns.has(match[1]!), match[1]).toBe(true);
      }
      const insertColumns = /INSERT INTO legislative_votes \(([\s\S]*?)\) VALUES/.exec(sql)?.[1];
      for (const column of insertColumns?.split(",").map((name) => name.trim()) ?? []) {
        expect(columns.has(column), column).toBe(true);
      }
    }
  });
});
