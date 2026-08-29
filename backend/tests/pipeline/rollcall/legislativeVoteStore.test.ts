import { describe, expect, it, vi } from "vitest";

import {
  applyLegislativeVoteJudgment,
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
          official_vote_date: null,
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
      officialVoteDate: null,
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
    officialVoteDate: null,
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
    official_vote_date: null,
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
    const redated = vi.fn().mockResolvedValue({ rows: [{ ...current, official_vote_date: "2025-05-23" }] });
    await expect(assertLegislativeVoteStillApproved({ query: redated }, vote)).rejects.toThrow(/different content/);
    const gone = vi.fn().mockResolvedValue({ rows: [] });
    await expect(assertLegislativeVoteStillApproved({ query: gone }, vote)).rejects.toThrow(/no longer exists/);
  });
});

describe("applyLegislativeVoteJudgment", () => {
  const judgment = {
    jurisdiction: "US",
    chamber: "house" as const,
    session: "119-1",
    rollNumber: 145,
    measureId: "H.R. 1",
    voteDate: "2025-05-22",
    officialVoteDate: null,
    yeaDescription: "Voted to pass H.R. 1. It passed the House 215-214.",
    nayDescription: "Voted against passing H.R. 1. It passed the House 215-214.",
    labels: [{ slug: "immigration", yea: "for" as const, nay: null }],
    acknowledgeLaterRolls: [] as number[],
    reviewStatus: "approved" as const,
  };
  const stored = {
    id: "row-1",
    is_floor_vote: true,
    // The Clerk's spelling; the judgment's `H.R. 1` must still match.
    measure_id: "H R 1",
    vote_date: "2025-05-22",
    official_vote_date: null,
    review_status: "pending",
    yeas: 215,
    nays: 214,
    yea_description: null,
    nay_description: null,
    labels_json: null,
  };

  function db(row: Record<string, unknown> | null) {
    // The catch-all answers the approval gates' later-stage SELECT (no
    // later floor votes) and the UPDATEs alike.
    return { query: vi.fn().mockResolvedValueOnce({ rows: row ? [row] : [] }).mockResolvedValue({ rows: [], rowCount: 1 }) };
  }

  it("locks the row, writes the judgment, and stamps reviewed_at only when approving", async () => {
    const approved = db(stored);
    await expect(applyLegislativeVoteJudgment(approved, judgment)).resolves.toBe("updated");
    // select FOR UPDATE, the later-stage gate's select, the write.
    expect(approved.query).toHaveBeenCalledTimes(3);
    expect(approved.query.mock.calls[0]?.[0]).toMatch(/FOR UPDATE/);
    expect(approved.query.mock.calls[0]?.[1]).toEqual(["US", "house", "119-1", 145]);
    expect(approved.query.mock.calls[1]?.[0]).toMatch(/is_floor_vote = true/);
    expect(approved.query.mock.calls[1]?.[1]).toEqual(["US", "house", "2025-05-22", "row-1"]);
    const [sql, params] = approved.query.mock.calls[2]!;
    expect(sql).toMatch(/reviewed_at = CASE WHEN \$6 = 'approved' THEN now\(\) ELSE NULL END/);
    expect(params).toEqual([
      "row-1",
      judgment.yeaDescription,
      judgment.nayDescription,
      JSON.stringify(judgment.labels),
      null,
      "approved",
    ]);

    const pending = db(stored);
    await expect(applyLegislativeVoteJudgment(pending, { ...judgment, reviewStatus: "pending" })).resolves.toBe("updated");
    expect(pending.query).toHaveBeenCalledTimes(2);
    expect(pending.query.mock.calls[1]?.[1]?.[5]).toBe("pending");
  });

  it("writes the official_vote_date override alongside the judgment", async () => {
    const overridden = db(stored);
    await expect(applyLegislativeVoteJudgment(overridden, { ...judgment, officialVoteDate: "2025-05-23" })).resolves.toBe(
      "updated"
    );
    expect(overridden.query.mock.calls[2]?.[0]).toMatch(/official_vote_date = \$5::date/);
    expect(overridden.query.mock.calls[2]?.[1]?.[4]).toBe("2025-05-23");
  });

  it("refuses approval when a description does not cite this roll call's tally", async () => {
    await expect(
      applyLegislativeVoteJudgment(db(stored), {
        ...judgment,
        // Written about a different stage: PA HB 103's first-passage prose
        // cited 148-55 while the approved roll call was another vote.
        nayDescription: "Voted against passing H.R. 1. It passed the House 148-55.",
      })
    ).rejects.toThrow(/nay_description does not cite this roll call's tally 215-214/);
    // A pending judgment is not gated; the tally check runs on approval only.
    await expect(
      applyLegislativeVoteJudgment(db(stored), {
        ...judgment,
        nayDescription: "Voted against passing H.R. 1. It passed the House 148-55.",
        reviewStatus: "pending",
      })
    ).resolves.toBe("updated");
  });

  it("refuses to approve a stage superseded by a later kept floor vote unless acknowledged", async () => {
    const laterVote = {
      roll_number: 320,
      vote_date: "2025-06-30",
      measure_id: "H R 1",
      exact_question: "On Motion to Concur in the Senate Amendment",
    };
    function dbWithLater() {
      return {
        query: vi
          .fn()
          .mockResolvedValueOnce({ rows: [stored] })
          .mockResolvedValueOnce({ rows: [laterVote] })
          .mockResolvedValue({ rows: [], rowCount: 1 }),
      };
    }
    await expect(applyLegislativeVoteJudgment(dbWithLater(), judgment)).rejects.toThrow(
      /not this chamber's last kept floor vote on H R 1: roll 320 on 2025-06-30 \(On Motion to Concur in the Senate Amendment\)/
    );
    // Acknowledging the exact later roll approves the earlier stage on purpose.
    await expect(
      applyLegislativeVoteJudgment(dbWithLater(), { ...judgment, acknowledgeLaterRolls: [320] })
    ).resolves.toBe("updated");
    // A later vote on ANOTHER measure never blocks.
    const otherMeasure = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [stored] })
        .mockResolvedValueOnce({ rows: [{ ...laterVote, measure_id: "H R 2" }] })
        .mockResolvedValue({ rows: [], rowCount: 1 }),
    };
    await expect(applyLegislativeVoteJudgment(otherMeasure, judgment)).resolves.toBe("updated");
  });

  it("is a no-op when the row already holds the same judgment and status", async () => {
    const same = db({
      ...stored,
      review_status: "approved",
      yea_description: judgment.yeaDescription,
      nay_description: judgment.nayDescription,
      // jsonb hands keys back in its own order.
      labels_json: [{ yea: "for", slug: "immigration" }],
    });
    await expect(applyLegislativeVoteJudgment(same, judgment)).resolves.toBe("unchanged");
    expect(same.query).toHaveBeenCalledTimes(1);
  });

  it("moves an approved row back to pending before writing a different judgment", async () => {
    const reworded = db({
      ...stored,
      review_status: "approved",
      yea_description: "Voted yes.",
      nay_description: judgment.nayDescription,
      labels_json: [{ slug: "immigration", yea: "for" }],
    });
    await expect(applyLegislativeVoteJudgment(reworded, judgment)).resolves.toBe("updated");
    // select, the later-stage gate's select, pending-first, the write.
    expect(reworded.query).toHaveBeenCalledTimes(4);
    expect(reworded.query.mock.calls[2]?.[0]).toMatch(/SET review_status = 'pending',\s+reviewed_at = NULL/);
    expect(reworded.query.mock.calls[3]?.[1]?.[5]).toBe("approved");

    // A changed override alone is a judgment change too: same pending-first
    // path, since the freeze trigger covers official_vote_date.
    const redated = db({
      ...stored,
      review_status: "approved",
      yea_description: judgment.yeaDescription,
      nay_description: judgment.nayDescription,
      labels_json: [{ slug: "immigration", yea: "for" }],
    });
    await expect(applyLegislativeVoteJudgment(redated, { ...judgment, officialVoteDate: "2025-05-23" })).resolves.toBe(
      "updated"
    );
    expect(redated.query).toHaveBeenCalledTimes(4);
    expect(redated.query.mock.calls[2]?.[0]).toMatch(/SET review_status = 'pending',\s+reviewed_at = NULL/);
    expect(redated.query.mock.calls[3]?.[1]?.[4]).toBe("2025-05-23");
  });

  it("refuses a judgment written about a different measure or date than the row holds", async () => {
    await expect(applyLegislativeVoteJudgment(db(stored), { ...judgment, measureId: "H.R. 2" })).rejects.toThrow(
      /house 119-1 roll 145 is H R 1 on 2025-05-22, but the judgment says H\.R\. 2 on 2025-05-22/
    );
    await expect(applyLegislativeVoteJudgment(db(stored), { ...judgment, voteDate: "2025-07-03" })).rejects.toThrow(
      /but the judgment says H\.R\. 1 on 2025-07-03/
    );
    await expect(applyLegislativeVoteJudgment(db({ ...stored, measure_id: null }), judgment)).rejects.toThrow(
      /is no measure on 2025-05-22, but the judgment says H\.R\. 1/
    );
    await expect(
      applyLegislativeVoteJudgment(db({ ...stored, measure_id: null, is_floor_vote: false }), {
        ...judgment,
        measureId: null,
        reviewStatus: "pending",
      })
    ).resolves.toBe("updated");
  });

  it("refuses to move an approved row back to pending once records were fanned out", async () => {
    const approved = {
      ...stored,
      review_status: "approved",
      yea_description: judgment.yeaDescription,
      nay_description: judgment.nayDescription,
      labels_json: [{ yea: "for", slug: "immigration" }],
    };
    const fannedOut = {
      query: vi.fn().mockResolvedValueOnce({ rows: [approved] }).mockResolvedValueOnce({ rows: [{ n: "89" }] }),
    };
    await expect(applyLegislativeVoteJudgment(fannedOut, { ...judgment, reviewStatus: "pending" })).rejects.toThrow(
      /already fanned out 89 live candidate records; setting it back to pending would not withdraw them/
    );
    expect(fannedOut.query.mock.calls[1]?.[1]).toEqual(["rollcall:US:house:119-1:145:"]);

    const nothingYet = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [approved] })
        .mockResolvedValueOnce({ rows: [{ n: "0" }] })
        .mockResolvedValue({ rowCount: 1 }),
    };
    await expect(applyLegislativeVoteJudgment(nothingYet, { ...judgment, reviewStatus: "pending" })).resolves.toBe("updated");
    expect(nothingYet.query.mock.calls[2]?.[1]?.[5]).toBe("pending");
  });

  it("refuses a missing row and refuses to approve a non-floor vote", async () => {
    await expect(applyLegislativeVoteJudgment(db(null), judgment)).rejects.toThrow(
      /house 119-1 roll 145 is not in legislative_votes; run rollcall:fetch first/
    );
    await expect(applyLegislativeVoteJudgment(db({ ...stored, is_floor_vote: false }), judgment)).rejects.toThrow(
      /not a kept floor vote \(is_floor_vote = false\); it cannot be approved/
    );
    // Storing a pending judgment on an excluded vote is fine.
    await expect(
      applyLegislativeVoteJudgment(db({ ...stored, is_floor_vote: null }), { ...judgment, reviewStatus: "pending" })
    ).resolves.toBe("updated");
  });
});
