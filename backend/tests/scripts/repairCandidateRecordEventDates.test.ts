import { describe, expect, it, vi } from "vitest";

import { buildCandidateRecordIdentityKey } from "../../src/pipeline/candidates/candidateRecordStore.js";
import {
  parseRepairsFile,
  repairOneEventDate,
  type RepairDeps,
} from "../../src/scripts/repairCandidateRecordEventDates.js";

// The wave-18 defect class this script exists for: the row carries the date
// the RESEARCH happened (2026-05-26) instead of the date the action did
// (2026-03-10, the page's real publication date).
const ROW = {
  id: "rec-1",
  candidate_id: "cand-1",
  description: "Organized a women's history month event with local officials.",
  source_url: "https://www.medaforalaska.com/womens-history-month",
  event_date: "2026-05-26",
  retired_at: null,
};

const CORRECT_DATE = "2026-03-10";

function makeDeps(overrides: Partial<RepairDeps> = {}): RepairDeps {
  return {
    loadRecord: async () => ({ ...ROW }),
    findIdentityCollision: async () => null,
    applyRepair: async () => 1,
    ...overrides,
  };
}

describe("repairOneEventDate", () => {
  it("rewrites the date and recomputes the identity key", async () => {
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneEventDate(
      { recordId: "rec-1", eventDate: CORRECT_DATE },
      makeDeps({ applyRepair }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "repaired", from: ROW.event_date, to: CORRECT_DATE });
    expect(applyRepair).toHaveBeenCalledWith({
      recordId: "rec-1",
      eventDate: CORRECT_DATE,
      identityKey: buildCandidateRecordIdentityKey({
        description: ROW.description,
        sourceUrl: ROW.source_url,
        eventDate: CORRECT_DATE,
      }),
      // Compare-and-swap guard: the update must be conditional on exactly the
      // columns the identity key was derived from.
      expected: {
        description: ROW.description,
        eventDate: ROW.event_date,
        sourceUrl: ROW.source_url,
      },
    });
  });

  it("refuses rather than colliding when the repaired identity already exists", async () => {
    // The correctly-dated record is already stored on another row — this row
    // is a duplicate, and removing a duplicate is a retire decision.
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneEventDate(
      { recordId: "rec-1", eventDate: CORRECT_DATE },
      makeDeps({ applyRepair, findIdentityCollision: async () => "rec-duplicate" }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("rec-duplicate");
    expect(outcome.status === "skipped" && outcome.reason).toContain("retire");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("skips a retired record — repairing a withdrawn claim is moot", async () => {
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneEventDate(
      { recordId: "rec-1", eventDate: CORRECT_DATE },
      makeDeps({
        applyRepair,
        loadRecord: async () => ({ ...ROW, retired_at: "2026-07-31 00:00:00+00" }),
      }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("retired");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("reports a no-op instead of a repair when the update matched no row", async () => {
    const outcome = await repairOneEventDate(
      { recordId: "rec-1", eventDate: CORRECT_DATE },
      makeDeps({ applyRepair: async () => 0 }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "skipped" });
    expect(outcome.status === "skipped" && outcome.reason).toContain("concurrent write");
  });

  it("does not write in dry-run mode", async () => {
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneEventDate(
      { recordId: "rec-1", eventDate: CORRECT_DATE },
      makeDeps({ applyRepair }),
      { apply: false }
    );

    expect(outcome.status).toBe("would_repair");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("skips a missing record and an already-repaired one", async () => {
    const missing = await repairOneEventDate(
      { recordId: "nope", eventDate: CORRECT_DATE },
      makeDeps({ loadRecord: async () => null }),
      { apply: true }
    );
    expect(missing.status === "skipped" && missing.reason).toContain("not found");

    const alreadyDone = await repairOneEventDate(
      { recordId: "rec-1", eventDate: ROW.event_date },
      makeDeps(),
      { apply: true }
    );
    expect(alreadyDone.status === "skipped" && alreadyDone.reason).toContain("already");
  });

  it("carries the operator's note into the outcome", async () => {
    const outcome = await repairOneEventDate(
      { recordId: "rec-1", eventDate: CORRECT_DATE, note: "WordPress publication date" },
      makeDeps(),
      { apply: true }
    );
    expect(outcome).toMatchObject({ status: "repaired", note: "WordPress publication date" });
  });
});

describe("parseRepairsFile", () => {
  it("accepts a well-formed repair list", () => {
    const parsed = parseRepairsFile(
      JSON.stringify([
        {
          recordId: "  98e5cd52-7f13-463f-b8f6-aaf76d70d4d6  ",
          eventDate: "2026-03-10",
          note: "WordPress publication date",
        },
      ])
    );
    expect(parsed).toEqual([
      {
        recordId: "98e5cd52-7f13-463f-b8f6-aaf76d70d4d6",
        eventDate: "2026-03-10",
        note: "WordPress publication date",
      },
    ]);
  });

  it("rejects a non-array payload", () => {
    expect(() => parseRepairsFile(JSON.stringify({ recordId: "a" }))).toThrow(/JSON array/);
  });

  it("rejects a missing or blank recordId", () => {
    expect(() => parseRepairsFile(JSON.stringify([{ eventDate: "2026-03-10" }]))).toThrow(
      /recordId/
    );
  });

  it("enforces the shared event-date rules — no partial dates, no future dates", () => {
    // recordEventDate.ts is shared with the discovery and source-repair
    // contracts precisely so a repair path cannot become an escape hatch for
    // dates the discovery parser rejects.
    expect(() =>
      parseRepairsFile(JSON.stringify([{ recordId: "a", eventDate: "2026-03" }]))
    ).toThrow(/incomplete/);
    expect(() =>
      parseRepairsFile(JSON.stringify([{ recordId: "a", eventDate: "2099-01-01" }]))
    ).toThrow(/future/);
    expect(() =>
      parseRepairsFile(JSON.stringify([{ recordId: "a", eventDate: "2026-02-30" }]))
    ).toThrow(/not a real calendar date/);
    expect(() => parseRepairsFile(JSON.stringify([{ recordId: "a" }]))).toThrow(/event_date/);
  });
});
