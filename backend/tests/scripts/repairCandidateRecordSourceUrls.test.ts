import { describe, expect, it, vi } from "vitest";

import { buildCandidateRecordIdentityKey } from "../../src/pipeline/candidates/candidateRecordStore.js";
import {
  parseRepairsFile,
  repairOneSourceUrl,
  type RepairDeps,
} from "../../src/scripts/repairCandidateRecordSourceUrls.js";

const ROW = {
  id: "rec-1",
  candidate_id: "cand-1",
  description: "Appointed a new elections director in 2025.",
  source_url: "https://validate.perfdrive.com/?ssc=https%3A%2F%2Fwww.sos.mn.gov%2Fnews%2Fx",
  event_date: "2025-10-21",
};

function makeDeps(overrides: Partial<RepairDeps> = {}): RepairDeps {
  return {
    loadRecord: async () => ({ ...ROW }),
    checkReachable: async () => ({ ok: true }),
    findIdentityCollision: async () => null,
    applyRepair: async () => {},
    ...overrides,
  };
}

const GOOD_URL = "https://www.sos.mn.gov/news/x";

describe("repairOneSourceUrl", () => {
  it("rewrites the citation and recomputes the identity key", async () => {
    const applyRepair = vi.fn(async () => {});
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({ applyRepair }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "repaired", from: ROW.source_url, to: GOOD_URL });
    expect(applyRepair).toHaveBeenCalledWith({
      recordId: "rec-1",
      sourceUrl: GOOD_URL,
      identityKey: buildCandidateRecordIdentityKey({
        description: ROW.description,
        sourceUrl: GOOD_URL,
        eventDate: ROW.event_date,
      }),
    });
  });

  it("keys off the stored calendar date, not a timezone-shifted Date", async () => {
    // node-postgres parses a DATE column into a JS Date at LOCAL midnight, and
    // the identity key derives its date via toISOString() (UTC). On a host east
    // of UTC that round trip lands on the PREVIOUS day, so the key would encode
    // a date the row does not have — while this script leaves event_date
    // untouched. Reading event_date as ::text is what prevents it.
    const shiftedDate = new Date("2025-10-21T00:00:00+02:00");
    expect(shiftedDate.toISOString().slice(0, 10)).toBe("2025-10-20"); // the hazard, made explicit

    const keyFromStoredDate = buildCandidateRecordIdentityKey({
      description: ROW.description,
      sourceUrl: GOOD_URL,
      eventDate: ROW.event_date,
    });
    const keyFromShiftedDate = buildCandidateRecordIdentityKey({
      description: ROW.description,
      sourceUrl: GOOD_URL,
      eventDate: shiftedDate,
    });
    expect(keyFromShiftedDate).not.toBe(keyFromStoredDate); // and it changes the key

    const applyRepair = vi.fn(async () => {});
    await repairOneSourceUrl({ recordId: "rec-1", sourceUrl: GOOD_URL }, makeDeps({ applyRepair }), {
      apply: true,
    });
    expect(applyRepair).toHaveBeenCalledWith(
      expect.objectContaining({ identityKey: keyFromStoredDate })
    );
  });

  it("does not write in dry-run mode", async () => {
    const applyRepair = vi.fn(async () => {});
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({ applyRepair }),
      { apply: false }
    );

    expect(outcome.status).toBe("would_repair");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("refuses a replacement that is itself a blocked domain", async () => {
    const applyRepair = vi.fn(async () => {});
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: "https://www.civoren.com/candidate/x" },
      makeDeps({ applyRepair }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "skipped" });
    expect(outcome.status === "skipped" && outcome.reason).toContain("source policy");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("refuses an unreachable replacement", async () => {
    const applyRepair = vi.fn(async () => {});
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({
        applyRepair,
        checkReachable: async () => ({ ok: false, reason: "HTTP 404" }),
      }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("HTTP 404");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("refuses rather than colliding when the repaired identity already exists", async () => {
    const applyRepair = vi.fn(async () => {});
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({ applyRepair, findIdentityCollision: async () => "rec-duplicate" }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("rec-duplicate");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("skips a missing record and an already-repaired one", async () => {
    const missing = await repairOneSourceUrl(
      { recordId: "nope", sourceUrl: GOOD_URL },
      makeDeps({ loadRecord: async () => null }),
      { apply: true }
    );
    expect(missing.status === "skipped" && missing.reason).toContain("not found");

    const alreadyDone = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: ROW.source_url },
      makeDeps(),
      { apply: true }
    );
    expect(alreadyDone.status === "skipped" && alreadyDone.reason).toContain("already");
  });
});

describe("parseRepairsFile", () => {
  it("accepts a well-formed repair list", () => {
    const parsed = parseRepairsFile(
      JSON.stringify([
        {
          recordId: "  98e5cd52-7f13-463f-b8f6-aaf76d70d4d6  ",
          sourceUrl: " https://www.sos.mn.gov/news/x ",
          note: "decoded from ssc=",
        },
      ])
    );
    expect(parsed).toEqual([
      {
        recordId: "98e5cd52-7f13-463f-b8f6-aaf76d70d4d6",
        sourceUrl: "https://www.sos.mn.gov/news/x",
        note: "decoded from ssc=",
      },
    ]);
  });

  it("rejects a non-array payload", () => {
    expect(() => parseRepairsFile(JSON.stringify({ recordId: "a" }))).toThrow(/JSON array/);
  });

  it("rejects a missing or blank recordId", () => {
    expect(() =>
      parseRepairsFile(JSON.stringify([{ sourceUrl: "https://example.gov/a" }]))
    ).toThrow(/recordId/);
    expect(() =>
      parseRepairsFile(JSON.stringify([{ recordId: "   ", sourceUrl: "https://example.gov/a" }]))
    ).toThrow(/recordId/);
  });

  it("rejects a replacement that is not an http(s) URL", () => {
    // A bare hostname or a file path would otherwise reach the UPDATE and
    // store an uncitable value.
    expect(() => parseRepairsFile(JSON.stringify([{ recordId: "a", sourceUrl: "sos.mn.gov" }]))).toThrow(
      /http\(s\) URL/
    );
    expect(() =>
      parseRepairsFile(JSON.stringify([{ recordId: "a", sourceUrl: "/tmp/page.html" }]))
    ).toThrow(/http\(s\) URL/);
  });
});
