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
  retired_at: null,
};

function makeDeps(overrides: Partial<RepairDeps> = {}): RepairDeps {
  return {
    loadRecord: async () => ({ ...ROW }),
    checkReachable: async (sourceUrl) => ({ ok: true, finalUrl: sourceUrl }),
    findIdentityCollision: async () => null,
    applyRepair: async () => 1,
    ...overrides,
  };
}

const GOOD_URL = "https://www.sos.mn.gov/news/x";

describe("repairOneSourceUrl", () => {
  it("rewrites the citation and recomputes the identity key", async () => {
    const applyRepair = vi.fn(async () => 1);
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
      // Compare-and-swap guard: the update must be conditional on exactly the
      // columns the identity key was derived from.
      expected: {
        description: ROW.description,
        eventDate: ROW.event_date,
        sourceUrl: ROW.source_url,
      },
    });
  });

  it("refuses a replacement that REDIRECTS to a blocked domain", async () => {
    // A shortener or open redirect passes the pre-fetch policy check on its
    // own hostname and then lands wherever it likes. Without judging the
    // post-redirect URL, the repair tool becomes a laundering route for the
    // very sources it exists to remove.
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: "https://short.example/article" },
      makeDeps({
        applyRepair,
        checkReachable: async () => ({
          ok: true,
          finalUrl: "https://www.civoren.com/candidate/some-person",
        }),
      }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "skipped" });
    expect(outcome.status === "skipped" && outcome.reason).toContain("redirects to");
    expect(outcome.status === "skipped" && outcome.reason).toContain(
      "auto-generated candidate directory"
    );
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("stores the post-redirect URL, not the submitted one", async () => {
    const applyRepair = vi.fn(async () => 1);
    const resolved = "https://www.sos.mn.gov/news/canonical-path";
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: "https://sos.mn.gov/news/x?utm_source=email" },
      makeDeps({ applyRepair, checkReachable: async () => ({ ok: true, finalUrl: resolved }) }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "repaired", to: resolved });
    expect(applyRepair).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceUrl: resolved,
        identityKey: buildCandidateRecordIdentityKey({
          description: ROW.description,
          sourceUrl: resolved,
          eventDate: ROW.event_date,
        }),
      })
    );
  });

  it("skips when the replacement resolves back to the stored URL", async () => {
    // An operator supplies a URL that merely canonicalizes to what is already
    // stored (non-www to www, a tracking param stripped). Without this branch
    // the compare-and-swap would match, rewrite the row to identical values,
    // and report "repaired" — a change that never happened.
    const applyRepair = vi.fn(async () => 1);
    const stored = "https://www.sos.mn.gov/news/canonical";
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: "https://sos.mn.gov/news/canonical" },
      makeDeps({
        applyRepair,
        loadRecord: async () => ({ ...ROW, source_url: stored }),
        checkReachable: async () => ({ ok: true, finalUrl: stored }),
      }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("nothing to repair");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("lets the source policy win when a redirect lands on a stored BLOCKED url", async () => {
    // Same shape as the case above, but the stored URL is itself blocked (the
    // perfdrive interstitial). Order matters here: the resolved-URL policy
    // check runs BEFORE the resolves-to-stored check, so the operator is told
    // the replacement lands on a blocked host rather than the far less useful
    // "nothing to repair". Pinning this down because the two branches are
    // easy to reorder and the weaker message would silently win.
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: "https://sos.mn.gov/news/x" },
      makeDeps({ applyRepair, checkReachable: async () => ({ ok: true, finalUrl: ROW.source_url }) }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("redirects to");
    expect(outcome.status === "skipped" && outcome.reason).toContain("bot-check interstitial");
    expect(outcome.status === "skipped" && outcome.reason).not.toContain("nothing to repair");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("reports a no-op instead of a repair when the update matched no row", async () => {
    // The compare-and-swap guard failing means another writer changed the
    // record between the read and the update. Nothing was modified, and
    // claiming "repaired" would be a false report.
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({ applyRepair: async () => 0 }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "skipped" });
    expect(outcome.status === "skipped" && outcome.reason).toContain("concurrent write");
  });

  it("carries the operator's note into the outcome", async () => {
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL, note: "decoded from ssc=" },
      makeDeps(),
      { apply: true }
    );
    expect(outcome).toMatchObject({ status: "repaired", note: "decoded from ssc=" });
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

    const applyRepair = vi.fn(async () => 1);
    await repairOneSourceUrl({ recordId: "rec-1", sourceUrl: GOOD_URL }, makeDeps({ applyRepair }), {
      apply: true,
    });
    expect(applyRepair).toHaveBeenCalledWith(
      expect.objectContaining({ identityKey: keyFromStoredDate })
    );
  });

  it("does not write in dry-run mode", async () => {
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({ applyRepair }),
      { apply: false }
    );

    expect(outcome.status).toBe("would_repair");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("refuses a replacement that is itself a blocked domain", async () => {
    const applyRepair = vi.fn(async () => 1);
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
    const applyRepair = vi.fn(async () => 1);
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
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({ applyRepair, findIdentityCollision: async () => "rec-duplicate" }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("rec-duplicate");
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("skips a retired record — repairing a withdrawn claim is moot", async () => {
    const applyRepair = vi.fn(async () => 1);
    const outcome = await repairOneSourceUrl(
      { recordId: "rec-1", sourceUrl: GOOD_URL },
      makeDeps({
        applyRepair,
        loadRecord: async () => ({ ...ROW, retired_at: "2026-07-31 00:00:00+00" }),
      }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("retired");
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
