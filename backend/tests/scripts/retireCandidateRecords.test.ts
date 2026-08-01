import { describe, expect, it, vi } from "vitest";

import {
  parseRetirementsFile,
  retireOneRecord,
  unretireOneRecord,
  type RetireDeps,
} from "../../src/scripts/retireCandidateRecords.js";

const ROW = {
  id: "rec-1",
  description: "Endorsed by the district's firefighters association.",
  source_url: "https://aggregator.example/candidate/some-person",
  event_date: "2026-04-02",
  retired_at: null,
};

const REASON = "Wrong attribution: the endorsement was for the same-named 2022 candidate.";

function makeDeps(overrides: Partial<RetireDeps> = {}): RetireDeps {
  return {
    loadRecord: async () => ({ ...ROW }),
    applyRetirement: async () => 1,
    applyUnretirement: async () => 1,
    ...overrides,
  };
}

const RETIRED_ROW = { ...ROW, retired_at: "2026-08-01 03:00:00-07" };
const UNRETIRE_REASON = "Materiality call reversed: the recusal block shows a pattern voters should see.";

describe("unretireOneRecord", () => {
  it("dry-runs by default and reports would_unretire", async () => {
    const applyUnretirement = vi.fn(async () => 1);
    const outcome = await unretireOneRecord(
      { recordId: "rec-1", reason: UNRETIRE_REASON },
      makeDeps({ loadRecord: async () => ({ ...RETIRED_ROW }), applyUnretirement }),
      { apply: false }
    );
    expect(outcome.status).toBe("would_unretire");
    expect(applyUnretirement).not.toHaveBeenCalled();
  });

  it("clears the retirement under the same content compare-and-swap", async () => {
    const applyUnretirement = vi.fn(async () => 1);
    const outcome = await unretireOneRecord(
      { recordId: "rec-1", reason: UNRETIRE_REASON },
      makeDeps({ loadRecord: async () => ({ ...RETIRED_ROW }), applyUnretirement }),
      { apply: true }
    );
    expect(outcome.status).toBe("unretired");
    expect(applyUnretirement).toHaveBeenCalledWith({
      recordId: "rec-1",
      expected: {
        description: RETIRED_ROW.description,
        eventDate: RETIRED_ROW.event_date,
        sourceUrl: RETIRED_ROW.source_url,
      },
    });
  });

  it("warns — without blocking — when the restored content would fail the current quality gate", async () => {
    // Unretirement is the operator's overrule of the detector, so the
    // detector must never gate it; but restoring a row the write-time gate
    // would reject deserves a visible advisory in the report.
    const candidacyRow = {
      ...RETIRED_ROW,
      description: "Michele Clark filed as the Democratic candidate for Illinois Senate District 33.",
    };
    const outcome = await unretireOneRecord(
      { recordId: "rec-1", reason: UNRETIRE_REASON },
      makeDeps({ loadRecord: async () => ({ ...candidacyRow }) }),
      { apply: true }
    );
    expect(outcome.status).toBe("unretired");
    if (outcome.status === "unretired") {
      expect(outcome.qualityWarning).toMatch(/quality gate/);
    }

    // A clean row carries no warning.
    const clean = await unretireOneRecord(
      { recordId: "rec-1", reason: UNRETIRE_REASON },
      makeDeps({ loadRecord: async () => ({ ...RETIRED_ROW }) }),
      { apply: true }
    );
    expect(clean.status).toBe("unretired");
    if (clean.status === "unretired") {
      expect(clean.qualityWarning).toBeUndefined();
    }
  });

  it("skips rows that are not retired", async () => {
    const applyUnretirement = vi.fn(async () => 1);
    const outcome = await unretireOneRecord(
      { recordId: "rec-1", reason: UNRETIRE_REASON },
      makeDeps({ applyUnretirement }),
      { apply: true }
    );
    expect(outcome).toEqual({
      recordId: "rec-1",
      status: "skipped",
      reason: "record is not retired",
    });
    expect(applyUnretirement).not.toHaveBeenCalled();
  });

  it("skips when the compare-and-swap loses a concurrent write", async () => {
    const outcome = await unretireOneRecord(
      { recordId: "rec-1", reason: UNRETIRE_REASON },
      makeDeps({ loadRecord: async () => ({ ...RETIRED_ROW }), applyUnretirement: async () => 0 }),
      { apply: true }
    );
    expect(outcome.status).toBe("skipped");
  });
});

describe("retireOneRecord", () => {
  it("retires the record with the operator's reason under a compare-and-swap guard", async () => {
    const applyRetirement = vi.fn(async () => 1);
    const outcome = await retireOneRecord(
      { recordId: "rec-1", reason: REASON },
      makeDeps({ applyRetirement }),
      { apply: true }
    );

    expect(outcome).toMatchObject({
      status: "retired",
      description: ROW.description,
      reason: REASON,
    });
    // The retire decision was made about the content the operator reviewed;
    // the update must be conditional on exactly that content.
    expect(applyRetirement).toHaveBeenCalledWith({
      recordId: "rec-1",
      reason: REASON,
      expected: {
        description: ROW.description,
        eventDate: ROW.event_date,
        sourceUrl: ROW.source_url,
      },
    });
  });

  it("skips an already-retired record instead of re-stamping it", async () => {
    const applyRetirement = vi.fn(async () => 1);
    const outcome = await retireOneRecord(
      { recordId: "rec-1", reason: REASON },
      makeDeps({
        applyRetirement,
        loadRecord: async () => ({ ...ROW, retired_at: "2026-07-30 00:00:00+00" }),
      }),
      { apply: true }
    );

    expect(outcome.status === "skipped" && outcome.reason).toContain("already retired");
    expect(applyRetirement).not.toHaveBeenCalled();
  });

  it("reports a no-op when the update matched no row (concurrent write)", async () => {
    // Retiring content nobody reviewed would withdraw the WRONG claim; the
    // compare-and-swap failing must skip, not report success.
    const outcome = await retireOneRecord(
      { recordId: "rec-1", reason: REASON },
      makeDeps({ applyRetirement: async () => 0 }),
      { apply: true }
    );

    expect(outcome).toMatchObject({ status: "skipped" });
    expect(outcome.status === "skipped" && outcome.reason).toContain("concurrent write");
  });

  it("does not write in dry-run mode and previews the claim being withdrawn", async () => {
    const applyRetirement = vi.fn(async () => 1);
    const outcome = await retireOneRecord(
      { recordId: "rec-1", reason: REASON },
      makeDeps({ applyRetirement }),
      { apply: false }
    );

    // The description in the outcome is what the operator checks against
    // their review before re-running with --apply.
    expect(outcome).toMatchObject({ status: "would_retire", description: ROW.description });
    expect(applyRetirement).not.toHaveBeenCalled();
  });

  it("skips a missing record", async () => {
    const outcome = await retireOneRecord(
      { recordId: "nope", reason: REASON },
      makeDeps({ loadRecord: async () => null }),
      { apply: true }
    );
    expect(outcome.status === "skipped" && outcome.reason).toContain("not found");
  });

  it("carries the operator's note into the outcome", async () => {
    const outcome = await retireOneRecord(
      { recordId: "rec-1", reason: REASON, note: "see wave-20 report section BB" },
      makeDeps(),
      { apply: true }
    );
    expect(outcome).toMatchObject({ status: "retired", note: "see wave-20 report section BB" });
  });
});

describe("parseRetirementsFile", () => {
  it("accepts a well-formed retirement list", () => {
    const parsed = parseRetirementsFile(
      JSON.stringify([
        {
          recordId: "  98e5cd52-7f13-463f-b8f6-aaf76d70d4d6  ",
          reason: `  ${REASON}  `,
          note: "wave-20 audit",
        },
      ])
    );
    expect(parsed).toEqual([
      {
        recordId: "98e5cd52-7f13-463f-b8f6-aaf76d70d4d6",
        reason: REASON,
        note: "wave-20 audit",
      },
    ]);
  });

  it("rejects a non-array payload", () => {
    expect(() => parseRetirementsFile(JSON.stringify({ recordId: "a" }))).toThrow(/JSON array/);
  });

  it("rejects a missing or blank recordId", () => {
    expect(() => parseRetirementsFile(JSON.stringify([{ reason: REASON }]))).toThrow(/recordId/);
  });

  it("rejects a missing or placeholder reason", () => {
    // retired_reason is what makes the withdrawal reviewable later; "bad" is
    // not a reviewable reason.
    expect(() => parseRetirementsFile(JSON.stringify([{ recordId: "a" }]))).toThrow(/reason/);
    expect(() => parseRetirementsFile(JSON.stringify([{ recordId: "a", reason: "bad" }]))).toThrow(
      /reason/
    );
  });
});
