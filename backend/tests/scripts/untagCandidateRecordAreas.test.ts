import { describe, expect, it, vi } from "vitest";

import {
  parseUntagsFile,
  untagOneRecordArea,
  type UntagDeps,
} from "../../src/scripts/untagCandidateRecordAreas.js";

const TAG = {
  tag_id: "tag-1",
  stance: "against",
  description: "Voted against a $26,349,041 sheriff security services contract.",
};

const INPUT = {
  recordId: "rec-1",
  researchAreaSlug: "public_safety_and_crime_control",
  expectedStance: "against" as const,
  expectedDescription: TAG.description,
  reason: "One contract vote does not support a general anti-public-safety stance.",
};

function makeDeps(overrides: Partial<UntagDeps> = {}): UntagDeps {
  return {
    loadTag: async () => ({ ...TAG }),
    applyUntag: async () => 1,
    ...overrides,
  };
}

describe("parseUntagsFile", () => {
  it("parses valid entries and trims fields", () => {
    const parsed = parseUntagsFile(
      JSON.stringify([{ ...INPUT, recordId: " rec-1 ", researchAreaSlug: " slug " }])
    );
    expect(parsed).toEqual([{ ...INPUT, recordId: "rec-1", researchAreaSlug: "slug" }]);
  });

  it("rejects a missing slug and a placeholder reason", () => {
    expect(() => parseUntagsFile(JSON.stringify([{ recordId: "rec-1", reason: INPUT.reason }]))).toThrow(
      /researchAreaSlug/
    );
    expect(() =>
      parseUntagsFile(JSON.stringify([{ ...INPUT, reason: "bad tag" }]))
    ).toThrow(/reason/);
  });

  it("requires the reviewed stance and description so apply cannot judge from today's content", () => {
    expect(() =>
      parseUntagsFile(JSON.stringify([{ ...INPUT, expectedStance: "neutral" }]))
    ).toThrow(/expectedStance/);
    expect(() => {
      const { expectedStance: _dropped, ...rest } = INPUT;
      return parseUntagsFile(JSON.stringify([rest]));
    }).toThrow(/expectedStance/);
    expect(() =>
      parseUntagsFile(JSON.stringify([{ ...INPUT, expectedDescription: "" }]))
    ).toThrow(/expectedDescription/);
    // null is a legitimate reviewed stance (null-stance tags).
    const parsed = parseUntagsFile(JSON.stringify([{ ...INPUT, expectedStance: null }]));
    expect(parsed[0]!.expectedStance).toBeNull();
  });

  it("rejects a non-array file", () => {
    expect(() => parseUntagsFile(JSON.stringify({}))).toThrow(/JSON array/);
  });
});

describe("untagOneRecordArea", () => {
  it("dry-runs by default: reports would_untag with the reviewed stance, deletes nothing", async () => {
    const applyUntag = vi.fn(async () => 1);
    const outcome = await untagOneRecordArea(INPUT, makeDeps({ applyUntag }), { apply: false });
    expect(outcome).toMatchObject({ status: "would_untag", stance: "against" });
    expect(applyUntag).not.toHaveBeenCalled();
  });

  it("deletes under a compare-and-swap pinned to the REVIEWED stance and description", async () => {
    const applyUntag = vi.fn(async () => 1);
    const outcome = await untagOneRecordArea(INPUT, makeDeps({ applyUntag }), { apply: true });
    expect(outcome.status).toBe("untagged");
    expect(applyUntag).toHaveBeenCalledWith({
      tagId: "tag-1",
      expected: { stance: INPUT.expectedStance, description: INPUT.expectedDescription },
    });
  });

  it("skips — in dry-run too — when the row no longer matches what the operator reviewed", async () => {
    // The record was rewritten after review: the tag may now be fair, so the
    // decision must be remade rather than applied to content nobody judged.
    const applyUntag = vi.fn(async () => 1);
    const drifted = makeDeps({
      loadTag: async () => ({ ...TAG, description: "Voted no on the contract and said deputies should not police campuses." }),
      applyUntag,
    });
    const dryRun = await untagOneRecordArea(INPUT, drifted, { apply: false });
    expect(dryRun).toMatchObject({ status: "skipped", reason: expect.stringMatching(/changed since review/) });
    const apply = await untagOneRecordArea(INPUT, drifted, { apply: true });
    expect(apply.status).toBe("skipped");
    expect(applyUntag).not.toHaveBeenCalled();

    const stanceDrift = await untagOneRecordArea(
      { ...INPUT, expectedStance: "for" },
      makeDeps({ applyUntag }),
      { apply: true }
    );
    expect(stanceDrift.status).toBe("skipped");
    expect(applyUntag).not.toHaveBeenCalled();
  });

  it("skips when no live record carries the tag", async () => {
    const outcome = await untagOneRecordArea(INPUT, makeDeps({ loadTag: async () => null }), {
      apply: true,
    });
    expect(outcome.status).toBe("skipped");
  });

  it("skips without deleting when the compare-and-swap misses (concurrent write)", async () => {
    const outcome = await untagOneRecordArea(INPUT, makeDeps({ applyUntag: async () => 0 }), {
      apply: true,
    });
    expect(outcome).toMatchObject({ status: "skipped", reason: expect.stringMatching(/concurrent write/) });
  });
});
