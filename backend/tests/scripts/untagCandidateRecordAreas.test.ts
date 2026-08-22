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

  it("deletes under a compare-and-swap on stance and record description", async () => {
    const applyUntag = vi.fn(async () => 1);
    const outcome = await untagOneRecordArea(INPUT, makeDeps({ applyUntag }), { apply: true });
    expect(outcome.status).toBe("untagged");
    expect(applyUntag).toHaveBeenCalledWith({
      tagId: "tag-1",
      expected: { stance: TAG.stance, description: TAG.description },
    });
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
