import { describe, expect, it, vi } from "vitest";

import {
  parseTagsFile,
  tagOneRecordArea,
  type TagDeps,
  type TagRecordRow,
} from "../../src/scripts/tagCandidateRecordAreas.js";

const RECORD: TagRecordRow = {
  candidate_id: "cand-1",
  description: "Sponsored a law requiring political ads made with artificial intelligence to say so.",
  office_id: "office-1",
  office_name: "state_lower/State Lower Chamber Legislator",
};

const INPUT = {
  recordId: "rec-1",
  researchAreaSlug: "ai_regulation",
  stance: "for" as const,
  expectedDescription: RECORD.description,
  reason: "A disclosure rule for AI-made political ads is an AI transparency rule.",
};

const ALLOWED = [
  { id: "area-ai", slug: "ai_regulation" },
  { id: "area-ei", slug: "election_integrity" },
];

function makeDeps(overrides: Partial<TagDeps> = {}): TagDeps {
  return {
    loadRecord: async () => ({ ...RECORD }),
    loadAllowedAreas: async () => ALLOWED,
    applyTag: async () => undefined,
    ...overrides,
  };
}

describe("parseTagsFile", () => {
  it("parses valid entries and trims fields", () => {
    const parsed = parseTagsFile(JSON.stringify([{ ...INPUT, recordId: " rec-1 ", researchAreaSlug: " slug " }]));
    expect(parsed).toEqual([{ ...INPUT, recordId: "rec-1", researchAreaSlug: "slug" }]);
  });

  it("rejects a null stance, a missing description, and a placeholder reason", () => {
    expect(() => parseTagsFile(JSON.stringify([{ ...INPUT, stance: null }]))).toThrow(/stance/);
    expect(() => parseTagsFile(JSON.stringify([{ ...INPUT, expectedDescription: "" }]))).toThrow(/expectedDescription/);
    expect(() => parseTagsFile(JSON.stringify([{ ...INPUT, reason: "AI" }]))).toThrow(/reason/);
  });

  it("rejects a non-array file", () => {
    expect(() => parseTagsFile(JSON.stringify({}))).toThrow(/JSON array/);
  });
});

describe("tagOneRecordArea", () => {
  it("dry-runs by default: reports would_tag with the office, writes nothing", async () => {
    const applyTag = vi.fn(async () => undefined);
    const outcome = await tagOneRecordArea(INPUT, makeDeps({ applyTag }), { apply: false });
    expect(outcome).toMatchObject({ status: "would_tag", stance: "for", office: RECORD.office_name });
    expect(applyTag).not.toHaveBeenCalled();
  });

  it("applies through the validated slug → id map", async () => {
    const applyTag = vi.fn(async () => undefined);
    const outcome = await tagOneRecordArea(INPUT, makeDeps({ applyTag }), { apply: true });
    expect(outcome.status).toBe("tagged");
    expect(applyTag).toHaveBeenCalledWith(
      expect.objectContaining({ recordId: "rec-1", researchAreaSlug: "ai_regulation", stance: "for" })
    );
    const call = applyTag.mock.calls[0]![0] as { researchAreaIdBySlug: Map<string, string> };
    expect(call.researchAreaIdBySlug.get("ai_regulation")).toBe("area-ai");
  });

  it("skips when the description moved since review, even on apply", async () => {
    const applyTag = vi.fn(async () => undefined);
    const outcome = await tagOneRecordArea(
      INPUT,
      makeDeps({ loadRecord: async () => ({ ...RECORD, description: "rewritten" }), applyTag }),
      { apply: true }
    );
    expect(outcome).toMatchObject({ status: "skipped", reason: expect.stringMatching(/changed since review/) });
    expect(applyTag).not.toHaveBeenCalled();
  });

  it("skips a slug the candidate's office does not allow", async () => {
    const outcome = await tagOneRecordArea(
      INPUT,
      makeDeps({ loadAllowedAreas: async () => [{ id: "area-ei", slug: "election_integrity" }] }),
      { apply: true }
    );
    expect(outcome).toMatchObject({ status: "skipped", reason: expect.stringMatching(/State Lower Chamber Legislator/) });
  });

  it("never rewrites an existing tag: same stance is a no-op, another stance is a conflict", async () => {
    const same = await tagOneRecordArea(
      INPUT,
      makeDeps({ loadRecord: async () => ({ ...RECORD, existing_stance: "for" }) }),
      { apply: true }
    );
    expect(same).toMatchObject({ status: "skipped", reason: "already tagged ai_regulation:for" });
    const conflict = await tagOneRecordArea(
      INPUT,
      makeDeps({ loadRecord: async () => ({ ...RECORD, existing_stance: "against" }) }),
      { apply: true }
    );
    expect(conflict).toMatchObject({ status: "skipped", reason: expect.stringMatching(/untag it first/) });
  });

  it("skips a retired record and a candidate with no office race", async () => {
    expect(await tagOneRecordArea(INPUT, makeDeps({ loadRecord: async () => null }), { apply: true })).toMatchObject({
      status: "skipped",
      reason: expect.stringMatching(/no live record/),
    });
    expect(
      await tagOneRecordArea(INPUT, makeDeps({ loadRecord: async () => ({ ...RECORD, office_id: null }) }), {
        apply: true,
      })
    ).toMatchObject({ status: "skipped", reason: expect.stringMatching(/no office race/) });
  });
});
