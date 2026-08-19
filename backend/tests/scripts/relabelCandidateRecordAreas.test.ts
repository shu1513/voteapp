import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { loadDoneCandidateIds, resolveFileLabels } from "../../src/scripts/relabelCandidateRecordAreas.js";

async function writeJsonl(rows: readonly Record<string, unknown>[]): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "relabel-"));
  const file = join(dir, "relabel.jsonl");
  await writeFile(file, `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`);
  return file;
}

describe("loadDoneCandidateIds", () => {
  it("returns an empty set when the out-file does not exist yet", async () => {
    const dir = await mkdtemp(join(tmpdir(), "relabel-"));
    expect(await loadDoneCandidateIds(join(dir, "missing.jsonl"), { dryRun: false })).toEqual(new Set());
  });

  it("treats only live ok rows as done for a live run, so a reviewed dry-run file still inserts", async () => {
    const file = await writeJsonl([
      { candidate_id: "dry-ok", status: "ok", dry_run: true },
      { candidate_id: "live-ok", status: "ok", dry_run: false },
      { candidate_id: "failed", status: "ai_failed", dry_run: false },
      { candidate_id: "no-context", status: "skipped_no_context" },
    ]);
    expect(await loadDoneCandidateIds(file, { dryRun: false })).toEqual(new Set(["live-ok"]));
  });

  it("treats dry-run and live ok rows as done for another dry-run", async () => {
    const file = await writeJsonl([
      { candidate_id: "dry-ok", status: "ok", dry_run: true },
      { candidate_id: "live-ok", status: "ok", dry_run: false },
      { candidate_id: "failed", status: "ai_failed", dry_run: true },
    ]);
    expect(await loadDoneCandidateIds(file, { dryRun: true })).toEqual(new Set(["dry-ok", "live-ok"]));
  });

  it("ignores a truncated final line (killed run) but rejects a malformed earlier line", async () => {
    const dir = await mkdtemp(join(tmpdir(), "relabel-"));
    const truncated = join(dir, "truncated.jsonl");
    await writeFile(
      truncated,
      `${JSON.stringify({ candidate_id: "live-ok", status: "ok", dry_run: false })}\n{"candidate_id":"half`
    );
    expect(await loadDoneCandidateIds(truncated, { dryRun: false })).toEqual(new Set(["live-ok"]));

    const corrupt = join(dir, "corrupt.jsonl");
    await writeFile(
      corrupt,
      `{"candidate_id":"half\n${JSON.stringify({ candidate_id: "live-ok", status: "ok", dry_run: false })}\n`
    );
    await expect(loadDoneCandidateIds(corrupt, { dryRun: false })).rejects.toThrow();
  });
});

describe("resolveFileLabels", () => {
  const live = new Set(["rec-1", "rec-2"]);
  const allowed = new Set(["general", "integrity_and_ethics", "government_spending_reduction", "public_education_quality"]);

  it("accepts stanced labels on live records and allowed areas, lowercasing the slug", () => {
    const result = resolveFileLabels(
      {
        labels: [
          { record_id: "rec-1", research_area_slug: "Government_Spending_Reduction", stance: "against" },
          { record_id: "rec-2", research_area_slug: "public_education_quality", stance: "for" },
        ],
      },
      live,
      allowed
    );
    expect(result).toEqual({
      ok: true,
      labels: [
        { record_id: "rec-1", research_area_slug: "government_spending_reduction", stance: "against" },
        { record_id: "rec-2", research_area_slug: "public_education_quality", stance: "for" },
      ],
    });
  });

  it("accepts an empty labels array (candidate reviewed, nothing to add)", () => {
    expect(resolveFileLabels({ labels: [] }, live, allowed)).toEqual({ ok: true, labels: [] });
  });

  it("collects every problem and prints the allowlist on an allowlist rejection", () => {
    const result = resolveFileLabels(
      {
        labels: [
          { record_id: "rec-9", research_area_slug: "public_education_quality", stance: "for" },
          { record_id: "rec-1", research_area_slug: "housing_affordability", stance: "for" },
          { record_id: "rec-1", research_area_slug: "general" },
          { record_id: "rec-1", research_area_slug: "public_education_quality", stance: "neutral" },
          { record_id: "rec-2", research_area_slug: "public_education_quality", stance: "for" },
          { record_id: "rec-2", research_area_slug: "public_education_quality", stance: "against" },
        ],
      },
      live,
      allowed
    );
    expect(result.ok).toBe(false);
    if (result.ok) {
      return;
    }
    expect(result.reason).toContain("labels[0]: record_id 'rec-9' is not a live record of this candidate");
    expect(result.reason).toContain("labels[1]: research_area_slug 'housing_affordability' is not in the allowed research areas");
    expect(result.reason).toContain("labels[2]: 'general' is a non-stance area");
    expect(result.reason).toContain("labels[3]: stance must be 'for' or 'against', got \"neutral\"");
    expect(result.reason).toContain("labels[5]: duplicate (record_id, research_area_slug) pair");
    expect(result.reason).toContain("allowed research areas for this office: general, government_spending_reduction, integrity_and_ethics, public_education_quality");
  });

  it("rejects payloads that are not {labels: [...]}", () => {
    expect(resolveFileLabels([], live, allowed)).toEqual({ ok: false, reason: "labels-file payload must be an object" });
    expect(resolveFileLabels({ labels: "x" }, live, allowed)).toEqual({ ok: false, reason: "labels-file payload.labels must be an array" });
  });
});
