import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { loadDoneCandidateIds } from "../../src/scripts/relabelCandidateRecordAreas.js";

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
