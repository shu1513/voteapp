import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import type { FederalMemberResolution } from "../../src/pipeline/rollcall/federalMemberResolver.js";
import {
  DEFAULT_SCOPE_FROM,
  listRollCallEvidenceFiles,
  summarizeUnmatched,
} from "../../src/scripts/resolveRollCallMembers.js";

describe("listRollCallEvidenceFiles", () => {
  const dirs: string[] = [];
  afterEach(() => {
    for (const dir of dirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("picks the fetcher's XML files and orders them by chamber, congress, session, roll", () => {
    const dir = mkdtempSync(join(tmpdir(), "rollcall-evidence-"));
    dirs.push(dir);
    for (const name of [
      "senate-119-1-roll00618.xml",
      "house-119-1-roll00145.xml",
      "house-119-1-roll00014.xml",
      "house-118-2-roll00400.xml",
      "report.json",
      "resolve-report.json",
      "house-119-1-roll00145.xml.bak",
    ]) {
      writeFileSync(join(dir, name), "");
    }
    expect(listRollCallEvidenceFiles(dir)).toEqual([
      { file: "house-118-2-roll00400.xml", chamber: "house", congress: 118, session: 2, roll: 400 },
      { file: "house-119-1-roll00014.xml", chamber: "house", congress: 119, session: 1, roll: 14 },
      { file: "house-119-1-roll00145.xml", chamber: "house", congress: 119, session: 1, roll: 145 },
      { file: "senate-119-1-roll00618.xml", chamber: "senate", congress: 119, session: 1, roll: 618 },
    ]);
  });
});

describe("summarizeUnmatched", () => {
  function resolution(
    memberId: string,
    outcome: FederalMemberResolution["outcome"],
    name = memberId
  ): FederalMemberResolution {
    return {
      member: { chamber: "house", memberId, name, state: "NC", party: "D", vote: "Yea" },
      outcome,
      legislator: outcome === "unknown_member" ? null : { bioguide: memberId, name: `${name} (official)`, fecIds: ["H1"] },
      candidate: null,
      candidates: [],
      detail: outcome,
    };
  }

  it("lists each unresolved person once per outcome with how many roll calls they appeared in", () => {
    const summary = summarizeUnmatched([
      resolution("A1", "matched"),
      resolution("B2", "no_candidate", "Bravo"),
      resolution("B2", "no_candidate", "Bravo"),
      resolution("C3", "unknown_member", "Charlie"),
      resolution("A4", "no_candidate", "Alpha"),
    ]);
    expect(summary.map((row) => [row.memberId, row.outcome, row.rolls, row.legislator])).toEqual([
      ["A4", "no_candidate", 1, "Alpha (official)"],
      ["B2", "no_candidate", 2, "Bravo (official)"],
      ["C3", "unknown_member", 1, null],
    ]);
  });

  it("defaults the scope to the November 2026 ballot", () => {
    expect(DEFAULT_SCOPE_FROM).toBe("2026-11-01");
  });
});
