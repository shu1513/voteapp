import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";

import type { OhioMemberResolution } from "../../src/pipeline/rollcall/ohioMemberResolver.js";
import { keptBillNumbers, parkStoredKeptVotes, parseBillList } from "../../src/scripts/fetchOhioRollCallVotes.js";
import { collectOhioVoters } from "../../src/scripts/importOhioRollCallVotes.js";
import { listOhioRollCallEvidenceFiles } from "../../src/scripts/resolveOhioRollCallMembers.js";

const UUID_A = "11111111-1111-4111-8111-111111111111";

describe("parseBillList", () => {
  it("normalizes, de-duplicates, and rejects non-bills", () => {
    expect(parseBillList("HB96, sb1,hb96")).toEqual(["hb96", "sb1"]);
    expect(() => parseBillList("hb96,am12")).toThrow("not an Ohio bill number");
    expect(() => parseBillList(" , ")).toThrow("names no bills");
  });
});

describe("keptBillNumbers", () => {
  it("keeps bills and joint resolutions only", () => {
    const list = {
      "0": { number: "hb1" },
      "1": { number: "sr101" },
      "2": { number: "hjr2" },
      "3": { number: "hcr9" },
      "4": { number: "sb42" },
      "5": { number: "hb1" },
    };
    expect(keptBillNumbers(list)).toEqual(["hb1", "hjr2", "sb42"]);
    expect(() => keptBillNumbers({ "0": {} })).toThrow("no number");
  });
});

describe("listOhioRollCallEvidenceFiles", () => {
  const dir = mkdtempSync(join(tmpdir(), "oh-evidence-"));
  afterAll(() => rmSync(dir, { recursive: true, force: true }));

  it("lists only Ohio evidence files, in chamber and time order", () => {
    for (const file of [
      "oh-senate-136-roll1750000000.json",
      "oh-house-136-roll1749000000.json",
      "oh-house-136-roll1740000000.json",
      // Federal files and reports in the same dir are not Ohio evidence.
      "house-119-1-roll00145.xml",
      "import-report.json",
    ]) {
      writeFileSync(join(dir, file), "{}");
    }
    expect(listOhioRollCallEvidenceFiles(dir)).toEqual([
      { file: "oh-house-136-roll1740000000.json", chamber: "house", generalAssembly: 136, roll: 1740000000 },
      { file: "oh-house-136-roll1749000000.json", chamber: "house", generalAssembly: 136, roll: 1749000000 },
      { file: "oh-senate-136-roll1750000000.json", chamber: "senate", generalAssembly: 136, roll: 1750000000 },
    ]);
  });
});

describe("collectOhioVoters", () => {
  const matched = (lpid: string, candidateId: string, side: "yea" | "nay"): OhioMemberResolution => ({
    lpid,
    side,
    outcome: "matched",
    legislator: null,
    candidate: { candidateId, name: `Candidate ${lpid}`, inScope: true },
    detail: "",
  });

  it("keeps matched voters with their side and counts every outcome", () => {
    const counts: Partial<Record<OhioMemberResolution["outcome"], number>> = {};
    const voters = collectOhioVoters(
      [
        matched("rep_a_1", UUID_A, "yea"),
        { lpid: "rep_b_1", side: "nay", outcome: "no_crosswalk", legislator: null, candidate: null, detail: "" },
        { lpid: "rep_c_1", side: "nay", outcome: "unmatched_reviewed", legislator: null, candidate: null, detail: "" },
      ],
      counts
    );
    expect(voters).toEqual([
      { candidateId: UUID_A, candidateName: "Candidate rep_a_1", lpid: "rep_a_1", memberName: null, side: "yea" },
    ]);
    expect(counts).toEqual({ matched: 1, no_crosswalk: 1, unmatched_reviewed: 1 });
  });

  it("fails the roll call when two lpids land on one candidate", () => {
    expect(() =>
      collectOhioVoters([matched("rep_a_1", UUID_A, "yea"), matched("sen_a_1", UUID_A, "nay")], {})
    ).toThrow("more than one member");
  });
});

describe("parkStoredKeptVotes", () => {
  it("parks stored pending kept rows and only reports approved ones", async () => {
    const statements: { text: string; values: unknown[] }[] = [];
    const db = {
      query: async (text: string, values?: unknown[]) => {
        statements.push({ text, values: values ?? [] });
        if (text.trimStart().startsWith("SELECT")) {
          return {
            rows: [
              { id: "row-pending", roll_number: 111, review_status: "pending" },
              { id: "row-approved", roll_number: 222, review_status: "approved" },
            ],
          };
        }
        return { rows: [] };
      },
    };
    const parked = await parkStoredKeptVotes(db as never, {
      chamber: "house",
      session: "136",
      measureId: "HB 96",
      voteDate: "2025-03-01",
    });
    expect(parked).toEqual([
      { id: "row-pending", rollNumber: 111, reviewStatus: "pending", parked: true },
      { id: "row-approved", rollNumber: 222, reviewStatus: "approved", parked: false },
    ]);
    // Exactly one UPDATE, aimed at the pending row, still guarded on pending.
    const updates = statements.filter((statement) => statement.text.includes("UPDATE"));
    expect(updates).toHaveLength(1);
    expect(updates[0]?.text).toContain("is_floor_vote = NULL");
    expect(updates[0]?.text).toContain("review_status = 'pending'");
    expect(updates[0]?.values).toEqual(["row-pending"]);
    // The scan is scoped to this jurisdiction/chamber/session/bill/day and
    // to rows currently kept.
    expect(statements[0]?.values).toEqual(["OH", "house", "136", "HB 96", "2025-03-01"]);
    expect(statements[0]?.text).toContain("is_floor_vote = true");
  });
});
