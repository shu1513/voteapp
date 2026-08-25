import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";

import type { LegiscanMemberResolution } from "../../src/pipeline/rollcall/legiscanMemberResolver.js";
import {
  parseLegiscanBillList,
  readLegiscanDataset,
  surveyLegiscanDataset,
} from "../../src/scripts/fetchLegiscanRollCallVotes.js";
import { collectLegiscanVoters } from "../../src/scripts/importLegiscanRollCallVotes.js";
import {
  listLegiscanRollCallEvidenceFiles,
  peopleSnapshotFileName,
} from "../../src/scripts/resolveLegiscanRollCallMembers.js";

const UUID_A = "11111111-1111-4111-8111-111111111111";

describe("parseLegiscanBillList", () => {
  it("normalizes to the stored measure spelling and rejects junk", () => {
    expect([...parseLegiscanBillList("hb1,SB0544, sb 544")]).toEqual(["HB 1", "SB 544"]);
    expect(() => parseLegiscanBillList("hb1,RV#105")).toThrow("not a bill number");
    expect(() => parseLegiscanBillList(" , ")).toThrow("names no bills");
  });
});

describe("readLegiscanDataset + surveyLegiscanDataset", () => {
  const dir = mkdtempSync(join(tmpdir(), "legiscan-dataset-"));
  afterAll(() => rmSync(dir, { recursive: true, force: true }));

  it("routes files by envelope across nested directories and surveys descs", () => {
    // The real archive nests <ST>/<session>/{bill,vote,people}; the reader
    // must not care.
    mkdirSync(join(dir, "XX/2025-2026_Regular_Session/bill"), { recursive: true });
    mkdirSync(join(dir, "XX/2025-2026_Regular_Session/vote"), { recursive: true });
    mkdirSync(join(dir, "XX/2025-2026_Regular_Session/people"), { recursive: true });
    const bill = {
      bill_id: 1,
      bill_number: "HB1",
      bill_type: "B",
      session: { session_id: 2172 },
      state: "XX",
      title: "An act",
      url: "https://legiscan.com/XX/bill/HB1/2025",
      state_link: "https://legis.example.gov/HB1",
      votes: [{ roll_call_id: 77, url: "https://legiscan.com/XX/rollcall/HB1/id/77" }],
    };
    const rollCall = {
      roll_call_id: 77,
      bill_id: 1,
      date: "2025-04-09",
      desc: "Third Reading",
      yea: 2,
      nay: 1,
      nv: 0,
      absent: 0,
      total: 3,
      passed: 1,
      chamber: "H",
      chamber_id: 1,
      votes: [
        { people_id: 10, vote_id: 1 },
        { people_id: 11, vote_id: 1 },
        { people_id: 12, vote_id: 2 },
      ],
    };
    writeFileSync(join(dir, "XX/2025-2026_Regular_Session/bill/HB1.json"), JSON.stringify({ status: "OK", bill }));
    writeFileSync(join(dir, "XX/2025-2026_Regular_Session/vote/77.json"), JSON.stringify({ status: "OK", roll_call: rollCall }));
    writeFileSync(
      join(dir, "XX/2025-2026_Regular_Session/people/10.json"),
      JSON.stringify({ status: "OK", person: { people_id: 10, name: "A B", first_name: "A", last_name: "B", role: "Rep" } })
    );
    writeFileSync(join(dir, "XX/2025-2026_Regular_Session/hash.md5.json"), JSON.stringify({ status: "OK" }));
    writeFileSync(join(dir, "XX/2025-2026_Regular_Session/broken.json"), "{not json");

    const dataset = readLegiscanDataset(dir);
    expect(dataset.billsById.get(1)?.measureId).toBe("HB 1");
    expect(dataset.votes).toHaveLength(1);
    expect(dataset.people).toHaveLength(1);
    expect(dataset.fileErrors).toHaveLength(1);
    expect(dataset.fileErrors[0]!.file).toContain("broken.json");

    const survey = surveyLegiscanDataset(dataset);
    expect(survey.billTypeCounts).toEqual({ B: 1 });
    expect(survey.sessionIds).toEqual({ "2172": 1 });
    expect(survey.rows).toEqual([
      {
        chamber: "house",
        desc: "Third Reading",
        count: 1,
        minTotal: 3,
        maxTotal: 3,
        billTypes: ["B"],
        sampleBills: ["HB1"],
      },
    ]);
  });
});

describe("listLegiscanRollCallEvidenceFiles", () => {
  const dir = mkdtempSync(join(tmpdir(), "legiscan-evidence-"));
  afterAll(() => rmSync(dir, { recursive: true, force: true }));

  it("lists only this state and session's evidence, in chamber and roll order", () => {
    for (const file of [
      "ls-tx-senate-2172-roll300.json",
      "ls-tx-house-2172-roll200.json",
      "ls-tx-house-2172-roll100.json",
      // Another state, another session, the Ohio pilot, and reports.
      "ls-ga-house-2172-roll400.json",
      "ls-tx-house-999-roll500.json",
      "oh-house-136-roll1740000000.json",
      "import-report.json",
    ]) {
      writeFileSync(join(dir, file), "{}");
    }
    expect(listLegiscanRollCallEvidenceFiles(dir, { jurisdiction: "TX", sessionId: 2172 })).toEqual([
      { file: "ls-tx-house-2172-roll100.json", state: "TX", chamber: "house", sessionId: 2172, roll: 100 },
      { file: "ls-tx-house-2172-roll200.json", state: "TX", chamber: "house", sessionId: 2172, roll: 200 },
      { file: "ls-tx-senate-2172-roll300.json", state: "TX", chamber: "senate", sessionId: 2172, roll: 300 },
    ]);
  });
});

describe("peopleSnapshotFileName", () => {
  it("matches the committed snapshot convention", () => {
    expect(peopleSnapshotFileName("TX", 2172)).toBe("legiscan-people-tx-2172.json");
  });
});

describe("collectLegiscanVoters", () => {
  const matched = (peopleId: number, candidateId: string, side: "yea" | "nay"): LegiscanMemberResolution => ({
    peopleId,
    side,
    outcome: "matched",
    person: null,
    candidate: { candidateId, name: `candidate ${candidateId.slice(0, 4)}`, inScope: true },
    detail: "",
  });

  it("keeps matched voters with their sides and counts every outcome", () => {
    const counts: Partial<Record<LegiscanMemberResolution["outcome"], number>> = {};
    const voters = collectLegiscanVoters(
      [
        matched(1, UUID_A, "yea"),
        { peopleId: 2, side: "nay", outcome: "no_crosswalk", person: null, candidate: null, detail: "" },
      ],
      counts
    );
    expect(voters).toEqual([{ candidateId: UUID_A, candidateName: `candidate ${UUID_A.slice(0, 4)}`, peopleId: 1, memberName: null, side: "yea" }]);
    expect(counts).toEqual({ matched: 1, no_crosswalk: 1 });
  });

  it("fails the roll call when two members land on one candidate", () => {
    expect(() => collectLegiscanVoters([matched(1, UUID_A, "yea"), matched(2, UUID_A, "nay")], {})).toThrow(
      "more than one member"
    );
  });
});
