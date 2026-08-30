import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import type { FederalMemberResolution } from "../../src/pipeline/rollcall/federalMemberResolver.js";
import { collectVoters, importReportFileName, ROLLCALL_IMPORT_IMPORTER_VERSION } from "../../src/scripts/importRollCallVotes.js";

function resolution(
  memberId: string,
  vote: string,
  outcome: FederalMemberResolution["outcome"] = "matched",
  candidateId = `cand-${memberId}`
): FederalMemberResolution {
  const matched = outcome === "matched";
  // An out_of_scope resolution still names the one candidate it landed on,
  // exactly as resolveFederalMember reports it.
  const outOfScope = outcome === "out_of_scope";
  return {
    member: { chamber: "house", memberId, name: memberId, state: "NC", party: "D", vote },
    outcome,
    legislator: { bioguide: memberId, name: memberId, fecIds: ["H1"] },
    candidate: matched ? { candidateId, name: `Candidate ${memberId}`, inScope: true } : null,
    candidates: outOfScope ? [{ candidateId, name: `Candidate ${memberId}`, inScope: false }] : [],
    detail: outcome,
  };
}

describe("collectVoters", () => {
  it("keeps matched members who took a position, counts every outcome, and skips Present / Not Voting", () => {
    const counts = {};
    const result = collectVoters(
      [
        resolution("A1", "Yea"),
        resolution("B2", "No"),
        resolution("C3", "Not Voting"),
        resolution("D4", "Present"),
        resolution("E5", "Yea", "no_candidate"),
        resolution("F6", "Nay", "out_of_scope"),
      ],
      counts
    );
    expect(counts).toEqual({ matched: 4, no_candidate: 1, out_of_scope: 1 });
    expect(result.notVoting).toBe(2);
    expect(result.voters).toEqual([
      { candidateId: "cand-A1", candidateName: "Candidate A1", memberId: "A1", memberName: "A1", state: "NC", vote: "Yea", side: "yea", maintenanceOnly: false },
      { candidateId: "cand-B2", candidateName: "Candidate B2", memberId: "B2", memberName: "B2", state: "NC", vote: "No", side: "nay", maintenanceOnly: false },
      { candidateId: "cand-F6", candidateName: "Candidate F6", memberId: "F6", memberName: "F6", state: "NC", vote: "Nay", side: "nay", maintenanceOnly: true },
    ]);
  });

  it("keeps an out-of-scope member as a maintenance voter, but never counts their non-vote or an unresolved one", () => {
    const counts = {};
    const result = collectVoters(
      [
        // A withdrawn candidate's records must still be maintainable.
        resolution("G7", "Yea", "out_of_scope"),
        // Their Present / Not Voting does not affect the matched notVoting count.
        resolution("H8", "Not Voting", "out_of_scope"),
      ],
      counts
    );
    expect(counts).toEqual({ out_of_scope: 2 });
    expect(result.notVoting).toBe(0);
    expect(result.voters).toEqual([
      { candidateId: "cand-G7", candidateName: "Candidate G7", memberId: "G7", memberName: "G7", state: "NC", vote: "Yea", side: "yea", maintenanceOnly: true },
    ]);
  });

  it("still fails the roll call when a matched and an out-of-scope row land on one candidate", () => {
    expect(() =>
      collectVoters([resolution("A1", "Yea", "matched", "same"), resolution("B2", "Nay", "out_of_scope", "same")], {})
    ).toThrow(/candidate same matches more than one member/);
  });

  it("fails the roll call when two member rows land on one candidate, or a vote value is unknown", () => {
    expect(() => collectVoters([resolution("A1", "Yea", "matched", "same"), resolution("B2", "Nay", "matched", "same")], {})).toThrow(
      /candidate same matches more than one member/
    );
    expect(() => collectVoters([resolution("A1", "Guilty")], {})).toThrow(/unknown vote value: Guilty/);
  });

  it("stamps its own importer version", () => {
    expect(ROLLCALL_IMPORT_IMPORTER_VERSION).toBe("rollcall-import-v1");
  });
});

describe("importReportFileName", () => {
  let evidenceDir: string | null = null;

  afterEach(() => {
    if (evidenceDir) {
      rmSync(evidenceDir, { recursive: true, force: true });
      evidenceDir = null;
    }
  });

  it("keeps the pre-import plan once the dir has been imported", () => {
    evidenceDir = mkdtempSync(join(tmpdir(), "rollcall-import-report-"));
    expect(importReportFileName(evidenceDir, false)).toBe("import-report.json");
    // The reviewing loop: dry runs before the import overwrite the plan.
    expect(importReportFileName(evidenceDir, true)).toBe("import-dry-run-report.json");
    writeFileSync(join(evidenceDir, "import-report.json"), "{}\n");
    // After the import, a dry run is a check and must not eat the plan.
    expect(importReportFileName(evidenceDir, true)).toBe("import-dry-run-rerun-report.json");
    expect(importReportFileName(evidenceDir, false)).toBe("import-report.json");
  });
});
