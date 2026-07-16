import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { afterEach, describe, expect, it } from "vitest";

import {
  buildManualResearchRepairReport,
  summarizeManualResearchGaps,
  writeManualResearchRepairReport,
  type ManualResearchRepairGap,
} from "../../src/scripts/manualResearchRepairReport.js";

const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("manualResearchRepairReport", () => {
  const repairGap: ManualResearchRepairGap = {
    id: "candidate_profile.summary",
    stage: "candidate_profile",
    objectType: "candidate_profile",
    outcome: "needs_repair",
    field: "summary",
    failureKind: "quality_gap",
    reason: "Candidate profile summary is missing.",
    promptFile: "src/ai/providers/candidateProfilePrompt.ts",
    focusedResearchPass: "Run a focused summary-only profile pass.",
  };

  it("builds a durable repair report with needs_repair status", () => {
    const report = buildManualResearchRepairReport({
      command: "manual:candidate-profile:write",
      manualKey: "manual:candidate-profile:election:jane",
      target: {
        electionId: "election-1",
        displayName: "Jane Candidate",
      },
      gaps: [repairGap],
    });

    expect(report.schemaVersion).toBe("manual_research_repair_report.v1");
    expect(report.status).toBe("needs_repair");
    expect(report.target).toEqual({
      electionId: "election-1",
      displayName: "Jane Candidate",
    });
    expect(report.gaps).toEqual([repairGap]);
  });

  it("marks reports as blocked_by_contract_only when no repairable gap exists", () => {
    const report = buildManualResearchRepairReport({
      command: "manual:candidate-profile:write",
      manualKey: "manual:candidate-profile:election:jane",
      target: { electionId: "election-1" },
      gaps: [
        {
          ...repairGap,
          id: "candidate_profile.current_office",
          outcome: "blocked_by_contract",
        },
      ],
    });

    expect(report.status).toBe("blocked_by_contract_only");
  });

  it("marks reports as confirmed_only when all gaps are operator-confirmed", () => {
    const report = buildManualResearchRepairReport({
      command: "manual:candidate-records:write",
      manualKey: "manual:candidate-records:election:jane",
      target: { electionId: "election-1" },
      gaps: [
        {
          ...repairGap,
          outcome: "confirmed_null",
        },
        {
          ...repairGap,
          id: "candidate_records.only_general_labels",
          outcome: "confirmed_neutral",
        },
      ],
    });

    expect(report.status).toBe("confirmed_only");
  });

  it("writes reports to nested paths", async () => {
    const dir = await mkdtemp(join(tmpdir(), "manual-research-report-"));
    tempDirs.push(dir);
    const path = join(dir, "nested", "repair.json");
    const report = buildManualResearchRepairReport({
      command: "manual:candidate-records:write",
      manualKey: "manual:candidate-records:election:candidate",
      target: { candidateId: "candidate-1", electionId: "election-1" },
      gaps: [repairGap],
    });

    await writeManualResearchRepairReport(path, report);

    const written = JSON.parse(await readFile(path, "utf8")) as unknown;
    expect(written).toMatchObject({
      schemaVersion: "manual_research_repair_report.v1",
      command: "manual:candidate-records:write",
      status: "needs_repair",
    });
  });

  it("overwrites a previous machine repair report at the same path", async () => {
    const dir = await mkdtemp(join(tmpdir(), "manual-research-report-"));
    tempDirs.push(dir);
    const path = join(dir, "repair.json");
    const report = buildManualResearchRepairReport({
      command: "manual:candidate-records:write",
      manualKey: "manual:candidate-records:election:candidate",
      target: { candidateId: "candidate-1", electionId: "election-1" },
      gaps: [repairGap],
    });

    await writeManualResearchRepairReport(path, report);
    await writeManualResearchRepairReport(path, {
      ...report,
      manualKey: "manual:candidate-records:election:candidate-2",
    });

    const written = JSON.parse(await readFile(path, "utf8")) as { manualKey?: string };
    expect(written.manualKey).toBe("manual:candidate-records:election:candidate-2");
  });

  it("refuses to overwrite a file that is not a previous repair report", async () => {
    // A live run passed its accumulated human Markdown run report as
    // --repair-report-file and the wrapper replaced it with machine JSON.
    const dir = await mkdtemp(join(tmpdir(), "manual-research-report-"));
    tempDirs.push(dir);
    const path = join(dir, "run-report.md");
    await writeFile(path, "# Human run report\n\nAccumulated findings.\n", "utf8");
    const report = buildManualResearchRepairReport({
      command: "manual:candidate-records:write",
      manualKey: "manual:candidate-records:election:candidate",
      target: { candidateId: "candidate-1", electionId: "election-1" },
      gaps: [repairGap],
    });

    await expect(writeManualResearchRepairReport(path, report)).rejects.toThrow(
      /refuses to overwrite .*run-report\.md/
    );
    expect(await readFile(path, "utf8")).toContain("# Human run report");
  });

  it("refuses to overwrite unrelated JSON (an evidence ledger) too", async () => {
    const dir = await mkdtemp(join(tmpdir(), "manual-research-report-"));
    tempDirs.push(dir);
    const path = join(dir, "evidence.json");
    await writeFile(path, JSON.stringify({ entries: [{ question: "q", finding: "f" }] }), "utf8");
    const report = buildManualResearchRepairReport({
      command: "manual:candidate-records:write",
      manualKey: "manual:candidate-records:election:candidate",
      target: { candidateId: "candidate-1", electionId: "election-1" },
      gaps: [repairGap],
    });

    await expect(writeManualResearchRepairReport(path, report)).rejects.toThrow(
      /not a previous manual-research repair report/
    );
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({
      entries: [{ question: "q", finding: "f" }],
    });
  });

  it("summarizes gaps for CLI errors", () => {
    expect(summarizeManualResearchGaps([repairGap])).toBe(
      "candidate_profile.summary: Candidate profile summary is missing."
    );
  });
});
