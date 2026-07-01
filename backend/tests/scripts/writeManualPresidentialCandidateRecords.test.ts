import { describe, expect, it } from "vitest";

import {
  buildNormalizedResearchAreaLookup,
  parseManualPresidentialCandidateRecordsArgs,
} from "../../src/scripts/writeManualPresidentialCandidateRecords.js";

describe("parseManualPresidentialCandidateRecordsArgs", () => {
  it("parses the required presidential records flags", () => {
    const parsed = parseManualPresidentialCandidateRecordsArgs([
      "--candidate-id",
      "candidate-1",
      "--presidential-cycle-id",
      "cycle-2028",
      "--presidential-role",
      "president",
      "--records-file",
      "records.json",
      "--labels-file",
      "labels.json",
      "--repair-report-file",
      "repair.json",
      "--strict-quality-gate",
      "--confirmed-gap",
      "candidate_records.only_general_labels",
      "--dry-run",
    ]);

    expect(parsed).toMatchObject({
      candidateId: "candidate-1",
      presidentialCycleId: "cycle-2028",
      presidentialRole: "president",
      recordsFile: "records.json",
      labelsFile: "labels.json",
      repairReportFile: "repair.json",
      strictQualityGate: true,
      dryRun: true,
    });
    expect([...parsed.confirmedGapIds]).toEqual(["candidate_records.only_general_labels"]);
  });

  it("supports vice president context", () => {
    expect(
      parseManualPresidentialCandidateRecordsArgs([
        "--candidate-id=candidate-1",
        "--presidential-cycle-id=cycle-2028",
        "--presidential-role=vice_president",
        "--records-file=records.json",
        "--labels-file=labels.json",
      ])
    ).toMatchObject({
      presidentialRole: "vice_president",
      strictQualityGate: false,
      dryRun: false,
    });
  });

  it("rejects invalid presidential roles", () => {
    expect(() =>
      parseManualPresidentialCandidateRecordsArgs([
        "--candidate-id",
        "candidate-1",
        "--presidential-cycle-id",
        "cycle-2028",
        "--presidential-role",
        "governor",
        "--records-file",
        "records.json",
        "--labels-file",
        "labels.json",
      ])
    ).toThrow("Invalid --presidential-role value: governor");
  });

  it("rejects boolean flags with explicit values", () => {
    expect(() =>
      parseManualPresidentialCandidateRecordsArgs([
        "--candidate-id=candidate-1",
        "--presidential-cycle-id=cycle-2028",
        "--presidential-role=president",
        "--records-file=records.json",
        "--labels-file=labels.json",
        "--dry-run=true",
      ])
    ).toThrow("Boolean flag must not include a value: --dry-run");
    expect(() =>
      parseManualPresidentialCandidateRecordsArgs([
        "--candidate-id=candidate-1",
        "--presidential-cycle-id=cycle-2028",
        "--presidential-role=president",
        "--records-file=records.json",
        "--labels-file=labels.json",
        "--strict-quality-gate",
        "true",
      ])
    ).toThrow("Boolean flag must not include a value: --strict-quality-gate");
  });

  it("does not accept the election-records target shape", () => {
    expect(() =>
      parseManualPresidentialCandidateRecordsArgs([
        "--candidate-id",
        "candidate-1",
        "--election-id",
        "election-1",
        "--records-file",
        "records.json",
        "--labels-file",
        "labels.json",
      ])
    ).toThrow("Missing --presidential-cycle-id");
  });
});

describe("buildNormalizedResearchAreaLookup", () => {
  it("normalizes allowed research-area slugs for validation and tag lookup", () => {
    const lookup = buildNormalizedResearchAreaLookup([
      { id: "area-general", slug: " General " },
      { id: "area-economy", slug: "ECONOMY" },
    ]);

    expect([...lookup.allowedSlugs].sort()).toEqual(["economy", "general"]);
    expect(lookup.researchAreaIdBySlug.get("general")).toBe("area-general");
    expect(lookup.researchAreaIdBySlug.get("economy")).toBe("area-economy");
  });
});
