import { describe, expect, it } from "vitest";

import { parseManualCandidateFinanceImportArgs } from "../../src/scripts/importManualCandidateFinance.js";

describe("parseManualCandidateFinanceImportArgs", () => {
  it("defaults to dry-run and accepts a deliberate write opt-in", () => {
    expect(parseManualCandidateFinanceImportArgs(["--file", "one.json", "--file", "two.json"])).toEqual({
      files: ["one.json", "two.json"],
      write: false,
    });
    expect(parseManualCandidateFinanceImportArgs(["--file", "one.json", "--write"])).toEqual({
      files: ["one.json"],
      write: true,
    });
  });

  it("rejects missing files, duplicate write flags, and unknown flags", () => {
    expect(() => parseManualCandidateFinanceImportArgs([])).toThrow("At least one --file is required");
    expect(() =>
      parseManualCandidateFinanceImportArgs(["--file", "one.json", "--write", "--write"])
    ).toThrow("--write may be provided only once");
    expect(() => parseManualCandidateFinanceImportArgs(["--file", "one.json", "--dry-run"])).toThrow(
      "unknown flag --dry-run"
    );
  });
});
