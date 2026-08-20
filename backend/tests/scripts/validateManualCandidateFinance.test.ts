import { describe, expect, it } from "vitest";

import { runValidateManualCandidateFinance } from "../../src/scripts/validateManualCandidateFinance.js";

describe("runValidateManualCandidateFinance", () => {
  it("rejects repeated --file flags instead of silently using the first", async () => {
    await expect(
      runValidateManualCandidateFinance(["--file", "first.json", "--file", "second.json"])
    ).rejects.toThrow("--file may be provided only once");
  });
});
