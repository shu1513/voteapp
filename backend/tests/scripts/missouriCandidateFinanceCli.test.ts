import { describe, expect, it } from "vitest";

import { parseMissouriCandidateFinanceCliArgs } from "../../src/scripts/missouriCandidateFinanceCli.js";

describe("parseMissouriCandidateFinanceCliArgs", () => {
  it("parses bounded operational options", () => {
    expect(parseMissouriCandidateFinanceCliArgs(["--dry-run", "--max-candidates=4", "--cache-dir", "/tmp/cache"])).toMatchObject({
      dryRun: true, maxCandidates: 4, cacheDir: "/tmp/cache",
    });
  });

  it("rejects unknown, repeated, and unsafe values", () => {
    expect(() => parseMissouriCandidateFinanceCliArgs(["--dryrun"])).toThrow("Unknown Missouri candidate finance flag");
    expect(() => parseMissouriCandidateFinanceCliArgs(["--max-candidates=2", "--max-candidates", "3"])).toThrow("at most once");
    expect(() => parseMissouriCandidateFinanceCliArgs(["--max-candidates=9007199254740992"])).toThrow("Invalid --max-candidates");
  });
});
