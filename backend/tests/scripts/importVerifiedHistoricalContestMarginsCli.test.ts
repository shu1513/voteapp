import { describe, expect, it } from "vitest";

import { parseVerifiedHistoricalContestMarginImportArgs } from "../../src/scripts/importVerifiedHistoricalContestMarginsCli.js";

describe("importVerifiedHistoricalContestMarginsCli", () => {
  it("defaults to a real import", () => {
    expect(parseVerifiedHistoricalContestMarginImportArgs([])).toEqual({
      dryRun: false,
    });
  });

  it("parses dry-run imports", () => {
    expect(parseVerifiedHistoricalContestMarginImportArgs(["--dry-run"])).toEqual({
      dryRun: true,
    });
  });

  it("rejects unknown arguments", () => {
    expect(() => parseVerifiedHistoricalContestMarginImportArgs(["--source=MIT_2024"])).toThrow(
      "Unknown verified historical contest import argument: --source=MIT_2024"
    );
  });
});
