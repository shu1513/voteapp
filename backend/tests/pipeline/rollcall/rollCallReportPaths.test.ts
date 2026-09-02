import { describe, expect, it } from "vitest";

import { reportPath } from "../../../src/pipeline/rollcall/rollCallReportPaths.js";

const CWD = "/Users/dev/voteApp/backend";
const HOME = "/Users/dev";

describe("reportPath", () => {
  it("records a path under the working directory relative to it", () => {
    expect(reportPath("/Users/dev/voteApp/backend/evidence/rollcall/x/crosswalk.json", CWD, HOME)).toBe(
      "evidence/rollcall/x/crosswalk.json"
    );
    expect(reportPath("evidence/rollcall/x/crosswalk.json", CWD, HOME)).toBe("evidence/rollcall/x/crosswalk.json");
    expect(reportPath(CWD, CWD, HOME)).toBe(".");
  });

  it("records a path under the home directory as ~/...", () => {
    expect(reportPath("/Users/dev/legiscan-data/ca-2172-evidence", CWD, HOME)).toBe("~/legiscan-data/ca-2172-evidence");
    // A sibling of the repo is not under the working directory, so it is
    // not written with a `..` chain that depends on checkout depth.
    expect(reportPath("/Users/dev/voteApp/other/file.json", CWD, HOME)).toBe("~/voteApp/other/file.json");
  });

  it("leaves a path outside both alone", () => {
    expect(reportPath("/data/legiscan/tx-2141", CWD, HOME)).toBe("/data/legiscan/tx-2141");
    expect(reportPath("/Users/dev-other/x", CWD, HOME)).toBe("/Users/dev-other/x");
  });
});
