import { describe, expect, it } from "vitest";

import { parseLinkIdahoCandidateFinanceScriptArgs } from "../../src/scripts/linkIdahoCandidateFinance.js";

const IDS = ["--candidate-id=c1", "--election-id", "e1", "--registration-guid=g1"];

describe("parseLinkIdahoCandidateFinanceScriptArgs", () => {
  it("parses the three ids in either flag form plus the boolean flags", () => {
    expect(parseLinkIdahoCandidateFinanceScriptArgs(IDS)).toEqual({
      force: false,
      dryRun: false,
      candidateId: "c1",
      electionId: "e1",
      registrationGuid: "g1",
    });
    expect(parseLinkIdahoCandidateFinanceScriptArgs([...IDS, "--dry-run", "--force"])).toMatchObject({
      force: true,
      dryRun: true,
    });
  });

  it("rejects missing ids, unknown flags, positionals, and repeats", () => {
    expect(() => parseLinkIdahoCandidateFinanceScriptArgs(IDS.slice(0, 3))).toThrow("Missing --registration-guid value");
    expect(() => parseLinkIdahoCandidateFinanceScriptArgs([...IDS, "--guid=x"])).toThrow(
      "Unknown Idaho candidate finance manual link flag: --guid"
    );
    expect(() => parseLinkIdahoCandidateFinanceScriptArgs([...IDS, "c2"])).toThrow("Unexpected positional argument");
    expect(() => parseLinkIdahoCandidateFinanceScriptArgs([...IDS, "--candidate-id=c2"])).toThrow("at most once");
    expect(() => parseLinkIdahoCandidateFinanceScriptArgs([...IDS, "--dry-run=true"])).toThrow("does not accept a value");
  });
});
