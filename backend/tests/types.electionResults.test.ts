import { describe, expect, it } from "vitest";

import {
  BALLOT_MEASURE_RESULT_OUTCOMES,
  CANDIDATE_ELECTION_STATUSES,
  ELECTION_RESULT_OUTCOMES,
  ELECTION_RESULT_MATCH_STATUSES,
  ELECTION_RESULT_PASS_TYPES,
  ELECTION_RESULT_SOURCE_TYPES,
  ELECTION_RESULT_STATUSES,
  canProjectToCanonicalElectionStatus,
} from "../src/types/electionResults.js";

describe("election result policy types", () => {
  it("defines the two result search pass types", () => {
    expect(ELECTION_RESULT_PASS_TYPES).toEqual(["election_night", "certified"]);
  });

  it("keeps election-night and projected results out of canonical candidate status", () => {
    expect(
      canProjectToCanonicalElectionStatus({
        passType: "election_night",
        resultStatus: "unofficial",
        sourceType: "official",
      })
    ).toBe(false);

    expect(
      canProjectToCanonicalElectionStatus({
        passType: "certified",
        resultStatus: "projected",
        sourceType: "ap",
      })
    ).toBe(false);
  });

  it("allows canonical status projection only for official certified-pass results", () => {
    expect(
      canProjectToCanonicalElectionStatus({
        passType: "certified",
        resultStatus: "certified",
        sourceType: "official",
      })
    ).toBe(true);

    expect(
      canProjectToCanonicalElectionStatus({
        passType: "certified",
        resultStatus: "certified",
        sourceType: "news",
      })
    ).toBe(false);
  });

  it("keeps schema constants aligned with planned result storage", () => {
    expect(ELECTION_RESULT_STATUSES).toContain("not_final_yet");
    expect(ELECTION_RESULT_SOURCE_TYPES).toEqual(["official", "ap", "news", "other"]);
    expect(ELECTION_RESULT_OUTCOMES).toContain("advanced");
    expect(ELECTION_RESULT_OUTCOMES).toContain("runoff");
    expect(ELECTION_RESULT_OUTCOMES).not.toContain("lost");
    expect(ELECTION_RESULT_MATCH_STATUSES).toEqual(["matched", "partial", "unmatched", "not_applicable", "not_found"]);
    expect(BALLOT_MEASURE_RESULT_OUTCOMES).toEqual(["passed", "failed", "unknown"]);
    expect(CANDIDATE_ELECTION_STATUSES).toEqual(["declared", "withdrawn", "won", "lost", "advanced", "runoff"]);
  });
});
