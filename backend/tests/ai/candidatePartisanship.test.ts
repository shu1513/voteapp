import { describe, expect, it } from "vitest";

import {
  assertCandidatePartyWillNotBeDiscarded,
  resolveIncludePartyForCandidateContest,
} from "../../src/ai/candidatePartisanship.js";

describe("candidatePartisanship", () => {
  it("fails closed when stored election metadata contradicts fixed Washington policy", () => {
    expect(() =>
      resolveIncludePartyForCandidateContest({
        districtType: "state_lower",
        state: "WA",
        officialBallotTitle: "State Representative Position 1",
        electionIsPartisan: false,
      })
    ).toThrow(/contradicts fixed partisanship policy/i);
  });

  it("still trusts explicit metadata for contests whose policy is contextual", () => {
    expect(
      resolveIncludePartyForCandidateContest({
        districtType: "place",
        state: "WA",
        officialBallotTitle: "City Council Member",
        electionIsPartisan: false,
      })
    ).toBe(false);
  });

  it("rejects a meaningful party label before nonpartisan storage discards it", () => {
    expect(() =>
      assertCandidatePartyWillNotBeDiscarded({
        includeParty: false,
        partyLabels: ["Republican"],
      })
    ).toThrow(/would discard candidate party/i);
  });

  it("allows absent and explicitly nonpartisan labels in nonpartisan contests", () => {
    expect(() =>
      assertCandidatePartyWillNotBeDiscarded({
        includeParty: false,
        partyLabels: [undefined, "Nonpartisan", "unknown"],
      })
    ).not.toThrow();
  });
});
