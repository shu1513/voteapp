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

  it("keeps the stored partisan flag on an Arkansas quorum-court seat", () => {
    // The JP title reads judicial state-blind; Arkansas's JPs are county
    // legislators printed with a party, so a stored true must not fail closed.
    expect(
      resolveIncludePartyForCandidateContest({
        districtType: "county",
        state: "AR",
        officialBallotTitle: "Izard County Justice of the Peace District 2",
        electionIsPartisan: true,
      })
    ).toBe(true);
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

  it("allows absent, nonpartisan, and explicit no-affiliation labels in nonpartisan contests", () => {
    expect(() =>
      assertCandidatePartyWillNotBeDiscarded({
        includeParty: false,
        partyLabels: [
          undefined,
          "Nonpartisan",
          "unknown",
          "No Party Preference",
          "NPP",
          "No Party Affiliation",
          "No Political Party",
          "Unaffiliated",
          "Unenrolled",
          "Undeclared",
          "Decline to State",
          "None",
        ],
      })
    ).not.toThrow();
  });

  it("keeps Independent meaningful instead of treating it as no affiliation", () => {
    expect(() =>
      assertCandidatePartyWillNotBeDiscarded({
        includeParty: false,
        partyLabels: ["Independent"],
      })
    ).toThrow(/Independent/);
  });
});
