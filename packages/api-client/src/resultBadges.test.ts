import { describe, expect, it } from "vitest";
import { deriveCandidateResultBadges } from "./resultBadges";

const ROSTER = [
  { candidate_id: "c-1", status: "declared" },
  { candidate_id: "c-2", status: "declared" },
  { candidate_id: "c-3", status: "withdrawn" },
];

function result(overrides: Partial<{ outcome: string; winners: { candidate_id?: string; candidate_name?: string }[] }> = {}) {
  return {
    outcome: "advanced",
    winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }],
    ...overrides,
  };
}

describe("deriveCandidateResultBadges", () => {
  it("badges the matched winner and the rest as losers when the winner set is exhaustive", () => {
    const badges = deriveCandidateResultBadges([result()], ROSTER);
    expect(badges.get("c-1")).toEqual({ label: "Advanced", kind: "winner" });
    expect(badges.get("c-2")).toEqual({ label: "Did not advance", kind: "loser" });
    // Withdrawn candidates never get a loser badge.
    expect(badges.get("c-3")).toBeUndefined();
  });

  it("uses Won/Lost wording on a won outcome", () => {
    const badges = deriveCandidateResultBadges([result({ outcome: "won" })], ROSTER);
    expect(badges.get("c-1")).toEqual({ label: "Won", kind: "winner" });
    expect(badges.get("c-2")).toEqual({ label: "Lost", kind: "loser" });
  });

  it("marks only who continues in a runoff", () => {
    // A runoff row names who advances and says nothing about who is out.
    const badges = deriveCandidateResultBadges([result({ outcome: "runoff" })], ROSTER);
    expect(badges.get("c-1")).toEqual({ label: "In runoff", kind: "winner" });
    expect(badges.get("c-2")).toBeUndefined();
  });

  it("withholds loser badges on a partial match", () => {
    // One winner matched, the other name-only. That second winner might be a
    // roster candidate the matcher missed — a loser badge on them would be
    // false — so only the confirmed winner is marked.
    const badges = deriveCandidateResultBadges(
      [result({ winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }, { candidate_name: "Sam Writein" }] })],
      ROSTER
    );
    expect(badges.get("c-1")).toEqual({ label: "Advanced", kind: "winner" });
    expect(badges.get("c-2")).toBeUndefined();
  });

  it("badges nobody when a winner id points outside the displayed roster", () => {
    // A stale or filtered-out id (e.g. a withdrawn candidate dropped from the
    // payload) must not flip everyone else to losers.
    const badges = deriveCandidateResultBadges(
      [result({ winners: [{ candidate_id: "c-elsewhere", candidate_name: "Jane Smith" }] })],
      ROSTER
    );
    expect(badges.size).toBe(0);
  });

  it("badges nobody on non-decisive outcomes, empty winner sets, or no results", () => {
    expect(deriveCandidateResultBadges([result({ outcome: "too_close" })], ROSTER).size).toBe(0);
    expect(deriveCandidateResultBadges([result({ winners: [] })], ROSTER).size).toBe(0);
    expect(deriveCandidateResultBadges([], ROSTER).size).toBe(0);
  });

  it("reads only the first (most authoritative) result row", () => {
    // A certified too_close correction sorts first and must suppress the
    // older election-night winner row behind it.
    const badges = deriveCandidateResultBadges(
      [result({ outcome: "too_close", winners: [] }), result({ outcome: "won" })],
      ROSTER
    );
    expect(badges.size).toBe(0);
  });
});
