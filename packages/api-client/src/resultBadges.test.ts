import { describe, expect, it } from "vitest";
import { deriveCandidateResultBadges } from "./resultBadges";

const ROSTER = [
  { candidate_id: "c-1", status: "declared", party: "Democratic" },
  { candidate_id: "c-2", status: "declared", party: "Democratic" },
  { candidate_id: "c-3", status: "withdrawn", party: "Democratic" },
  { candidate_id: "c-4", status: "declared", party: "Republican" },
  { candidate_id: "c-5", status: "declared", party: "Republican" },
];

function result(overrides: Partial<{ outcome: string; winners: { candidate_id?: string; candidate_name?: string }[] }> = {}) {
  return {
    outcome: "advanced",
    winners: [
      { candidate_id: "c-1", candidate_name: "Jane Smith" },
      { candidate_id: "c-4", candidate_name: "John Roe" },
    ],
    ...overrides,
  };
}

describe("deriveCandidateResultBadges", () => {
  it("badges advancers and, per called party, who did not advance", () => {
    const badges = deriveCandidateResultBadges([result()], ROSTER, null);
    expect(badges.get("c-1")).toEqual({ label: "Advanced", kind: "winner" });
    expect(badges.get("c-4")).toEqual({ label: "Advanced", kind: "winner" });
    expect(badges.get("c-2")).toEqual({ label: "Did not advance", kind: "loser" });
    expect(badges.get("c-5")).toEqual({ label: "Did not advance", kind: "loser" });
    // Withdrawn candidates never get a loser badge.
    expect(badges.get("c-3")).toBeUndefined();
  });

  it("spares the uncalled party on a partial election-night call", () => {
    // Only the Democratic race is called; the Republican race is still
    // counting. Republican candidates must not read "Did not advance".
    const badges = deriveCandidateResultBadges(
      [result({ winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }] })],
      ROSTER,
      null
    );
    expect(badges.get("c-1")).toEqual({ label: "Advanced", kind: "winner" });
    expect(badges.get("c-2")).toEqual({ label: "Did not advance", kind: "loser" });
    expect(badges.get("c-4")).toBeUndefined();
    expect(badges.get("c-5")).toBeUndefined();
  });

  it("uses Won/Lost wording when the winners fill the seats", () => {
    const badges = deriveCandidateResultBadges(
      [result({ outcome: "won", winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }] })],
      ROSTER,
      null
    );
    expect(badges.get("c-1")).toEqual({ label: "Won", kind: "winner" });
    // A won race with the seat count satisfied is decided for everyone.
    expect(badges.get("c-2")).toEqual({ label: "Lost", kind: "loser" });
    expect(badges.get("c-4")).toEqual({ label: "Lost", kind: "loser" });
  });

  it("withholds Lost badges when a multi-seat row lists fewer winners than seats", () => {
    // Two seats, one recorded winner: the second winner is missing from the
    // payload, so nobody else can be called a loser yet.
    const badges = deriveCandidateResultBadges(
      [result({ outcome: "won", winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }] })],
      ROSTER,
      2
    );
    expect(badges.get("c-1")).toEqual({ label: "Won", kind: "winner" });
    expect(badges.get("c-2")).toBeUndefined();
    expect(badges.get("c-4")).toBeUndefined();
  });

  it("marks only who continues in a runoff", () => {
    // A runoff row names who advances and says nothing about who is out.
    const badges = deriveCandidateResultBadges(
      [result({ outcome: "runoff", winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }] })],
      ROSTER,
      null
    );
    expect(badges.get("c-1")).toEqual({ label: "In runoff", kind: "winner" });
    expect(badges.get("c-2")).toBeUndefined();
  });

  it("withholds loser badges entirely on a partial id-match", () => {
    // One winner matched, the other name-only. That second winner might be a
    // roster candidate the matcher missed — a loser badge anywhere would risk
    // marking the actual winner.
    const badges = deriveCandidateResultBadges(
      [result({ winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }, { candidate_name: "Sam Writein" }] })],
      ROSTER,
      null
    );
    expect(badges.get("c-1")).toEqual({ label: "Advanced", kind: "winner" });
    expect(badges.size).toBe(1);
  });

  it("badges nobody when a winner id points outside the displayed roster", () => {
    // A stale or filtered-out id (e.g. a withdrawn candidate dropped from the
    // payload) must not flip everyone else to losers.
    const badges = deriveCandidateResultBadges(
      [result({ winners: [{ candidate_id: "c-elsewhere", candidate_name: "Jane Smith" }] })],
      ROSTER,
      null
    );
    expect(badges.size).toBe(0);
  });

  it("badges nobody on non-decisive outcomes, empty winner sets, or no results", () => {
    expect(deriveCandidateResultBadges([result({ outcome: "too_close" })], ROSTER, null).size).toBe(0);
    expect(deriveCandidateResultBadges([result({ winners: [] })], ROSTER, null).size).toBe(0);
    expect(deriveCandidateResultBadges([], ROSTER, null).size).toBe(0);
  });

  it("reads only the first (most authoritative) result row", () => {
    // A certified too_close correction sorts first and must suppress the
    // older election-night winner row behind it.
    const badges = deriveCandidateResultBadges(
      [result({ outcome: "too_close", winners: [] }), result({ outcome: "won" })],
      ROSTER,
      null
    );
    expect(badges.size).toBe(0);
  });

  it("compares party labels case-insensitively", () => {
    const roster = [
      { candidate_id: "c-1", status: "declared", party: "Democratic" },
      { candidate_id: "c-2", status: "declared", party: "democratic" },
    ];
    const badges = deriveCandidateResultBadges(
      [result({ winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }] })],
      roster,
      null
    );
    expect(badges.get("c-2")).toEqual({ label: "Did not advance", kind: "loser" });
  });
});
