import { describe, expect, it } from "vitest";
import { readCandidateNavState, readElectionNavState } from "./detailNavContext";

const BACK_TO = { path: "/ballot?d=d-1&sort=soonest", label: "All elections" };

describe("readElectionNavState", () => {
  it("passes a full valid state through", () => {
    const state = { backTo: BACK_TO, contests: [{ id: "e-1", title: "Governor" }] };
    expect(readElectionNavState(state)).toEqual(state);
  });

  it("returns null for junk, null, and missing backTo", () => {
    expect(readElectionNavState(null)).toBeNull();
    expect(readElectionNavState("nonsense")).toBeNull();
    expect(readElectionNavState({})).toBeNull();
    expect(readElectionNavState({ backTo: { path: "/ballot" } })).toBeNull();
    expect(readElectionNavState({ backTo: { path: "/ballot", label: "  " } })).toBeNull();
  });

  it("rejects non-internal back paths", () => {
    expect(readElectionNavState({ backTo: { path: "https://evil.example", label: "x" } })).toBeNull();
    expect(readElectionNavState({ backTo: { path: "//evil.example", label: "x" } })).toBeNull();
  });

  it("keeps the back link when only the contests list is malformed", () => {
    expect(readElectionNavState({ backTo: BACK_TO, contests: "not-a-list" })).toEqual({ backTo: BACK_TO });
    expect(
      readElectionNavState({ backTo: BACK_TO, contests: [{ id: "e-1", title: "Governor" }, { id: 7 }] })
    ).toEqual({ backTo: BACK_TO });
  });

  it("treats whitespace-only contest ids and titles as malformed", () => {
    // A whitespace-only id would build a broken href; a whitespace-only
    // title would render an invisible pager link.
    expect(
      readElectionNavState({ backTo: BACK_TO, contests: [{ id: "   ", title: "Governor" }] })
    ).toEqual({ backTo: BACK_TO });
    expect(
      readElectionNavState({ backTo: BACK_TO, contests: [{ id: "e-1", title: "   " }] })
    ).toEqual({ backTo: BACK_TO });
    expect(readElectionNavState({ backTo: BACK_TO, contests: [{ id: "e-1", title: "" }] })).toEqual({
      backTo: BACK_TO,
    });
  });
});

describe("readCandidateNavState", () => {
  const ELECTION_BACK = { path: "/elections/e-1", label: "Governor" };

  it("passes a full valid state through", () => {
    const state = {
      backTo: ELECTION_BACK,
      backState: { backTo: BACK_TO, contests: [{ id: "e-1", title: "Governor" }] },
      electionId: "e-1",
      candidates: [{ id: "c-1", name: "Jordan Voter" }],
    };
    expect(readCandidateNavState(state)).toEqual(state);
  });

  it("returns null only when backTo is unusable", () => {
    expect(readCandidateNavState(undefined)).toBeNull();
    expect(readCandidateNavState({ electionId: "e-1" })).toBeNull();
  });

  it("degrades each optional field independently", () => {
    expect(
      readCandidateNavState({
        backTo: ELECTION_BACK,
        backState: { backTo: { path: "not-internal", label: "x" } },
        electionId: 42,
        candidates: [{ id: "c-1" }],
      })
    ).toEqual({ backTo: ELECTION_BACK });
  });

  it("treats whitespace-only candidate ids and names as malformed", () => {
    expect(
      readCandidateNavState({ backTo: ELECTION_BACK, candidates: [{ id: " ", name: "Jordan Voter" }] })
    ).toEqual({ backTo: ELECTION_BACK });
    expect(
      readCandidateNavState({ backTo: ELECTION_BACK, candidates: [{ id: "c-1", name: " " }] })
    ).toEqual({ backTo: ELECTION_BACK });
  });
});
