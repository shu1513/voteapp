import { describe, expect, it } from "vitest";
import { pagerNeighbors, readCandidateNavState, readElectionNavState } from "./detailNavContext";

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

  it("keeps a nested candidate backState and drops a malformed one alone", () => {
    const nested = { backTo: { path: "/candidates/c-1", label: "Jordan Voter" } };
    expect(readElectionNavState({ backTo: BACK_TO, backState: nested })).toEqual({
      backTo: BACK_TO,
      backState: nested,
    });
    expect(readElectionNavState({ backTo: BACK_TO, backState: { backTo: null } })).toEqual({
      backTo: BACK_TO,
    });
  });

  it("keeps the back link when only the contests list is malformed", () => {
    expect(readElectionNavState({ backTo: BACK_TO, contests: "not-a-list" })).toEqual({ backTo: BACK_TO });
    expect(
      readElectionNavState({ backTo: BACK_TO, contests: [{ id: "e-1", title: "Governor" }, { id: 7 }] })
    ).toEqual({ backTo: BACK_TO });
  });

  it("round-trips contest race_type and the engaged raceType tab", () => {
    const state = {
      backTo: BACK_TO,
      contests: [
        { id: "e-1", title: "Governor", race_type: "office" },
        { id: "q-1", title: "Measure A", race_type: "ballot_measure" },
      ],
      raceType: "ballot_measure",
    };
    expect(readElectionNavState(state)).toEqual(state);
  });

  it("drops an invalid race_type per entry and an invalid raceType alone", () => {
    // An unknown race_type withholds only that entry's field (disabling the
    // tabs, which need every entry typed) — never the entry or the list.
    expect(
      readElectionNavState({
        backTo: BACK_TO,
        contests: [
          { id: "e-1", title: "Governor", race_type: "banana" },
          { id: "q-1", title: "Measure A", race_type: "ballot_measure" },
        ],
        raceType: "banana",
      })
    ).toEqual({
      backTo: BACK_TO,
      contests: [
        { id: "e-1", title: "Governor" },
        { id: "q-1", title: "Measure A", race_type: "ballot_measure" },
      ],
    });
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

describe("pagerNeighbors", () => {
  const LIST = [
    { id: "e-1", title: "Governor" },
    { id: "e-2", title: "Mayor" },
    { id: "e-3", title: "Sheriff" },
  ];

  it("returns both neighbors for a middle entry and null slots at the ends", () => {
    expect(pagerNeighbors(LIST, "e-2")).toEqual({ prev: LIST[0], next: LIST[2] });
    expect(pagerNeighbors(LIST, "e-1")).toEqual({ prev: null, next: LIST[1] });
    expect(pagerNeighbors(LIST, "e-3")).toEqual({ prev: LIST[1], next: null });
  });

  it("returns null with no list, a short list, or the current id missing", () => {
    expect(pagerNeighbors(undefined, "e-1")).toBeNull();
    expect(pagerNeighbors([LIST[0]], "e-1")).toBeNull();
    // A stale snapshot that no longer contains the page must not page.
    expect(pagerNeighbors(LIST, "e-999")).toBeNull();
  });
});
