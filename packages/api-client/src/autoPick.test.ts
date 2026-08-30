import { describe, expect, it } from "vitest";
import { hasClearableAutoPicks, reasonLabel, summarizeAutoPick } from "./autoPick";
import type { AutoPickElectionResult, ElectionChoice } from "./types";

// The full summary/panel copy matrix is exercised by the web control tests
// (AutoPickControl.test.tsx renders these sentences); this file pins the
// pure helpers where they now live.

function choice(overrides: Partial<ElectionChoice>): ElectionChoice {
  return {
    election_id: "e1",
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    seats_to_fill: 1,
    picks: [],
    measure_position: null,
    updated_at: "2026-08-29T00:00:00Z",
    ...overrides,
  };
}

function result(overrides: Partial<AutoPickElectionResult>): AutoPickElectionResult {
  return {
    election_id: "e1",
    race_type: "office",
    outcome: "no_pick",
    reason: null,
    picked_candidate_ids: [],
    measure_position: null,
    shortlist_candidate_ids: [],
    candidates: [],
    measure_per_issue: [],
    unresearched: [],
    ...overrides,
  };
}

describe("reasonLabel", () => {
  it("reads null (no recorded reason) as the evidence gap", () => {
    expect(reasonLabel(null)).toBe("not enough evidence");
    expect(reasonLabel("tie")).toBe("a tie");
    expect(reasonLabel("too_few_issues")).toBe("fewer than 3 ranked issues");
  });
});

describe("hasClearableAutoPicks", () => {
  it("finds auto rows only on the given date", () => {
    const choices = [
      choice({
        election_id: "e1",
        election_date: "2026-11-03",
        picks: [{ candidate_id: "c1", display_name: "Jane", candidacy_status: "active", origin: "auto" }],
      }),
    ];
    expect(hasClearableAutoPicks(choices, "2026-11-03")).toBe(true);
    expect(hasClearableAutoPicks(choices, "2026-12-01")).toBe(false);
  });

  it("ignores manual picks and manual measure positions", () => {
    const choices = [
      choice({
        election_id: "e1",
        picks: [{ candidate_id: "c1", display_name: "Jane", candidacy_status: "active", origin: "manual" }],
      }),
      choice({
        election_id: "e2",
        race_type: "ballot_measure",
        measure_position: "yes",
        measure_origin: "manual",
      }),
    ];
    expect(hasClearableAutoPicks(choices, "2026-11-03")).toBe(false);
  });

  it("counts an auto measure position", () => {
    const choices = [
      choice({
        election_id: "e2",
        race_type: "ballot_measure",
        measure_position: "no",
        measure_origin: "auto",
      }),
    ];
    expect(hasClearableAutoPicks(choices, "2026-11-03")).toBe(true);
  });
});

describe("summarizeAutoPick", () => {
  const reports = [
    { candidate_id: "c1", display_name: "Jane Doe", score: 1, has_evidence: true, vetoed_by: [], per_issue: [] },
    { candidate_id: "c2", display_name: "John Roe", score: 0, has_evidence: false, vetoed_by: [], per_issue: [] },
  ];

  it("names the winner", () => {
    const summary = summarizeAutoPick(
      result({ outcome: "picked", picked_candidate_ids: ["c1"], candidates: reports }),
      1
    );
    expect(summary).toBe("Picked Jane Doe — the best match for your issues.");
  });

  it("flags open seats on a partial multi-seat fill", () => {
    const summary = summarizeAutoPick(
      result({ outcome: "picked", picked_candidate_ids: ["c1"], candidates: reports }),
      3
    );
    expect(summary).toContain("Picked Jane Doe");
    expect(summary).toContain("2 seats are still open");
  });

  it("puts the couldn't-run reasons before the measure fork", () => {
    const summary = summarizeAutoPick(
      result({ race_type: "ballot_measure", reason: "too_few_issues" }),
      null
    );
    expect(summary).toBe("Rank at least 3 issues first, so the pick reflects what matters to you.");
  });

  it("tells the two measure no-answer cases apart", () => {
    expect(summarizeAutoPick(result({ race_type: "ballot_measure" }), null)).toBe(
      "No answer — this measure isn't tagged with any of your issues yet."
    );
    expect(
      summarizeAutoPick(
        result({
          race_type: "ballot_measure",
          measure_per_issue: [{ research_area_id: "a1", net: 1 }],
        }),
        null
      )
    ).toBe("No answer — this measure helps some of your issues and hurts others about equally, so it's your call.");
  });

  it("names the shortlist on a tie", () => {
    const summary = summarizeAutoPick(
      result({ reason: "tie", shortlist_candidate_ids: ["c1", "c2"], candidates: reports }),
      1
    );
    expect(summary).toBe("It's a tie between Jane Doe and John Roe on your issues — your call between them.");
  });
});
