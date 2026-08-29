import { describe, expect, it } from "vitest";

import type { ElectionChoice } from "./types";
import { myDraftLabel, nearestDayPickProgress } from "./pickProgress";

function choice(partial: Partial<ElectionChoice>): ElectionChoice {
  return {
    election_id: "e1",
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    seats_to_fill: null,
    picks: [],
    measure_position: null,
    updated_at: "2026-08-28T00:00:00Z",
    ...partial,
  };
}

const decided = (electionId: string) =>
  choice({
    election_id: electionId,
    picks: [{ candidate_id: "c1", display_name: "Jane Doe", candidacy_status: "active" }],
  });

describe("myDraftLabel", () => {
  it("stays plain until the first pick, counts up, then earns My Picks ✓", () => {
    expect(myDraftLabel(null)).toBe("My Draft");
    expect(myDraftLabel({ picked: 0, total: 8, complete: false })).toBe("My Draft");
    expect(myDraftLabel({ picked: 3, total: 8, complete: false })).toBe("My Draft 3/8");
    expect(myDraftLabel({ picked: 8, total: 8, complete: true })).toBe("My Picks ✓");
  });
});

describe("nearestDayPickProgress", () => {
  const elections = [
    { id: "nov-1", election_date: "2026-11-03" },
    { id: "nov-2", election_date: "2026-11-03" },
    { id: "sep-1", election_date: "2026-09-15" },
    { id: "past-1", election_date: "2026-08-01" },
  ];

  it("returns null until both inputs settle and when nothing is upcoming", () => {
    const choices = new Map<string, ElectionChoice>();
    expect(nearestDayPickProgress(undefined, choices, "2026-08-28")).toBeNull();
    expect(nearestDayPickProgress(elections, undefined, "2026-08-28")).toBeNull();
    expect(nearestDayPickProgress(elections, choices, "2027-01-01")).toBeNull();
  });

  it("counts only the nearest upcoming day, ignoring past races and other days", () => {
    const choices = new Map<string, ElectionChoice>([
      // Decided on the nearest day...
      ["sep-1", decided("sep-1")],
      // ...and on a later day + a past day, which must not count.
      ["nov-1", decided("nov-1")],
      ["past-1", decided("past-1")],
    ]);
    expect(nearestDayPickProgress(elections, choices, "2026-08-28")).toEqual({
      picked: 1,
      total: 1,
      complete: true,
    });
    // Once September passes, the November group (2 races, 1 decided) leads.
    expect(nearestDayPickProgress(elections, choices, "2026-10-01")).toEqual({
      picked: 1,
      total: 2,
      complete: false,
    });
  });

  it("treats an election day itself as upcoming and an emptied choice as undecided", () => {
    const choices = new Map<string, ElectionChoice>([["sep-1", choice({ election_id: "sep-1" })]]);
    expect(nearestDayPickProgress(elections, choices, "2026-09-15")).toEqual({
      picked: 0,
      total: 1,
      complete: false,
    });
  });
});
