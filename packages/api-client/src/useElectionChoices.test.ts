import { describe, expect, it } from "vitest";

import type { ElectionChoice } from "./types";
import { formatChoiceLabel, isDecidedChoice } from "./useElectionChoices";

function choice(partial: Partial<ElectionChoice>): ElectionChoice {
  return {
    election_id: "e1",
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    seats_to_fill: null,
    picks: [],
    measure_position: null,
    ...partial,
  };
}

describe("isDecidedChoice", () => {
  it("counts a candidate pick or a measure position, but not an emptied row", () => {
    expect(isDecidedChoice(undefined)).toBe(false);
    // A choice row emptied of picks (undo) is undecided again.
    expect(isDecidedChoice(choice({}))).toBe(false);
    expect(
      isDecidedChoice(choice({ picks: [{ candidate_id: "c1", display_name: "Jane Doe", candidacy_status: "active" }] }))
    ).toBe(true);
    expect(isDecidedChoice(choice({ race_type: "ballot_measure", measure_position: "no" }))).toBe(true);
  });
});

describe("formatChoiceLabel", () => {
  it("labels measure positions, single and multi picks, and flags withdrawals", () => {
    expect(formatChoiceLabel(choice({ race_type: "ballot_measure", measure_position: "yes" }))).toBe("My pick: Yes");
    expect(formatChoiceLabel(choice({}))).toBeNull();
    expect(
      formatChoiceLabel(choice({ picks: [{ candidate_id: "c1", display_name: "Jane Doe", candidacy_status: "active" }] }))
    ).toBe("My pick: Jane Doe");
    expect(
      formatChoiceLabel(
        choice({
          picks: [
            { candidate_id: "c1", display_name: "Jane Doe", candidacy_status: "active" },
            { candidate_id: "c2", display_name: "John Roe", candidacy_status: "withdrawn" },
          ],
        })
      )
    ).toBe("My picks: Jane Doe, John Roe (withdrew)");
  });
});
