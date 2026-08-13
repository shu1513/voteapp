import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import type { ElectionChoice, ElectionPreviewCandidate } from "@voteapp/api-client";
import { BallotPreviewSheets } from "./BallotPreview";
import { electionSummary } from "../test/fixtures";

function previewCandidate(overrides: Partial<ElectionPreviewCandidate> = {}): ElectionPreviewCandidate {
  return {
    candidate_election_id: "ce-1",
    candidate_id: "c-1",
    display_name: "Denise M. Porter",
    party: "",
    is_incumbent: true,
    status: "declared",
    ...overrides,
  };
}

function choice(overrides: Partial<ElectionChoice> = {}): ElectionChoice {
  return {
    election_id: "e-1",
    race_type: "office",
    official_ballot_title: "Retention of 4th Judicial District Court Judge Denise M. Porter",
    election_date: "2026-11-03",
    seats_to_fill: null,
    picks: [{ candidate_id: "c-1", display_name: "Denise M. Porter", candidacy_status: "active" }],
    measure_position: null,
    updated_at: "2026-08-01T00:00:00.000Z",
    ...overrides,
  };
}

describe("BallotPreviewSheets retention races", () => {
  // Retention races are stored as office races with the judge as the single
  // candidate, but the paper ballot prints a Yes/No question — the sheet must
  // never show "Vote for One" with an oval beside the judge's name.
  it("renders a retention race as a Yes/No question with the pick on Yes", () => {
    render(
      <BallotPreviewSheets
        elections={[
          electionSummary({
            official_ballot_title: "Retention of 4th Judicial District Court Judge Denise M. Porter",
            preview: { seats_to_fill: null, candidates: [previewCandidate()], measure: null },
          }),
        ]}
        choiceByElectionId={new Map([["e-1", choice()]])}
        today="2026-08-01"
      />
    );

    const contest = screen
      .getByRole("heading", { name: /Retention of 4th Judicial District Court Judge/ })
      .closest("section")!;
    expect(within(contest).getByText("Vote Yes or No")).toBeInTheDocument();
    expect(within(contest).queryByText("Vote for One")).not.toBeInTheDocument();
    expect(within(contest).getByText("Yes")).toBeInTheDocument();
    expect(within(contest).getByText("No")).toBeInTheDocument();
    // Picking the judge means voting to retain: the mark lands on Yes.
    const yesRow = within(contest).getByText("Yes").closest("li")!;
    expect(within(yesRow).getByText("Your pick")).toBeInTheDocument();
    // The judge's name still appears (as ballot-question context, not a row
    // with its own oval).
    expect(within(contest).getByText("Denise M. Porter")).toBeInTheDocument();
  });

  it("leaves both retention ovals unmarked without a pick", () => {
    render(
      <BallotPreviewSheets
        elections={[
          electionSummary({
            official_ballot_title: "Shall Judge Pat Example be retained in office?",
            preview: { seats_to_fill: null, candidates: [previewCandidate()], measure: null },
          }),
        ]}
        choiceByElectionId={undefined}
        today="2026-08-01"
      />
    );

    expect(screen.getByText("Vote Yes or No")).toBeInTheDocument();
    expect(screen.queryByText("Your pick")).not.toBeInTheDocument();
  });
});
