import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PickCard, PickCardEntry } from "@voteapp/api-client";
import { PublicPickCardPage } from "./PublicPickCardPage";
import { renderRoutes } from "../test/render";

// The card arrives via the route loader (server-fetched in production);
// tests supply it directly instead of stubbing the loader's fetch — same
// pattern as CandidatePage.test.
function renderCard(card: PickCard) {
  return renderRoutes(
    [
      { path: "/picks/:token", loader: () => card, element: <PublicPickCardPage /> },
      // Landing routes for the nav-context tests' click-throughs.
      { path: "/elections/:electionId", element: <p>Election page</p> },
      { path: "/candidates/:candidateId", element: <p>Candidate page</p> },
    ],
    "/picks/tok_abcdefghijklmnopqrstuvwxyz012345"
  );
}

function pickCard(overrides: Partial<PickCard> = {}): PickCard {
  return {
    first_name: "Ava",
    election_date: "2026-11-03",
    entries: [
      {
        election_id: "e-1",
        official_ballot_title: "Mayor",
        race_type: "office",
        district_name: "Springfield",
        picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "declared" }],
        measure_position: null,
      },
    ],
    ...overrides,
  };
}

describe("PublicPickCardPage", () => {
  it("names the card's owner in the heading", async () => {
    renderCard(pickCard());
    expect(
      await screen.findByRole("heading", { name: "Ava's choices for November 3, 2026 elections" })
    ).toBeInTheDocument();
    expect(screen.getByText("Jane Smith")).toBeInTheDocument();
  });

  it("falls back to the unnamed heading when the payload has no first name", async () => {
    // Deploy skew: a not-yet-redeployed backend omits first_name; the card
    // must render unnamed, not crash or say "undefined's choices".
    renderCard(pickCard({ first_name: undefined }));
    expect(
      await screen.findByRole("heading", { name: "Election Picks for November 3, 2026" })
    ).toBeInTheDocument();
  });

  it("keeps a legacy anonymous share unnamed (null first name)", async () => {
    // Shares minted before the named page existed return first_name: null
    // (show_owner_name=false) — they were posted under an anonymous page
    // and must stay that way until the owner re-shares.
    renderCard(pickCard({ first_name: null }));
    expect(
      await screen.findByRole("heading", { name: "Election Picks for November 3, 2026" })
    ).toBeInTheDocument();
  });

  it("marks a measure pick with its outcome once certified results land", async () => {
    renderCard(pickCard({ entries: [measureEntry({ measure_position: "yes", measure_result: "passed" })] }));
    expect(await screen.findByText("Yes on this measure")).toBeInTheDocument();
    // Chip word states the fact; green styling means it matched the pick.
    expect(screen.getByText("Passed").className).toContain("bg-green-700");
  });

  it("mutes the outcome chip when the measure went against the pick", async () => {
    renderCard(pickCard({ entries: [measureEntry({ measure_position: "yes", measure_result: "failed" })] }));
    expect(await screen.findByText("Failed")).toBeInTheDocument();
    // The exact muted style, not just "not green": a broken class would
    // otherwise pass.
    expect(screen.getByText("Failed").className).toContain("bg-surface");
  });

  it("shows no outcome chip before a canonical result exists", async () => {
    // Covers both a null result and a pre-field backend (deploy skew).
    renderCard(pickCard({ entries: [measureEntry({ measure_position: "no", measure_result: null })] }));
    expect(await screen.findByText("No on this measure")).toBeInTheDocument();
    expect(screen.queryByText("Passed")).not.toBeInTheDocument();
    expect(screen.queryByText("Failed")).not.toBeInTheDocument();
  });

  it("flags a pick among the canonical result's winners before certification", async () => {
    // Election-night calls land as result rows long before candidacy_status
    // flips — the recipient should see "Advanced" the same day the owner
    // shares the card.
    renderCard(
      pickCard({
        entries: [
          officeEntry({
            current_result_outcome: "advanced",
            current_result_winners: [
              { candidate_id: "c-1", candidate_name: "Jane Smith", party: "Democratic" },
              { candidate_id: "c-2", candidate_name: "John James", party: "Republican" },
            ],
          }),
        ],
      })
    );
    expect(await screen.findByText("Advanced")).toBeInTheDocument();
    expect(screen.getByText("Advanced").className).toContain("bg-green-700");
  });

  it("stays silent on a pick that missed the winners", async () => {
    renderCard(
      pickCard({
        entries: [
          officeEntry({
            current_result_outcome: "advanced",
            current_result_winners: [{ candidate_id: "c-2", candidate_name: "John James" }],
          }),
        ],
      })
    );
    expect(await screen.findByText("Jane Smith")).toBeInTheDocument();
    expect(screen.queryByText("Advanced")).not.toBeInTheDocument();
    expect(screen.queryByText("Lost")).not.toBeInTheDocument();
  });

  it("shows a certified advanced/runoff candidacy without needing a result row", async () => {
    // The certified writer projects advanced/runoff onto candidacy_status;
    // the chip must not depend on the result-row fallback.
    renderCard(
      pickCard({
        entries: [
          officeEntry({
            picks: [
              { candidate_id: "c-1", display_name: "Ada Advancer", candidacy_status: "advanced" },
              { candidate_id: "c-3", display_name: "Rae Runoff", candidacy_status: "runoff" },
            ],
          }),
        ],
      })
    );
    expect(await screen.findByText("Advanced")).toBeInTheDocument();
    expect(screen.getByText("In runoff")).toBeInTheDocument();
  });

  it("never doubles a certified candidacy chip with the result-derived one", async () => {
    renderCard(
      pickCard({
        entries: [
          officeEntry({
            picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "won" }],
            current_result_outcome: "won",
            current_result_winners: [{ candidate_id: "c-1", candidate_name: "Jane Smith" }],
          }),
        ],
      })
    );
    expect(await screen.findByText("Jane Smith")).toBeInTheDocument();
    expect(screen.getAllByText("Won")).toHaveLength(1);
  });

  it("marks a measure pick from the canonical result before the certified field lands", async () => {
    renderCard(
      pickCard({
        entries: [
          measureEntry({ measure_position: "yes", measure_result: null, current_result_outcome: "passed" }),
        ],
      })
    );
    expect(await screen.findByText("Passed")).toBeInTheDocument();
    expect(screen.getByText("Passed").className).toContain("bg-green-700");
  });
});

describe("PublicPickCardPage nav context", () => {
  const SHARED_STATE = {
    backTo: { path: "/picks/tok_abcdefghijklmnopqrstuvwxyz012345", label: "Shared picks" },
  };

  it("hands election links the card as their back destination", async () => {
    const user = userEvent.setup();
    const { router } = renderCard(pickCard());

    await user.click(await screen.findByRole("link", { name: "Mayor" }));

    expect(router.state.location.pathname).toBe("/elections/e-1");
    expect(router.state.location.state).toEqual(SHARED_STATE);
  });

  it("hands candidate links the card as their back destination", async () => {
    const user = userEvent.setup();
    const { router } = renderCard(pickCard());

    await user.click(await screen.findByRole("link", { name: "Jane Smith" }));

    expect(router.state.location.pathname).toBe("/candidates/c-1");
    expect(router.state.location.state).toEqual(SHARED_STATE);
  });
});

function officeEntry(overrides: Partial<PickCardEntry> = {}): PickCardEntry {
  return {
    election_id: "e-1",
    official_ballot_title: "Governor",
    race_type: "office",
    district_name: "Springfield",
    picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "declared" }],
    measure_position: null,
    measure_result: null,
    ...overrides,
  };
}

function measureEntry(overrides: Partial<PickCardEntry> = {}): PickCardEntry {
  return {
    election_id: "e-2",
    official_ballot_title: "Proposition 5",
    race_type: "ballot_measure",
    district_name: "Springfield",
    picks: [],
    measure_position: "yes",
    measure_result: null,
    ...overrides,
  };
}
