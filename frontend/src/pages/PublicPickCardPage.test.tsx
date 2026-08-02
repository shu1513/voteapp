import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PickCard } from "@voteapp/api-client";
import { ErrorBoundary, PublicPickCardPage } from "./PublicPickCardPage";
import { renderRoutes } from "../test/render";

// The subject arrives via the route loader (server-fetched in production);
// tests supply it directly instead of stubbing the loader's fetch.
function renderCard(card: PickCard, token = "tok-1") {
  return renderRoutes(
    [
      {
        path: "/picks/:token",
        element: <PublicPickCardPage />,
        errorElement: <ErrorBoundary />,
        hydrateFallbackElement: <p />,
        loader: () => card,
      },
      { path: "/elections/:electionId", element: <p>Election page</p> },
      { path: "/candidates/:candidateId", element: <p>Candidate page</p> },
    ],
    "/picks/tok-1".replace("tok-1", token)
  );
}

function pickCard(): PickCard {
  return {
    election_date: "2026-11-03",
    entries: [
      {
        election_id: "e-1",
        official_ballot_title: "Governor",
        race_type: "office",
        district_name: "Alaska",
        picks: [{ candidate_id: "c-1", display_name: "Jordan Voter", candidacy_status: "declared" }],
        measure_position: null,
      },
    ],
  };
}

describe("PublicPickCardPage nav context", () => {
  const SHARED_STATE = { backTo: { path: "/picks/tok-1", label: "Shared picks" } };

  it("hands election links the card as their back destination", async () => {
    const user = userEvent.setup();
    const { router } = renderCard(pickCard());

    await user.click(await screen.findByRole("link", { name: "Governor" }));

    expect(router.state.location.pathname).toBe("/elections/e-1");
    expect(router.state.location.state).toEqual(SHARED_STATE);
  });

  it("hands candidate links the card as their back destination", async () => {
    const user = userEvent.setup();
    const { router } = renderCard(pickCard());

    await user.click(await screen.findByRole("link", { name: "Jordan Voter" }));

    expect(router.state.location.pathname).toBe("/candidates/c-1");
    expect(router.state.location.state).toEqual(SHARED_STATE);
  });
});
