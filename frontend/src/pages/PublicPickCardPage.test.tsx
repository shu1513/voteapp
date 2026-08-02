import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import type { PickCard } from "@voteapp/api-client";
import { PublicPickCardPage } from "./PublicPickCardPage";
import { renderRoutes } from "../test/render";

// The card arrives via the route loader (server-fetched in production);
// tests supply it directly instead of stubbing the loader's fetch — same
// pattern as CandidatePage.test.
function renderCard(card: PickCard) {
  return renderRoutes(
    [{ path: "/picks/:token", loader: () => card, element: <PublicPickCardPage /> }],
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
});
