import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { SavedBallotPage } from "./SavedBallotPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, electionSummary, ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

const VERIFIED_BASE = {
  "/api/me": { body: ME_VERIFIED },
  "/api/me/ballot-preferences": { body: { sort: "vote_power", followed_first: true } },
  "/api/me/research-area-preferences": { body: { preferences: [] } },
};

function renderSavedBallot(state?: unknown) {
  return renderRoutes(
    [
      { path: "/me/ballot", element: <SavedBallotPage /> },
      { path: "/login", element: <p /> },
      { path: "/elections/:electionId", element: <p /> },
    ],
    { pathname: "/me/ballot", state }
  );
}

beforeEach(() => {
  // No pending anonymous-search districts: the handoff must stay quiet.
  localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("SavedBallotPage", () => {
  it("asks logged-out visitors to log in", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderSavedBallot();
    expect(await screen.findByText("Log in to see your saved ballot.")).toBeInTheDocument();
  });

  it("shows the verify interstitial for unverified users", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderSavedBallot();
    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
  });

  it("routes verified users with no saved districts to the address form", async () => {
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": { body: { district_ids: [], districts: [], elections: [] } },
    });
    renderSavedBallot();
    expect(await screen.findByRole("heading", { name: "Set your address" })).toBeInTheDocument();
  });

  it("renders the saved ballot under a date heading, without banner or subtitle", async () => {
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderSavedBallot();

    // The date heading is the page's identity — no "Your saved ballot"
    // banner and no election/district count subtitle.
    expect(
      await screen.findByRole("heading", { name: "Elections on November 3, 2026" })
    ).toBeInTheDocument();
    expect(screen.queryByText("Your saved ballot")).not.toBeInTheDocument();
    expect(screen.queryByText(/election across/)).not.toBeInTheDocument();
    expect(screen.queryByText(/ordered by/)).not.toBeInTheDocument();
    expect(screen.getByText("Governor")).toBeInTheDocument();
    // Address changes live in Settings now; the ballot only links there.
    expect(screen.getByRole("link", { name: "Change your address in Settings" })).toHaveAttribute(
      "href",
      "/me/settings"
    );
    expect(screen.queryByLabelText("New address")).not.toBeInTheDocument();
  });

  it("confirms a fresh address save from router state, then wipes the history state", async () => {
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
    });
    const { router } = renderSavedBallot({
      addressSaved: { matched_address: "123 MAIN ST, AUSTIN, TX", address_match_count: 1 },
    });

    const confirmation = await screen.findByRole("status");
    expect(confirmation).toHaveTextContent("Your election districts are updated from 123 MAIN ST, AUSTIN, TX");
    // The account keeps election districts, not the address — the notice
    // states only what WAS saved instead of announcing "Address saved" or
    // making an absolute no-save claim (the backend's 14-day geocoder cache
    // would falsify one).
    expect(confirmation).toHaveTextContent("Only the new election districts were saved in your profile");
    expect(confirmation).not.toHaveTextContent("Address saved");
    // Exact single match: no ambiguity warning.
    expect(confirmation).not.toHaveTextContent("possible locations");
    // The election list renders beneath the confirmation.
    expect(screen.getByText("Governor")).toBeInTheDocument();

    // Regression: the history entry must not keep the save state, or a
    // refresh/back-forward would replay the banner (and retain the home
    // address) indefinitely — while the banner itself stays visible for
    // this visit.
    await waitFor(() => expect(router.state.location.state).toBeNull());
    expect(screen.getByRole("status")).toBeInTheDocument();
  });

  it("warns when the saved address matched multiple locations", async () => {
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderSavedBallot({
      addressSaved: {
        matched_address: "100 MAIN ST, SPRINGFIELD, MA, 01105",
        address_match_count: 7,
      },
    });

    const confirmation = await screen.findByRole("status");
    expect(confirmation).toHaveTextContent("Your election districts are updated from 100 MAIN ST, SPRINGFIELD, MA, 01105");
    expect(confirmation).toHaveTextContent(
      "Your address matched 7 possible locations and the first one was used"
    );
  });

  it("falls back to the verify interstitial when the ballot 403s", async () => {
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": apiError(403, "forbidden", "Email verification is required"),
    });
    renderSavedBallot();
    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
  });
});
