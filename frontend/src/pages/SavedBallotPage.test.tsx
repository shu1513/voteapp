import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { Outlet } from "react-router";
import userEvent from "@testing-library/user-event";
import { SavedBallotPage } from "./SavedBallotPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, electionSummary, ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";
import { readPendingDistrictIds, savePendingDistrictIds } from "../lib/pendingDistricts";
import { resetDistrictHandoffForTests, useDistrictHandoffRunner } from "../lib/districtHandoff";

const VERIFIED_BASE = {
  "/api/me": { body: ME_VERIFIED },
  "/api/me/ballot-preferences": { body: { sort: "vote_power", followed_first: true } },
  "/api/me/research-area-preferences": { body: { preferences: [] } },
};

// The district handoff runs from App, not the page: mirror that so the page
// under test reads the real global run instead of one it would start itself.
function HandoffLayout() {
  useDistrictHandoffRunner();
  return <Outlet />;
}

function renderSavedBallot(state?: unknown, search?: string) {
  return renderRoutes(
    [
      {
        element: <HandoffLayout />,
        children: [
          { path: "/me/ballot", element: <SavedBallotPage /> },
          { path: "/login", element: <p /> },
          { path: "/elections/:electionId", element: <p /> },
        ],
      },
    ],
    { pathname: "/me/ballot", search, state }
  );
}

beforeEach(() => {
  // No pending anonymous-search districts: the handoff must stay quiet.
  localStorage.clear();
  sessionStorage.clear();
  resetDistrictHandoffForTests();
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

  it("runs the anonymous-search handoff exactly once, then shows the ballot", async () => {
    savePendingDistrictIds(["district-1", "district-2"]);
    const initializeCalls: unknown[] = [];
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/districts/initialize": (_url, init) => {
        initializeCalls.push(JSON.parse(String(init?.body)));
        return { body: { ok: true } };
      },
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderSavedBallot();

    expect(
      await screen.findByRole("heading", { name: "Elections on November 3, 2026" })
    ).toBeInTheDocument();
    expect(initializeCalls).toEqual([{ district_ids: ["district-1", "district-2"] }]);
    // The queue is consumed: a reload must not re-run the handoff.
    expect(readPendingDistrictIds()).toEqual([]);
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

    // A visible "My elections:" h1 (same wording as the public ballot page)
    // sits above the date group headings. No election/district count
    // subtitle.
    expect(
      await screen.findByRole("heading", { name: "Elections on November 3, 2026" })
    ).toBeInTheDocument();
    const h1 = screen.getByRole("heading", { level: 1, name: "My elections:" });
    expect(h1).not.toHaveClass("sr-only");
    expect(h1).toHaveClass("text-title");
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

  it("offers the official how-to-vote resources for the saved ballot's state", async () => {
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
      "/api/state-resources": {
        body: {
          state_resources: {
            state_abbreviation: "AK",
            state_name: "Alaska",
            polling_place_url: "https://myvoterinformation.alaska.gov",
            mail_voting_available: true,
            mail_ballot_request_url: "https://absenteeballotapplication.alaska.gov",
            mail_ballot_request_type: "online_portal",
            mail_ballot_request_deadline_rule:
              "Applications to receive an absentee ballot by mail must be received at least 10 days before the election.",
          },
        },
      },
    });
    const user = userEvent.setup();
    renderSavedBallot();

    // Signed-in voters land here (their home redirects to the saved ballot),
    // so the how-to-vote disclosure must exist on this page too.
    const toggle = await screen.findByRole("button", { name: "How to vote in AK" });
    await user.click(toggle);
    const requestLink = await screen.findByRole("link", { name: "Request your ballot online" });
    const pollingLink = screen.getByRole("link", { name: "Find your polling place" });
    expect(requestLink).toHaveAttribute("href", "https://absenteeballotapplication.alaska.gov");
    expect(pollingLink).toHaveAttribute("href", "https://myvoterinformation.alaska.gov");
    // Mail first, in-person second.
    expect(requestLink.compareDocumentPosition(pollingLink) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
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
    expect(confirmation).toHaveTextContent(
      "Only the new election districts were saved in your profile — not your address",
    );
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

  describe("ballot filters", () => {
    const HOUSING = { id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null };
    const SAVED_HOUSING = {
      ...VERIFIED_BASE,
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null, rank: 1 },
          ],
        },
      },
    };

    it("offers only the Order section to a viewer with no saved areas", async () => {
      stubApiRoutes({
        ...VERIFIED_BASE,
        "/api/me/ballot": {
          body: ballotSummary([electionSummary(), electionSummary({ id: "e-2", official_ballot_title: "State Senate" })]),
        },
      });
      const user = userEvent.setup();
      renderSavedBallot();
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      // The disclosure itself always renders here — signed-in viewers
      // always have the persisted followed-first preference — but with no
      // saved areas and a short ballot it holds no filter toggles.
      await user.click(screen.getByRole("button", { name: "Filters" }));
      expect(await screen.findByRole("checkbox", { name: "Followed candidates first" })).toBeInTheDocument();
      expect(screen.queryByRole("checkbox", { name: "Affects my issues" })).not.toBeInTheDocument();
      expect(screen.queryByRole("checkbox", { name: "High or above" })).not.toBeInTheDocument();
      expect(screen.queryByRole("checkbox", { name: "Normal or above" })).not.toBeInTheDocument();
    });

    it("filters to matching races with a hidden count, and Show all restores", async () => {
      stubApiRoutes({
        ...SAVED_HOUSING,
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({ research_areas: [HOUSING] }),
            electionSummary({ id: "e-2", official_ballot_title: "State Senate" }),
          ]),
        },
      });
      const user = userEvent.setup();
      const { router } = renderSavedBallot();

      await user.click(await screen.findByRole("button", { name: "Filters" }));
      await user.click(await screen.findByRole("checkbox", { name: "Affects my issues" }));
      expect(screen.getByText("Governor")).toBeInTheDocument();
      expect(screen.queryByText("State Senate")).not.toBeInTheDocument();
      expect(screen.getByText(/1 election hidden/)).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Filters · 1" })).toBeInTheDocument();
      // URL state like the anonymous ballot's sort — deliberately not an
      // account preference, so hiding races never silently persists.
      expect(router.state.location.search).toContain("issues=mine");

      await user.click(screen.getByRole("button", { name: "Show all" }));
      expect(screen.getByText("State Senate")).toBeInTheDocument();
      expect(router.state.location.search).not.toContain("issues=mine");
    });

    it("saves the followed-first preference from the Order section", async () => {
      const putBodies: unknown[] = [];
      stubApiRoutes({
        ...VERIFIED_BASE,
        "/api/me/ballot-preferences": (_url, init) => {
          if (init?.method === "PUT") {
            const body = JSON.parse(String(init.body));
            putBodies.push(body);
            return { body };
          }
          return { body: { sort: "vote_power", followed_first: true } };
        },
        "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
      });
      const user = userEvent.setup();
      renderSavedBallot();

      // The checkbox moved inside the disclosure but keeps its persisted
      // full-object PUT semantics.
      await user.click(await screen.findByRole("button", { name: "Filters" }));
      await user.click(await screen.findByRole("checkbox", { name: "Followed candidates first" }));
      await waitFor(() => expect(putBodies).toHaveLength(1));
      expect(putBodies[0]).toEqual({ sort: "vote_power", followed_first: false });
    });

    it("shows a ballot error instead of the withhold notice while saved areas still load", async () => {
      // A ballot error has no list to withhold: it must win over the
      // saved-areas gate instead of hiding behind the loading notice.
      const neverSettles = new Promise<never>(() => {});
      stubApiRoutes({
        ...VERIFIED_BASE,
        "/api/me/research-area-preferences": () => neverSettles,
        "/api/me/ballot": apiError(500, "internal_error", "boom"),
      });
      renderSavedBallot(undefined, "?issues=mine");

      expect(
        await screen.findByText("The service is having trouble right now. Please try again shortly.")
      ).toBeInTheDocument();
      expect(screen.queryByText("Loading your ballot…")).not.toBeInTheDocument();
    });

    it("fails open to the full list when the saved-areas fetch fails on a ?issues=mine load", async () => {
      stubApiRoutes({
        ...VERIFIED_BASE,
        "/api/me/research-area-preferences": apiError(500, "internal_error", "boom"),
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({ research_areas: [HOUSING] }),
            electionSummary({ id: "e-2", official_ballot_title: "State Senate" }),
          ]),
        },
      });
      renderSavedBallot(undefined, "?issues=mine");

      // Deliberate fail-open, not a spinner: a ballot app errs toward
      // showing races, and no on-page element claims filtering here — the
      // request is ignored and the toggle is not offered (the disclosure
      // itself stays, for the Order section).
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      expect(screen.getByText("State Senate")).toBeInTheDocument();
      const user = userEvent.setup();
      await user.click(screen.getByRole("button", { name: "Filters" }));
      expect(screen.queryByRole("checkbox", { name: "Affects my issues" })).not.toBeInTheDocument();
    });
  });
});

describe("SavedBallotPage sort override", () => {
  it("applies ?sort= to the ballot request and clears it only after a saved change", async () => {
    const putBodies: unknown[] = [];
    const ballotUrls: string[] = [];
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": (url) => {
        ballotUrls.push(String(url));
        return { body: ballotSummary([electionSummary()]) };
      },
      "/api/me/ballot-preferences": (_url, init) => {
        if (init?.method === "PUT") {
          const body = JSON.parse(String(init.body));
          putBodies.push(body);
          return { body };
        }
        return { body: { sort: "vote_power", followed_first: true } };
      },
    });
    const user = userEvent.setup();
    const { router } = renderSavedBallot(undefined, "?sort=soonest");

    await screen.findByText("Governor");
    // The override reaches the API and the select shows the order the list
    // is actually in — the override, not the saved preference.
    expect(ballotUrls[0]).toContain("sort=soonest");
    const select = await screen.findByRole("combobox");
    expect(select).toHaveValue("soonest");

    // Choosing a sort saves the preference FIRST; the override clears only
    // after the PUT succeeds, so the refetch can only see the new order.
    await user.selectOptions(select, "district_size");
    expect(putBodies).toEqual([{ sort: "district_size", followed_first: true }]);
    await waitFor(() => expect(router.state.location.search).not.toContain("sort="));
    // The post-save fetch runs without the stale override.
    await waitFor(() =>
      expect(ballotUrls.some((url) => !url.includes("sort="))).toBe(true)
    );
  });

  it("keeps the override and shows the error when the save fails", async () => {
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
      "/api/me/ballot-preferences": (_url, init) =>
        init?.method === "PUT"
          ? apiError(500, "internal_error", "boom")
          : { body: { sort: "vote_power", followed_first: true } },
    });
    const user = userEvent.setup();
    const { router } = renderSavedBallot(undefined, "?sort=soonest");

    await screen.findByText("Governor");
    await user.selectOptions(await screen.findByRole("combobox"), "district_size");

    // Failed PUT: the override still describes the list on screen, and the
    // select falls back to it beside the error notice.
    expect(
      await screen.findByText("The service is having trouble right now. Please try again shortly.")
    ).toBeInTheDocument();
    expect(router.state.location.search).toContain("sort=soonest");
    expect(screen.getByRole("combobox")).toHaveValue("soonest");
  });
});

describe("SavedBallotPage nav context", () => {
  it("hands election cards its own URL including active filter params", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
    });
    const { router } = renderSavedBallot(undefined, "?issues=mine");

    await user.click(await screen.findByRole("link", { name: /Governor/ }));

    expect(router.state.location.pathname).toBe("/elections/e-1");
    expect(router.state.location.state).toEqual({
      backTo: { path: "/me/ballot?issues=mine", label: "My Elections" },
      contests: [
        {
          id: "e-1",
          title: "Governor",
          race_type: "office",
          vote_power_score: 42,
          election_date: "2026-11-03",
          research_area_ids: [],
        },
      ],
      // The saved preference (vote_power) seeds the rail's sort.
      railSort: "vote_power",
    });
  });

  it("withholds the list until the saved sort is known, then seeds the rail with it", async () => {
    const user = userEvent.setup();
    // The ballot lands first (already server-ordered by the saved soonest
    // preference); the preferences response is held so the client cannot yet
    // know which sort that was.
    let releasePreferences!: (value: { body: { sort: string; followed_first: boolean } }) => void;
    const delayed = new Promise<{ body: { sort: string; followed_first: boolean } }>((resolve) => {
      releasePreferences = resolve;
    });
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot-preferences": () => delayed,
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
    });
    const { router } = renderSavedBallot();

    // Navigable cards before the sort is known would stamp no railSort and
    // open the rail in the wrong order — the list must wait.
    expect(await screen.findByText("Loading your ballot…")).toBeInTheDocument();
    expect(screen.queryByText("Governor")).not.toBeInTheDocument();

    releasePreferences({ body: { sort: "soonest", followed_first: true } });
    await user.click(await screen.findByRole("link", { name: /Governor/ }));
    const navState = router.state.location.state as { railSort?: string };
    expect(navState.railSort).toBe("soonest");
  });

  it("falls open on a preferences failure: list shown, rail seed omitted", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      ...VERIFIED_BASE,
      "/api/me/ballot-preferences": apiError(500, "internal_error", "boom"),
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
    });
    const { router } = renderSavedBallot();

    // A preferences outage must not hide the ballot; the seed is the only
    // thing lost, and the rail simply opens in its own default order.
    await user.click(await screen.findByRole("link", { name: /Governor/ }));
    const navState = router.state.location.state as { railSort?: string };
    expect(navState.railSort).toBeUndefined();
  });
});
