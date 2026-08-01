import { afterEach, describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi } from "vitest";
import { BallotPage } from "./BallotPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, electionSummary, ME_VERIFIED } from "../test/fixtures";

const ANONYMOUS = { "/api/me": apiError(401, "unauthorized", "Not logged in") };

function renderBallot(entry: string | { pathname: string; search?: string; state?: unknown }) {
  return renderRoutes(
    [
      { path: "/ballot", element: <BallotPage /> },
      { path: "/", element: <p /> },
    ],
    entry
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("BallotPage", () => {
  it("asks for an address when no districts are in the URL", () => {
    stubApiRoutes(ANONYMOUS);
    renderBallot("/ballot");
    expect(screen.getByText("No districts selected.")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Start with your address" })).toHaveAttribute("href", "/");
  });

  it("shows the service-trouble copy on a 500", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": apiError(500, "internal_error", "boom"),
    });
    renderBallot("/ballot?d=d-1");
    expect(
      await screen.findByText("The service is having trouble right now. Please try again shortly.")
    ).toBeInTheDocument();
  });

  it("renders election cards under a date heading, with only public sorts", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": {
        body: ballotSummary([
          electionSummary(),
          electionSummary({ id: "e-2", official_ballot_title: "State Senate" }),
        ]),
      },
    });
    renderBallot("/ballot?d=d-1");

    expect(await screen.findByText("Governor")).toBeInTheDocument();
    expect(screen.getByText("State Senate")).toBeInTheDocument();
    // The date heading is the page's visible identity — the "Elections"
    // h1 survives for screen readers only. No election/district count line,
    // no explainer collapsibles.
    expect(
      screen.getByRole("heading", { name: "Elections on November 3, 2026" })
    ).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 1, name: "Elections" })).toHaveClass("sr-only");
    expect(screen.queryByText(/elections across/)).not.toBeInTheDocument();
    expect(screen.queryByText("Which districts?")).not.toBeInTheDocument();
    expect(screen.queryByText("What do these labels mean?")).not.toBeInTheDocument();

    // The anonymous endpoint cannot honor my_areas; the dropdown must not offer it.
    const options = screen.getAllByRole("option").map((option) => option.textContent);
    expect(options).not.toContain("My issues");
    expect(options).toContain("Vote impact");
  });

  it("replaces the 0-candidates chip with the roster-status explanation", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": {
        body: ballotSummary([
          electionSummary({
            candidate_count: 0,
            candidate_roster_status: { reason: "awaiting_official_roster", check_after: "2026-08-27" },
          }),
        ]),
      },
    });
    renderBallot("/ballot?d=d-1");

    // The roster status renders in the card's title row where the candidate
    // count would sit, so match it as a substring.
    expect(await screen.findByText(/Candidate list not final/)).toBeInTheDocument();
    expect(screen.queryByText(/0 candidates/)).not.toBeInTheDocument();
  });

  it("shows no matched-address confirmation line for an unambiguous match", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderBallot({
      pathname: "/ballot",
      search: "?d=d-1",
      state: { matchedAddress: "123 MAIN ST, JUNEAU, AK, 99801" },
    });

    // The always-on confirmation line was dropped as clutter; without an
    // ambiguous match count the address never renders.
    await screen.findByText("Governor");
    expect(screen.queryByText(/123 MAIN ST/)).not.toBeInTheDocument();
    expect(screen.queryByText("Not your address?")).not.toBeInTheDocument();
  });

  it("warns with the matched address when the search matched multiple addresses", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderBallot({
      pathname: "/ballot",
      search: "?d=d-1",
      state: { matchedAddress: "100 MAIN ST, SPRINGFIELD, MA, 01105", addressMatchCount: 7 },
    });

    // Self-contained warning: it names the matched address itself since the
    // confirmation line above it is gone.
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Your search matched 7 possible addresses");
    expect(alert).toHaveTextContent("100 MAIN ST, SPRINGFIELD, MA, 01105");
    expect(screen.getByRole("link", { name: "search again" })).toHaveAttribute("href", "/?new=1");
  });

  it("shows no ambiguity warning for an exact single match", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderBallot({
      pathname: "/ballot",
      search: "?d=d-1",
      state: { matchedAddress: "123 MAIN ST, JUNEAU, AK, 99801", addressMatchCount: 1 },
    });

    await screen.findByText("Governor");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("omits the matched-address line on direct visits without router state", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderBallot("/ballot?d=d-1");
    await screen.findByText("Governor");
    expect(screen.queryByText(/Matched address:/)).not.toBeInTheDocument();
  });

  it("shows the empty-ballot message when districts have no elections", async () => {
    stubApiRoutes({ ...ANONYMOUS, "/api/ballot": { body: ballotSummary([]) } });
    renderBallot("/ballot?d=d-1");
    expect(await screen.findByText(/No upcoming elections found for these districts yet/)).toBeInTheDocument();
  });

  describe("only-my-issues filter", () => {
    const HOUSING = { id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null };
    const SAVED_HOUSING = {
      "/api/me": { body: ME_VERIFIED },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null, rank: 1 },
          ],
        },
      },
    };
    // A ballot that splits: one race on the saved area, one not.
    const SPLIT_BALLOT = {
      body: ballotSummary([
        electionSummary({ research_areas: [HOUSING] }),
        electionSummary({ id: "e-2", official_ballot_title: "State Senate" }),
      ]),
    };

    it("never renders for viewers without saved areas, even with ?issues=mine", async () => {
      stubApiRoutes({ ...ANONYMOUS, "/api/ballot": SPLIT_BALLOT });
      renderBallot("/ballot?d=d-1&issues=mine");

      // The request is ignored (the intersection is meaningless without
      // saved areas): full list, no control.
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      expect(screen.getByText("State Senate")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Only my issues" })).not.toBeInTheDocument();
    });

    it("filters to matching races with a hidden count, and Show all restores", async () => {
      stubApiRoutes({ ...SAVED_HOUSING, "/api/ballot": SPLIT_BALLOT });
      const user = userEvent.setup();
      const { router } = renderBallot("/ballot?d=d-1");

      await user.click(await screen.findByRole("button", { name: "Only my issues" }));
      expect(screen.getByText("Governor")).toBeInTheDocument();
      expect(screen.queryByText("State Senate")).not.toBeInTheDocument();
      expect(screen.getByText(/1 election hidden/)).toBeInTheDocument();
      // URL state like sort, so the choice survives navigation.
      expect(router.state.location.search).toContain("issues=mine");

      await user.click(screen.getByRole("button", { name: "Show all" }));
      expect(screen.getByText("State Senate")).toBeInTheDocument();
      expect(router.state.location.search).not.toContain("issues=mine");
    });

    it("arrives filtered from a ?issues=mine URL", async () => {
      stubApiRoutes({ ...SAVED_HOUSING, "/api/ballot": SPLIT_BALLOT });
      renderBallot("/ballot?d=d-1&issues=mine");

      // The pressed toggle proves both async queries (ballot + saved areas)
      // have landed and the filter engaged.
      expect(await screen.findByRole("button", { name: "Only my issues" })).toHaveAttribute(
        "aria-pressed",
        "true"
      );
      // findByText: the toggle can render (saved areas landed) before the
      // ballot payload does. Once a race is visible the filter is already
      // engaged, so the absence check cannot race.
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      expect(screen.queryByText("State Senate")).not.toBeInTheDocument();
    });

    it("hides the off toggle when no race matches", async () => {
      stubApiRoutes({ ...SAVED_HOUSING, "/api/ballot": { body: ballotSummary([electionSummary()]) } });
      renderBallot("/ballot?d=d-1");

      // Filtering would empty the list, so the (off) toggle has nothing to
      // offer.
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Only my issues" })).not.toBeInTheDocument();
    });

    it("hides the off toggle when every race matches", async () => {
      stubApiRoutes({
        ...SAVED_HOUSING,
        "/api/ballot": { body: ballotSummary([electionSummary({ research_areas: [HOUSING] })]) },
      });
      renderBallot("/ballot?d=d-1");

      // Filtering would be a no-op.
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Only my issues" })).not.toBeInTheDocument();
    });

    it("withholds the list on a ?issues=mine load until the saved areas arrive", async () => {
      // The ballot is one request; the saved areas are two chained ones, so
      // the ballot usually lands first. The page must not flash the full
      // unfiltered list in that window.
      let releasePreferences!: () => void;
      const preferencesGate = new Promise<void>((resolve) => {
        releasePreferences = resolve;
      });
      stubApiRoutes({
        "/api/me": { body: ME_VERIFIED },
        "/api/me/research-area-preferences": async () => {
          await preferencesGate;
          return SAVED_HOUSING["/api/me/research-area-preferences"];
        },
        "/api/ballot": SPLIT_BALLOT,
      });
      renderBallot("/ballot?d=d-1&issues=mine");

      // The ballot payload lands well within this window; no race may
      // render while the saved areas are still unknown.
      await expect(screen.findByText("Governor", {}, { timeout: 250 })).rejects.toThrow();
      expect(screen.queryByText("State Senate")).not.toBeInTheDocument();
      expect(screen.getByText("Loading your elections…")).toBeInTheDocument();

      releasePreferences();
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      expect(screen.queryByText("State Senate")).not.toBeInTheDocument();
    });

    it("shows a ballot error instead of the withhold notice while saved areas still load", async () => {
      // A ballot error has no list to withhold: it must win over the
      // saved-areas gate, not render alongside (or behind) a loading notice.
      const neverSettles = new Promise<never>(() => {});
      stubApiRoutes({
        "/api/me": { body: ME_VERIFIED },
        "/api/me/research-area-preferences": () => neverSettles,
        "/api/ballot": apiError(500, "internal_error", "boom"),
      });
      renderBallot("/ballot?d=d-1&issues=mine");

      expect(
        await screen.findByText("The service is having trouble right now. Please try again shortly.")
      ).toBeInTheDocument();
      expect(screen.queryByText("Loading your elections…")).not.toBeInTheDocument();
    });

    it("fails open to the full list when the saved-areas fetch fails", async () => {
      stubApiRoutes({
        "/api/me": { body: ME_VERIFIED },
        "/api/me/research-area-preferences": apiError(500, "internal_error", "boom"),
        "/api/ballot": SPLIT_BALLOT,
      });
      renderBallot("/ballot?d=d-1&issues=mine");

      // Deliberate fail-open, not a spinner: a ballot app errs toward
      // showing races, and no on-page element claims filtering here — the
      // request is ignored and the control stays hidden.
      expect(await screen.findByText("Governor")).toBeInTheDocument();
      expect(screen.getByText("State Senate")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Only my issues" })).not.toBeInTheDocument();
    });

    it("keeps an active filter applied and visible when it empties the ballot", async () => {
      stubApiRoutes({
        ...SAVED_HOUSING,
        "/api/ballot": {
          body: ballotSummary([
            electionSummary(),
            electionSummary({ id: "e-2", official_ballot_title: "State Senate" }),
          ]),
        },
      });
      renderBallot("/ballot?d=d-1&issues=mine");

      // No race matches: an active filter must not silently stop applying —
      // it honestly empties the view, with the hidden count and Show all
      // explaining the empty list.
      expect(await screen.findByText(/2 elections hidden/)).toBeInTheDocument();
      expect(screen.queryByText("Governor")).not.toBeInTheDocument();
      expect(screen.queryByText("State Senate")).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Only my issues" })).toHaveAttribute("aria-pressed", "true");
      expect(screen.getByRole("button", { name: "Show all" })).toBeInTheDocument();
    });
  });
});
