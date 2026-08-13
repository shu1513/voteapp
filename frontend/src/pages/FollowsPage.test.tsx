import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { FollowsPage } from "./FollowsPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { candidateFollow, ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

function renderFollows() {
  return renderRoutes(
    [
      { path: "/me/follows", element: <FollowsPage /> },
      { path: "/login", element: <p /> },
      { path: "/elections/:electionId", element: <p>Election page</p> },
      { path: "/candidates/:candidateId", element: <p>Candidate page</p> },
    ],
    "/me/follows"
  );
}

function verifiedRoutes(overrides: Record<string, unknown> = {}) {
  return {
    "/api/me": { body: ME_VERIFIED },
    "/api/me/candidate-follows": { body: { follows: [] } },
    ...overrides,
  } as Parameters<typeof stubApiRoutes>[0];
}

afterEach(() => {
  vi.unstubAllGlobals();
});

// Back-link state every outbound link on this page must carry — races and
// candidates opened from here return to My Candidates, not My Picks.
const MY_CANDIDATES_STATE = { backTo: { path: "/me/follows", label: "My Candidates" } };

describe("FollowsPage", () => {
  it("asks logged-out visitors to log in", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderFollows();
    expect(await screen.findByText(/Log in to manage the candidates/)).toBeInTheDocument();
  });

  it("shows the verify interstitial for unverified users", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderFollows();
    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
  });

  it("renders the followed-candidates manager", async () => {
    stubApiRoutes(verifiedRoutes({ "/api/me/candidate-follows": { body: { follows: [candidateFollow()] } } }));
    renderFollows();
    expect(await screen.findByRole("heading", { name: "My Candidates" })).toBeInTheDocument();
    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
  });

  it("defaults to next-election order with electionless follows last, and can switch to A–Z", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/candidate-follows": {
          body: {
            follows: [
              candidateFollow({
                candidate_id: "c-1",
                display_name: "Walter Late",
                active_election: {
                  election_id: "e-1",
                  official_ballot_title: "Governor",
                  election_date: "2026-11-03",
                },
              }),
              candidateFollow({
                candidate_id: "c-2",
                display_name: "Zoe Soon",
                active_election: {
                  election_id: "e-3",
                  official_ballot_title: "Mayor",
                  election_date: "2026-09-01",
                },
              }),
              candidateFollow({ candidate_id: "c-3", display_name: "Adam None", active_election: null }),
            ],
          },
        },
      })
    );
    const user = userEvent.setup();
    renderFollows();

    function followedNames(): (string | null)[] {
      return screen
        .getAllByRole("link")
        .filter((link) => link.getAttribute("href")?.startsWith("/candidates/"))
        .map((link) => link.textContent);
    }

    await screen.findByText("Zoe Soon");
    expect(followedNames()).toEqual(["Zoe Soon", "Walter Late", "Adam None"]);

    await user.selectOptions(screen.getByRole("combobox", { name: "Sort by:" }), "name");
    expect(followedNames()).toEqual(["Adam None", "Walter Late", "Zoe Soon"]);
  });

  it("hands followed-candidate links My Candidates as their back destination", async () => {
    const user = userEvent.setup();
    stubApiRoutes(verifiedRoutes({ "/api/me/candidate-follows": { body: { follows: [candidateFollow()] } } }));
    const { router } = renderFollows();

    await user.click(await screen.findByRole("link", { name: "Jordan Voter" }));

    expect(router.state.location.pathname).toBe("/candidates/c-1");
    expect(router.state.location.state).toEqual(MY_CANDIDATES_STATE);
  });

  it("carries the My Candidates context through a picked search suggestion", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/candidates/search": {
          body: {
            candidates: [
              {
                candidate_id: "c-7",
                display_name: "Sam Searcher",
                party: "Independent",
                state: "AK",
                current_office: null,
              },
            ],
          },
        },
      })
    );
    const user = userEvent.setup();
    const { router } = renderFollows();

    // Typing past the 2-char minimum fires the debounced search.
    await user.type(await screen.findByRole("combobox", { name: "Search candidates:" }), "sam");
    await user.click(await screen.findByRole("option", { name: /Sam Searcher/ }));

    expect(router.state.location.pathname).toBe("/candidates/c-7");
    expect(router.state.location.state).toEqual(MY_CANDIDATES_STATE);
  });
});
