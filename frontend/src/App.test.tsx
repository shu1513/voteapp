import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import { App } from "./App";
import { PicksPage } from "./pages/PicksPage";
import { renderRoutes } from "./test/render";
import { apiError, stubApiRoutes } from "./test/mockApi";
import { ballotSummary, electionSummary, ME_VERIFIED } from "./test/fixtures";

function renderApp() {
  return renderRoutes(
    [
      {
        path: "/",
        element: <App />,
        children: [{ index: true, element: <p>home content</p> }],
      },
    ],
    "/"
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("App account nav", () => {
  it("shows log in and sign up when logged out", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderApp();
    expect(await screen.findByRole("link", { name: "Log in" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Sign up" })).toBeInTheDocument();
    // A first-time visitor has no draft to link to — the nav stays clean
    // until they've seen a ballot or made a pick.
    expect(screen.queryByRole("link", { name: "My Ballot Draft" })).not.toBeInTheDocument();
  });

  it("shows the draft link once the guest has looked at a ballot", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    window.localStorage.setItem(
      "voteapp_ballot_draft",
      JSON.stringify({ v: 1, district_ids: ["dddddddd-1111-4111-8111-111111111111"], target: null, choices: {} })
    );
    window.dispatchEvent(new StorageEvent("storage", { key: "voteapp_ballot_draft" }));
    renderApp();
    expect(await screen.findByRole("link", { name: "My Ballot Draft" })).toHaveAttribute(
      "href",
      "/draft"
    );
    window.localStorage.clear();
    window.dispatchEvent(new StorageEvent("storage", { key: "voteapp_ballot_draft" }));
  });

  it("renders the signed-in links inline at every width — no hamburger menu", async () => {
    stubApiRoutes({ "/api/me": { body: ME_VERIFIED } });
    renderApp();

    expect(await screen.findByRole("link", { name: "My Elections" })).toHaveAttribute("href", "/me/ballot");
    // Plain "My Draft" (no counter) while no pick is made / progress unknown.
    expect(screen.getByRole("link", { name: "My Draft" })).toHaveAttribute("href", "/me/picks");
    expect(screen.getByRole("link", { name: "My Candidates" })).toHaveAttribute("href", "/me/follows");
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("href", "/me/settings");

    // The greeting sits beside the logo as plain text, not a link or button.
    const greeting = screen.getByText("Hi Sam");
    expect(greeting.closest("a")).toBeNull();
    expect(greeting.closest("button")).toBeNull();

    // The links must never hide behind a disclosure at narrow widths — the
    // nav wraps instead. No Menu button, no menu items.
    expect(screen.queryByRole("button", { name: "Menu" })).not.toBeInTheDocument();
  });

  it("cold-loading /me/picks shares ONE ballot request between the header badge and the page", async () => {
    // The header's pick counter (usePickProgress) and PicksPage ride the
    // same query key and url on purpose — rendering the page WITHOUT the
    // app shell would hide a regression back to two requests.
    // Frozen clock so the 2026-11-03 fixture stays upcoming (same reason as
    // PicksPage.test).
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.setSystemTime(new Date("2026-08-01T12:00:00Z"));
    const fetchMock = stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/ballot": { body: ballotSummary([electionSummary()]) },
      "/api/me/election-choices": {
        body: {
          choices: [
            {
              election_id: "e-1",
              race_type: "office",
              official_ballot_title: "Governor",
              election_date: "2026-11-03",
              seats_to_fill: null,
              picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "declared" }],
              measure_position: null,
              updated_at: "2026-08-01T00:00:00.000Z",
            },
          ],
        },
      },
    });
    renderRoutes(
      [
        {
          path: "/",
          element: <App />,
          children: [{ path: "me/picks", element: <PicksPage /> }],
        },
      ],
      "/me/picks"
    );

    // Both consumers settled: the page's date card AND the header counter
    // computed off the same payload (1 race, 1 picked → the earned label).
    expect(
      await screen.findByRole("heading", { name: "My November 3, 2026 Election Draft" })
    ).toBeInTheDocument();
    expect(await screen.findByRole("link", { name: "My Picks ✓" })).toBeInTheDocument();

    const ballotCalls = fetchMock.mock.calls.filter(([input]) => String(input).includes("/api/me/ballot?"));
    expect(ballotCalls).toHaveLength(1);
    expect(String(ballotCalls[0][0])).toContain("include=preview");
  });
});
