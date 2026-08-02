import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ElectionChoice } from "@voteapp/api-client";
import { PicksPage } from "./PicksPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, electionSummary, ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

function renderPicks() {
  return renderRoutes(
    [
      { path: "/me/picks", element: <PicksPage /> },
      { path: "/login", element: <p /> },
      { path: "/elections/:electionId", element: <p>Election page</p> },
    ],
    "/me/picks"
  );
}

function electionChoice(overrides: Partial<ElectionChoice> = {}): ElectionChoice {
  return {
    election_id: "e-1",
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    seats_to_fill: null,
    picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "declared" }],
    measure_position: null,
    updated_at: "2026-08-01T00:00:00.000Z",
    ...overrides,
  };
}

// Everything a verified render touches; individual tests override entries.
function verifiedRoutes(overrides: Record<string, unknown> = {}) {
  return {
    "/api/me": { body: ME_VERIFIED },
    "/api/me/ballot": {
      body: ballotSummary([
        electionSummary(),
        electionSummary({ id: "e-2", official_ballot_title: "Mayor" }),
      ]),
    },
    "/api/me/election-choices": { body: { choices: [electionChoice()] } },
    "/api/me/candidate-follows": { body: { follows: [] } },
    "/api/research-areas": { body: { research_areas: [] } },
    "/api/me/research-area-preferences": { body: { preferences: [] } },
    ...overrides,
  } as Parameters<typeof stubApiRoutes>[0];
}

// Frozen clock: the page classifies races as upcoming/past against the real
// date (usLatestLocalDate), so the 2026-11-03 fixtures would flip into the
// past section — and these assertions would rot — once that day passes.
beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  vi.setSystemTime(new Date("2026-08-01T12:00:00Z"));
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("PicksPage", () => {
  it("asks logged-out visitors to log in", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderPicks();
    expect(await screen.findByText(/Log in to plan your votes/)).toBeInTheDocument();
  });

  it("shows the verify interstitial for unverified users", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderPicks();
    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
  });

  it("marks a measure pick with its outcome, muted when it went against the pick", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({ race_type: "ballot_measure", candidate_count: 0 }),
            electionSummary({
              id: "e-2",
              official_ballot_title: "Proposition 9",
              race_type: "ballot_measure",
              candidate_count: 0,
            }),
          ]),
        },
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice({ picks: [], measure_position: "yes", measure_result: "passed" }),
              electionChoice({
                election_id: "e-2",
                official_ballot_title: "Proposition 9",
                race_type: "ballot_measure",
                picks: [],
                measure_position: "yes",
                measure_result: "failed",
              }),
            ],
          },
        },
      })
    );
    renderPicks();

    // Matched pick: green chip; unmatched: muted — same semantics as the
    // candidate Won/Lost chips.
    expect(await screen.findByText("Passed")).toBeInTheDocument();
    expect(screen.getByText("Passed").className).toContain("bg-green-700");
    // The exact muted style, not just "not green": a broken class would
    // otherwise pass.
    expect(screen.getByText("Failed").className).toContain("bg-surface");
  });

  it("renders a date card with picked and undecided races, and all three sections", async () => {
    stubApiRoutes(verifiedRoutes());
    renderPicks();

    // Date card heading + decided count.
    expect(await screen.findByRole("heading", { name: "My November 3, 2026 Election Picks" })).toBeInTheDocument();
    expect(screen.getByText("1 of 2 races decided")).toBeInTheDocument();

    // Picked race: title links to the race, pick renders beside it.
    expect(screen.getByRole("link", { name: "Governor" })).toHaveAttribute("href", "/elections/e-1");
    expect(screen.getByText("Jane Smith")).toBeInTheDocument();

    // Undecided race: the grey line itself is the link to the race.
    const undecided = screen.getByRole("link", { name: "Mayor — no pick yet" });
    expect(undecided).toHaveAttribute("href", "/elections/e-2");

    // The other two sections mounted below.
    expect(screen.getByText("Issues you care about")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Followed candidates" })).toBeInTheDocument();
  });

  it("mints a share link on demand and swaps in the share menu", async () => {
    const fetchMock = stubApiRoutes(
      verifiedRoutes({
        "/api/me/pick-card-shares": {
          body: { share: { token: "tok_abcdefghijklmnopqrstuvwxyz012345", election_date: "2026-11-03" } },
        },
      })
    );
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderPicks();

    await user.click(await screen.findByRole("button", { name: "Share this card" }));

    // The standard ShareButton takes over once the token exists (menu shape
    // in jsdom — no navigator.share), alongside the visibility warning.
    expect(await screen.findByRole("button", { name: "Share" })).toBeInTheDocument();
    // The caption must disclose the name reveal — minting is the consent
    // event, so the sharer learns it here, not from a recipient.
    expect(
      screen.getByText("Anyone with the link can see this card and your first name.")
    ).toBeInTheDocument();

    // The minted URL itself is visible — canonical host in the text, the
    // relative path as the href (the token only resolves where it was
    // minted; see ShareCardControl).
    const mintedLink = screen.getByRole("link", {
      name: "electionssimplified.com/picks/tok_abcdefghijklmnopqrstuvwxyz012345",
    });
    expect(mintedLink).toHaveAttribute("href", "/picks/tok_abcdefghijklmnopqrstuvwxyz012345");
    expect(mintedLink).toHaveAttribute("target", "_blank");

    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(post).toBeDefined();
    expect(String(post![0])).toContain("/api/me/pick-card-shares");
    expect(JSON.parse(String(post![1]!.body))).toEqual({ election_date: "2026-11-03" });

    // The minted link lands in the share menu's copy target.
    await user.click(screen.getByRole("button", { name: "Share" }));
    expect(await screen.findByRole("menuitem", { name: "Share on X" })).toHaveAttribute(
      "href",
      expect.stringContaining("tok_abcdefghijklmnopqrstuvwxyz012345")
    );
  });

  it("shows an error instead of all-undecided cards when the choices fetch fails", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": apiError(500, "internal_error", "boom"),
      })
    );
    renderPicks();

    // The error is the whole story: no card may render claiming races are
    // undecided when the truth is unknown.
    expect(await screen.findByText(/Could not load your picks/)).toBeInTheDocument();
    expect(screen.queryByText(/no pick yet/)).not.toBeInTheDocument();
    expect(screen.queryByText(/races decided/)).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /My November 3, 2026 Election Picks/ })).not.toBeInTheDocument();
  });

  it("hides the share control on a card with zero picks", async () => {
    stubApiRoutes(verifiedRoutes({ "/api/me/election-choices": { body: { choices: [] } } }));
    renderPicks();

    expect(await screen.findByRole("heading", { name: "My November 3, 2026 Election Picks" })).toBeInTheDocument();
    expect(screen.getByText("0 of 2 races decided")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Share this card" })).not.toBeInTheDocument();
  });

  it("lists past picks in a collapsible section with won/lost flags", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice(),
              electionChoice({
                election_id: "e-old",
                official_ballot_title: "Sheriff",
                election_date: "2024-11-05",
                picks: [{ candidate_id: "c-9", display_name: "Pat Winner", candidacy_status: "won" }],
              }),
            ],
          },
        },
      })
    );
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderPicks();

    const summary = await screen.findByText("Past elections (1)");
    await user.click(summary);

    expect(screen.getByText("Pat Winner")).toBeInTheDocument();
    expect(screen.getByText("Won")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Sheriff" })).toHaveAttribute("href", "/elections/e-old");
  });
});
