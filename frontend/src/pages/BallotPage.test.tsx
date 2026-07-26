import { afterEach, describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import { vi } from "vitest";
import { BallotPage } from "./BallotPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, electionSummary } from "../test/fixtures";

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
});
