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

  it("renders election cards, the count line, and only public sorts", async () => {
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
    expect(screen.getByText(/2 elections across 1 district/)).toBeInTheDocument();

    // The anonymous endpoint cannot honor my_areas; the dropdown must not offer it.
    const options = screen.getAllByRole("option").map((option) => option.textContent);
    expect(options).not.toContain("My issues");
    expect(options).toContain("Vote power");
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

    // The roster status renders inside the card's meta line, so match as a
    // substring of that line rather than a standalone element.
    expect(await screen.findByText(/Candidate list not final/)).toBeInTheDocument();
    expect(screen.queryByText(/0 candidates/)).not.toBeInTheDocument();
  });

  it("confirms the matched address from router state and lists the districts", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderBallot({
      pathname: "/ballot",
      search: "?d=d-1",
      state: { matchedAddress: "123 MAIN ST, JUNEAU, AK, 99801" },
    });

    // Geocoder confirmation: the visitor can catch a wrong match and bail out.
    expect(screen.getByText("123 MAIN ST, JUNEAU, AK, 99801")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Not your address?" })).toHaveAttribute("href", "/?new=1");

    // District names come from the ballot response, so they survive a refresh.
    const districtsToggle = await screen.findByText("Which districts?");
    const list = districtsToggle.closest("details");
    expect(list).not.toBeNull();
    expect(list).toHaveTextContent("Alaska");
  });

  it("warns when the search matched multiple addresses so the visitor confirms the first match", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/ballot": { body: ballotSummary([electionSummary()]) },
    });
    renderBallot({
      pathname: "/ballot",
      search: "?d=d-1",
      state: { matchedAddress: "100 MAIN ST, SPRINGFIELD, MA, 01105", addressMatchCount: 7 },
    });

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Your search matched 7 possible addresses, and this ballot is for the first one."
    );
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
