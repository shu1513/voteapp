import { afterEach, describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import { vi } from "vitest";
import { BallotPage } from "./BallotPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, electionSummary } from "../test/fixtures";

const ANONYMOUS = { "/api/me": apiError(401, "unauthorized", "Not logged in") };

function renderBallot(entry: string) {
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

  it("shows the empty-ballot message when districts have no elections", async () => {
    stubApiRoutes({ ...ANONYMOUS, "/api/ballot": { body: ballotSummary([]) } });
    renderBallot("/ballot?d=d-1");
    expect(await screen.findByText(/No upcoming elections found for these districts yet/)).toBeInTheDocument();
  });
});
