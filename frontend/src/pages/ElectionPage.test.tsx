import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import { ElectionPage } from "./ElectionPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { electionDetail } from "../test/fixtures";

const ANONYMOUS = { "/api/me": apiError(401, "unauthorized", "Not logged in") };

function renderElection(id = "e-1") {
  return renderRoutes(
    [
      { path: "/elections/:electionId", element: <ElectionPage /> },
      { path: "/candidates/:candidateId", element: <p /> },
      { path: "/disclaimer", element: <p /> },
    ],
    `/elections/${id}`
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ElectionPage", () => {
  it("surfaces the backend's 404 message for an unknown election", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-missing": apiError(404, "not_found", "Election not found"),
    });
    renderElection("e-missing");
    expect(await screen.findByText("Election not found")).toBeInTheDocument();
  });

  it("renders the election header and every candidate", async () => {
    stubApiRoutes({ ...ANONYMOUS, "/api/elections/e-1": { body: electionDetail() } });
    renderElection();

    expect(await screen.findByRole("heading", { name: "Governor" })).toBeInTheDocument();
    expect(screen.getByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Riley Runner")).toBeInTheDocument();
    // Anonymous visitors get no follow controls.
    expect(screen.queryByRole("button", { name: /follow/i })).not.toBeInTheDocument();
  });

  it("renders ballot measure yes/no explanations when present", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-1": {
        body: electionDetail({
          race_type: "ballot_measure",
          candidates: [],
          ballot_measure: {
            id: "m-1",
            official_ballot_title: "Measure 1",
            summary: "A measure.",
            what_yes_means: "Yes approves the bond.",
            what_no_means: "No rejects the bond.",
            result: null,
            source_urls: [],
            official_measure_url: null,
            research_area_tags: [],
          },
        }),
      },
    });
    renderElection();

    expect(await screen.findByText("Yes approves the bond.")).toBeInTheDocument();
    expect(screen.getByText("No rejects the bond.")).toBeInTheDocument();
  });
});
