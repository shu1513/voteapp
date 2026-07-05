import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import { CandidatePage } from "./CandidatePage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { candidateDetail } from "../test/fixtures";

const ANONYMOUS = { "/api/me": apiError(401, "unauthorized", "Not logged in") };

function renderCandidate(id = "c-1") {
  return renderRoutes(
    [
      { path: "/candidates/:candidateId", element: <CandidatePage /> },
      { path: "/elections/:electionId", element: <p /> },
    ],
    `/candidates/${id}`
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("CandidatePage", () => {
  it("surfaces the backend's 404 message for an unknown candidate", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/candidates/c-missing": apiError(404, "not_found", "Candidate not found"),
    });
    renderCandidate("c-missing");
    expect(await screen.findByText("Candidate not found")).toBeInTheDocument();
  });

  it("renders the profile with records grouped under their research area", async () => {
    stubApiRoutes({ ...ANONYMOUS, "/api/candidates/c-1": { body: candidateDetail() } });
    renderCandidate();

    expect(await screen.findByRole("heading", { name: "Jordan Voter" })).toBeInTheDocument();
    expect(screen.getByText("Voted for the clean water act.")).toBeInTheDocument();
    // The record's area tag names the group heading.
    expect(screen.getByText("Environment")).toBeInTheDocument();
  });
});
