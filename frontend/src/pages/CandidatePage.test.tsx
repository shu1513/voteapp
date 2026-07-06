import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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
    expect(screen.getByRole("button", { name: "Report an issue with candidate profile" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Report an issue with candidate record" })).toBeInTheDocument();
  });

  it("submits record reports with the candidate record target", async () => {
    let submittedBody: unknown = null;
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/candidates/c-1": { body: candidateDetail() },
      "/api/content-reports": (_url, init) => {
        submittedBody = JSON.parse(String(init?.body));
        return { status: 201, body: { report: { id: "report-1" } } };
      },
    });
    renderCandidate();

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Report an issue with candidate record" }));
    await user.type(screen.getByLabelText("What's wrong?"), "This record needs another source.");
    await user.click(screen.getByRole("button", { name: "Send report" }));

    expect(await screen.findByText("Report sent. Thank you.")).toBeInTheDocument();
    expect(submittedBody).toEqual({
      entity_type: "candidate_record",
      entity_id: "r-1",
      message: "This record needs another source.",
    });
  });
});
