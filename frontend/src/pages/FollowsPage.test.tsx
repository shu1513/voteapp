import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
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
      { path: "/candidates/:candidateId", element: <p>Candidate page</p> },
    ],
    "/me/follows"
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("FollowsPage", () => {
  it("asks logged-out visitors to log in", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderFollows();
    expect(await screen.findByText("Log in to manage the candidates you follow.")).toBeInTheDocument();
  });

  it("shows the verify interstitial for unverified users", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderFollows();
    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Resend verification email" })).toBeInTheDocument();
  });

  it("shows the empty state when nothing is followed", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
    });
    renderFollows();
    expect(await screen.findByText(/You aren't following anyone yet/)).toBeInTheDocument();
    // The tab title matches the on-page heading.
    expect(document.title).toContain("Followed Candidates");
  });

  it("keeps the follows list unfiltered while typing in the search box", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": {
        body: {
          follows: [
            candidateFollow(),
            candidateFollow({ candidate_id: "c-2", display_name: "Alex Mayor" }),
          ],
        },
      },
      "/api/candidates/search": { body: { candidates: [] } },
    });
    const user = userEvent.setup();
    renderFollows();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Alex Mayor")).toBeInTheDocument();

    // The search box is typeahead-only: the followed list must stay put.
    await user.type(screen.getByRole("combobox", { name: "Search candidates by name" }), "jord");
    expect(screen.getByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Alex Mayor")).toBeInTheDocument();
    expect(screen.queryByText(/No followed candidates match/)).not.toBeInTheDocument();
  });

  it("offers candidate search to users with zero follows", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/candidates/search": {
        body: {
          candidates: [
            {
              candidate_id: "33333333-3333-4333-8333-333333333333",
              display_name: "Hilary Brown",
              party: "Independent",
              state: "CA",
              current_office: null,
            },
          ],
        },
      },
    });
    const user = userEvent.setup();
    renderFollows();

    expect(await screen.findByText(/You aren't following anyone yet/)).toBeInTheDocument();
    await user.type(screen.getByRole("combobox", { name: "Search candidates by name" }), "hilar");
    expect(await screen.findByRole("option", { name: /Hilary Brown/ })).toBeInTheDocument();
  });

  it("closes the suggestion dropdown on Escape", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [candidateFollow()] } },
      "/api/candidates/search": {
        body: {
          candidates: [
            {
              candidate_id: "33333333-3333-4333-8333-333333333333",
              display_name: "Hilary Brown",
              party: "Independent",
              state: "CA",
              current_office: null,
            },
          ],
        },
      },
    });
    const user = userEvent.setup();
    renderFollows();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    await user.type(screen.getByRole("combobox", { name: "Search candidates by name" }), "hilar");
    expect(await screen.findByRole("option", { name: /Hilary Brown/ })).toBeInTheDocument();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("option")).not.toBeInTheDocument();
  });

  it("suggests candidates from the whole database and navigates on pick", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [candidateFollow()] } },
      "/api/candidates/search": {
        body: {
          candidates: [
            {
              candidate_id: "33333333-3333-4333-8333-333333333333",
              display_name: "Hilary Brown",
              party: "Independent",
              state: "CA",
              current_office: null,
            },
          ],
        },
      },
    });
    const user = userEvent.setup();
    renderFollows();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    await user.type(screen.getByRole("combobox", { name: "Search candidates by name" }), "hilar");

    // Waits out the typeahead debounce; the suggestion is NOT a follow.
    const option = await screen.findByRole("option", { name: /Hilary Brown/ });
    await user.click(option);

    expect(await screen.findByText("Candidate page")).toBeInTheDocument();
  });

  it("renders follow rows and sends following:false on Unfollow", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": (_url, init) =>
        init?.method === "PUT" ? { body: { follow: null } } : { body: { follows: [candidateFollow()] } },
    });
    const user = userEvent.setup();
    renderFollows();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Unfollow" }));

    await waitFor(() => {
      const put = fetchMock.mock.calls.find(([, init]) => init?.method === "PUT");
      expect(put).toBeDefined();
      expect(JSON.parse(String(put![1]!.body))).toEqual({ candidate_id: "c-1", following: false });
    });
  });
});
