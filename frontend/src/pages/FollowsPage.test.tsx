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
  });

  it("filters follows by name via the search bar", async () => {
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
      // The same input also fires the debounced typeahead; keep it empty so
      // the dropdown stays closed while this test exercises the filter.
      "/api/candidates/search": { body: { candidates: [] } },
    });
    const user = userEvent.setup();
    renderFollows();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Alex Mayor")).toBeInTheDocument();

    await user.type(screen.getByRole("combobox", { name: "Search candidates by name" }), "jord");
    expect(screen.getByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.queryByText("Alex Mayor")).not.toBeInTheDocument();
    // Screen readers hear the result count through the polite live region.
    expect(screen.getByRole("status")).toHaveTextContent("1 of 2 followed candidates shown.");

    // Clearing the query restores the full list and silences the live region.
    await user.clear(screen.getByRole("combobox", { name: "Search candidates by name" }));
    expect(screen.getByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Alex Mayor")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("");

    await user.type(screen.getByRole("combobox", { name: "Search candidates by name" }), "nobody");
    // Once in the visible notice, once in the live region.
    expect(screen.getAllByText(/No followed candidates match/)).toHaveLength(2);
    expect(screen.getByRole("status")).toHaveTextContent("No followed candidates match “nobody”.");
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
