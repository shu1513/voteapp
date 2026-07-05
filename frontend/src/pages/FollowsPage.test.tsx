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
      { path: "/candidates/:candidateId", element: <p /> },
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
