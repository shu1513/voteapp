import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { FollowedCandidatesSection } from "./FollowedCandidatesSection";
import { renderRoutes } from "../test/render";
import { stubApiRoutes } from "../test/mockApi";
import { candidateFollow, ME_VERIFIED } from "../test/fixtures";

// Behavior ported from the retired FollowsPage (the section is the page's
// old body minus the login/verify gating, which now lives on PicksPage, and
// minus the "Latest:" record preview, dropped deliberately).

function renderSection() {
  return renderRoutes(
    [
      { path: "/", element: <FollowedCandidatesSection /> },
      { path: "/candidates/:candidateId", element: <p>Candidate page</p> },
    ],
    "/"
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("FollowedCandidatesSection", () => {
  it("shows the empty state when nothing is followed", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
    });
    renderSection();
    expect(await screen.findByText(/You aren't following anyone yet/)).toBeInTheDocument();
  });

  it("renders follow rows without the old Latest record preview", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": {
        body: {
          follows: [
            candidateFollow({
              latest_record: { description: "Voted to expand transit funding.", event_date: "2026-05-01" },
            }),
          ],
        },
      },
    });
    renderSection();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    // The record preview is deliberately gone on My Picks.
    expect(screen.queryByText(/Latest:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/expand transit funding/)).not.toBeInTheDocument();
    // The notification toggles survive the move.
    expect(screen.getByLabelText("Email me about their future elections")).toBeInTheDocument();
    expect(screen.getByLabelText("Email me about their new actions")).toBeInTheDocument();
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
    renderSection();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Alex Mayor")).toBeInTheDocument();

    // The search box is typeahead-only: the followed list must stay put.
    await user.type(screen.getByRole("combobox", { name: "Search candidates:" }), "jord");
    expect(screen.getByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Alex Mayor")).toBeInTheDocument();
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
    renderSection();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    await user.type(screen.getByRole("combobox", { name: "Search candidates:" }), "hilar");

    // Waits out the typeahead debounce; the suggestion is NOT a follow.
    const option = await screen.findByRole("option", { name: /Hilary Brown/ });
    await user.click(option);

    expect(await screen.findByText("Candidate page")).toBeInTheDocument();
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
    renderSection();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    await user.type(screen.getByRole("combobox", { name: "Search candidates:" }), "hilar");
    expect(await screen.findByRole("option", { name: /Hilary Brown/ })).toBeInTheDocument();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("option")).not.toBeInTheDocument();
  });

  it("sends following:false on Unfollow", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": (_url, init) =>
        init?.method === "PUT" ? { body: { follow: null } } : { body: { follows: [candidateFollow()] } },
    });
    const user = userEvent.setup();
    renderSection();

    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Unfollow" }));

    await waitFor(() => {
      const put = fetchMock.mock.calls.find(([, init]) => init?.method === "PUT");
      expect(put).toBeDefined();
      expect(JSON.parse(String(put![1]!.body))).toEqual({ candidate_id: "c-1", following: false });
    });
  });
});
