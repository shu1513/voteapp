import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AutoPickElectionResult, ElectionChoice, Me } from "@voteapp/api-client";
import { AutoPickFillControl } from "./AutoPickFillControl";
import { stubApiRoutes } from "../test/mockApi";
import { electionSummary } from "../test/fixtures";
import { renderRoutes } from "../test/render";

// Same mocking rationale as AutoPickControl.test: useMyResearchAreas forks
// on the session, and renderRoutes seeds nothing — a real useMe would stay
// loading forever.
const SIGNED_IN: Me = {
  email: "voter@example.com",
  first_name: "Vo",
  email_verified: true,
  accepted_terms_version: null,
  has_password: true,
};
vi.mock("@voteapp/api-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@voteapp/api-client")>();
  return {
    ...actual,
    useMe: () => ({ me: SIGNED_IN, isLoading: false, isError: false, refetch: vi.fn() }),
  };
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const TODAY = "2026-08-01";
const E_EMPTY = "11111111-1111-4111-8111-111111111111";
const E_PICKED = "22222222-2222-4222-8222-222222222222";
const E_MEASURE = "33333333-3333-4333-8333-333333333333";
const E_PAST = "44444444-4444-4444-8444-444444444444";
const CAND_A = "bbbbbbbb-0000-4000-8000-00000000000a";
const AREA = "aaaaaaaa-0000-4000-8000-000000000001";

const THREE_PREFERENCES = {
  preferences: [1, 2, 3].map((rank) => ({
    research_area_id: `${AREA.slice(0, -1)}${rank}`,
    slug: `issue-${rank}`,
    name: `Issue ${rank}`,
    description: null,
    rank,
    direction: "support",
    hard_veto: false,
  })),
};

function choice(overrides: Partial<ElectionChoice>): ElectionChoice {
  return {
    election_id: E_PICKED,
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    seats_to_fill: null,
    picks: [],
    measure_position: null,
    updated_at: "2026-08-01T00:00:00.000Z",
    ...overrides,
  };
}

function noPickResult(electionId: string, reason: AutoPickElectionResult["reason"]): AutoPickElectionResult {
  return {
    election_id: electionId,
    race_type: "office",
    outcome: "no_pick",
    reason,
    picked_candidate_ids: [],
    measure_position: null,
    shortlist_candidate_ids: [],
    candidates: [],
    measure_per_issue: [],
    unresearched: [],
  };
}

function pickedResult(electionId: string): AutoPickElectionResult {
  return { ...noPickResult(electionId, null), outcome: "picked", picked_candidate_ids: [CAND_A] };
}

const ELECTIONS = [
  electionSummary({ id: E_EMPTY, official_ballot_title: "Mayor" }),
  electionSummary({ id: E_PICKED, official_ballot_title: "Governor" }),
  electionSummary({ id: E_MEASURE, official_ballot_title: "Measure Q", race_type: "ballot_measure" }),
];

function renderControl({
  elections = ELECTIONS,
  choices = [] as ElectionChoice[],
}: { elections?: typeof ELECTIONS; choices?: ElectionChoice[] } = {}) {
  const byId = new Map(choices.map((entry) => [entry.election_id, entry]));
  return renderRoutes([
    {
      path: "/",
      element: (
        <AutoPickFillControl
          elections={elections}
          choices={choices}
          choiceByElectionId={byId}
          today={TODAY}
        />
      ),
    },
    { path: "/me/settings", element: <p>Settings page</p> },
    { path: "/elections/:electionId", element: <p>Election page</p> },
  ]);
}

/** The button disables while the preferences load; click only once ready. */
async function clickFill() {
  const button = await screen.findByRole("button", { name: /Fill my empty picks/ });
  await waitFor(() => expect(button).toBeEnabled());
  await userEvent.click(button);
}

function requestsTo(fetchMock: ReturnType<typeof stubApiRoutes>, pathname: string) {
  return fetchMock.mock.calls
    .filter(([input]) => new URL(String(input), "http://localhost").pathname === pathname)
    .map(([, init]) => JSON.parse(String(init?.body)));
}

describe("AutoPickFillControl", () => {
  it("renders nothing when every race is decided and nothing is auto-picked", async () => {
    stubApiRoutes({ "/api/me/research-area-preferences": { body: THREE_PREFERENCES } });
    const { container } = renderControl({
      elections: [ELECTIONS[1]!],
      choices: [
        choice({ picks: [{ candidate_id: CAND_A, display_name: "Alice", candidacy_status: "declared" }] }),
      ],
    });
    await waitFor(() => expect(container.querySelector("section")).toBeNull());
  });

  it("fills only the empty races and reports the filled/open breakdown with per-race reasons", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/auto-picks": {
        body: {
          results: [pickedResult(E_EMPTY), noPickResult(E_MEASURE, "insufficient_evidence")],
        },
      },
    });
    renderControl({
      choices: [
        choice({ picks: [{ candidate_id: CAND_A, display_name: "Alice", candidacy_status: "declared" }] }),
      ],
    });
    // The decided race stays out of the count and out of the request.
    await clickFill();

    expect(await screen.findByText("Filled 1 · 1 left open — 1 not enough evidence.")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Measure Q" })).toHaveAttribute("href", `/elections/${E_MEASURE}`);
    expect(screen.getByText(/not enough evidence$/)).toBeInTheDocument();
    expect(requestsTo(fetchMock, "/api/me/auto-picks")).toEqual([
      { election_ids: [E_EMPTY, E_MEASURE], mode: "fill_empty" },
    ]);
  });

  it("sends large ballots in chunks of 200", async () => {
    const many = Array.from({ length: 201 }, (_, index) =>
      electionSummary({
        id: `${index.toString(16).padStart(8, "0")}-0000-4000-8000-000000000000`,
        official_ballot_title: `Race ${index}`,
      })
    );
    const fetchMock = stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/auto-picks": (_url, init) => {
        const body = JSON.parse(String(init?.body)) as { election_ids: string[] };
        return { body: { results: body.election_ids.map((id) => pickedResult(id)) } };
      },
    });
    renderControl({ elections: many });
    await clickFill();

    expect(await screen.findByText("Filled 201.")).toBeInTheDocument();
    const bodies = requestsTo(fetchMock, "/api/me/auto-picks");
    expect(bodies.map((body) => body.election_ids.length)).toEqual([200, 1]);
  });

  it("prompts for the issue floor instead of calling the API", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderControl();
    await clickFill();

    expect(await screen.findByText(/Rank at least 3 issues first/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Rank your issues" })).toHaveAttribute("href", "/me/settings");
    expect(requestsTo(fetchMock, "/api/me/auto-picks")).toEqual([]);
  });

  it("clears upcoming auto rows only — manual picks and past races stay", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choice: {} } },
    });
    renderControl({
      elections: [],
      choices: [
        choice({
          picks: [
            { candidate_id: CAND_A, display_name: "Alice", candidacy_status: "declared", origin: "auto" },
            { candidate_id: E_EMPTY, display_name: "Manny", candidacy_status: "declared", origin: "manual" },
          ],
        }),
        choice({
          election_id: E_MEASURE,
          race_type: "ballot_measure",
          measure_position: "yes",
          measure_origin: "auto",
        }),
        choice({
          election_id: E_PAST,
          election_date: "2026-05-05",
          picks: [
            { candidate_id: CAND_A, display_name: "Alice", candidacy_status: "declared", origin: "auto" },
          ],
        }),
      ],
    });
    await userEvent.click(await screen.findByRole("button", { name: "Clear auto picks" }));

    await waitFor(() =>
      expect(requestsTo(fetchMock, "/api/me/election-choices")).toEqual([
        { election_id: E_PICKED, candidate_id: CAND_A, chosen: false },
        { election_id: E_MEASURE, measure_position: null },
      ])
    );
  });
});
