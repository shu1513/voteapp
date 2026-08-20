import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AutoPickElectionResult, Me } from "@voteapp/api-client";
import { AutoPickControl } from "./AutoPickControl";
import { stubApiRoutes } from "../test/mockApi";
import { renderRoutes } from "../test/render";

// Same mocking rationale as ElectionChoiceControls.test: the control forks
// on the session, and renderRoutes seeds nothing — a real useMe would stay
// loading forever.
const SIGNED_IN: Me = {
  email: "voter@example.com",
  first_name: "Vo",
  email_verified: true,
  accepted_terms_version: null,
  has_password: true,
};
let mockMe: Me | null | undefined = SIGNED_IN;
vi.mock("@voteapp/api-client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@voteapp/api-client")>();
  return {
    ...actual,
    useMe: () => ({ me: mockMe, isLoading: false, isError: false, refetch: vi.fn() }),
  };
});

beforeEach(() => {
  mockMe = SIGNED_IN;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const ELECTION_ID = "33333333-3333-4333-8333-333333333333";
const AREA_HOUSING = "aaaaaaaa-0000-4000-8000-000000000001";
const AREA_CLIMATE = "aaaaaaaa-0000-4000-8000-000000000002";
const AREA_TAXES = "aaaaaaaa-0000-4000-8000-000000000003";
const CAND_A = "bbbbbbbb-0000-4000-8000-00000000000a";
const CAND_B = "bbbbbbbb-0000-4000-8000-00000000000b";

function preference(researchAreaId: string, name: string, rank: number) {
  return {
    research_area_id: researchAreaId,
    slug: name.toLowerCase(),
    name,
    description: null,
    rank,
    direction: "support",
    hard_veto: false,
  };
}

const THREE_PREFERENCES = {
  preferences: [
    preference(AREA_HOUSING, "Housing", 1),
    preference(AREA_CLIMATE, "Climate", 2),
    preference(AREA_TAXES, "Taxes", 3),
  ],
};

function pickedResult(overrides: Partial<AutoPickElectionResult> = {}): AutoPickElectionResult {
  return {
    election_id: ELECTION_ID,
    race_type: "office",
    outcome: "picked",
    reason: null,
    picked_candidate_ids: [CAND_A],
    measure_position: null,
    shortlist_candidate_ids: [],
    candidates: [
      {
        candidate_id: CAND_A,
        display_name: "Alice Alvarez",
        score: 1,
        has_evidence: true,
        vetoed_by: [],
        per_issue: [{ research_area_id: AREA_HOUSING, net: 1, for_count: 2, against_count: 0 }],
      },
      {
        candidate_id: CAND_B,
        display_name: "Bob Boone",
        score: 0,
        has_evidence: false,
        vetoed_by: [],
        per_issue: [],
      },
    ],
    measure_per_issue: [],
    unresearched: [{ candidate_id: CAND_B, display_name: "Bob Boone", never_researched: true }],
    ...overrides,
  };
}

function renderControl() {
  return renderRoutes([
    { path: "/", element: <AutoPickControl electionId={ELECTION_ID} /> },
    { path: "/login", element: <p>Login page</p> },
    { path: "/me/settings", element: <p>Settings page</p> },
  ]);
}

describe("AutoPickControl", () => {
  it("prompts guests to sign in without calling the API", async () => {
    mockMe = null;
    const fetchMock = stubApiRoutes({});
    renderControl();
    await userEvent.click(screen.getByRole("button", { name: "Pick for me" }));
    expect(await screen.findByRole("link", { name: "Sign in" })).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalledWith(expect.stringContaining("auto-picks"), expect.anything());
  });

  it("prompts for more ranked issues below the floor, linking to the issue editor", async () => {
    stubApiRoutes({
      // useMyResearchAreas resolves the session through the real useMe (the
      // vi.mock above only swaps the component's own import), so the identity
      // endpoint must answer for the preferences query to enable.
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": {
        body: { preferences: THREE_PREFERENCES.preferences.slice(0, 2) },
      },
    });
    renderControl();
    await userEvent.click(await screen.findByRole("button", { name: "Pick for me" }));
    const link = await screen.findByRole("link", { name: "Rank your issues" });
    expect(link).toHaveAttribute("href", "/me/settings");
  });

  it("runs the engine and renders the why-this-pick panel for a winner", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": { body: { results: [pickedResult()] } },
    });
    renderControl();
    await userEvent.click(await screen.findByRole("button", { name: "Pick for me" }));
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent("Picked Alice Alvarez — the best match for your issues.");
    expect(panel).toHaveTextContent("Housing · aligned");
    expect(panel).toHaveTextContent("Bob Boone (not researched yet)");
    const call = fetchMock.mock.calls.find(([input]) => String(input).includes("auto-picks"));
    expect(JSON.parse(String(call?.[1]?.body))).toEqual({
      election_ids: [ELECTION_ID],
      mode: "replace",
    });
  });

  it("explains an honest no-pick, naming the shortlist", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": {
        body: {
          results: [
            pickedResult({
              outcome: "no_pick",
              reason: "tie",
              picked_candidate_ids: [],
              shortlist_candidate_ids: [CAND_A, CAND_B],
              unresearched: [],
            }),
          ],
        },
      },
    });
    renderControl();
    await userEvent.click(await screen.findByRole("button", { name: "Pick for me" }));
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent(
      "It's a tie between Alice Alvarez and Bob Boone on your issues — your call between them."
    );
  });

  it("shows the vetoed candidate with the offending record", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": {
        body: {
          results: [
            pickedResult({
              candidates: [
                pickedResult().candidates[0]!,
                {
                  candidate_id: CAND_B,
                  display_name: "Bob Boone",
                  score: 0.5,
                  has_evidence: true,
                  vetoed_by: [
                    {
                      research_area_id: AREA_CLIMATE,
                      record_id: "cccccccc-0000-4000-8000-000000000001",
                      description: "Voted to repeal the emissions standard",
                    },
                  ],
                  per_issue: [],
                },
              ],
              unresearched: [],
            }),
          ],
        },
      },
    });
    renderControl();
    await userEvent.click(await screen.findByRole("button", { name: "Pick for me" }));
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent("Bob Boone excluded — crossed your line on Climate");
    expect(panel).toHaveTextContent("Voted to repeal the emissions standard");
  });

  it("answers a measure with the veto explanation", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": {
        body: {
          results: [
            pickedResult({
              race_type: "ballot_measure",
              reason: "veto",
              picked_candidate_ids: [],
              measure_position: "no",
              candidates: [],
              measure_per_issue: [{ research_area_id: AREA_CLIMATE, net: -1 }],
              unresearched: [],
            }),
          ],
        },
      },
    });
    renderControl();
    await userEvent.click(await screen.findByRole("button", { name: "Pick for me" }));
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent("Vote No — this measure goes against an issue you drew a line on.");
    expect(panel).toHaveTextContent("Climate · conflicts");
  });
});
