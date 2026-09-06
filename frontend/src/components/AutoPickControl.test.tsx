import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Link } from "react-router";
import type { AutoPickElectionResult, Me } from "@voteapp/api-client";
import { AutoPickControl } from "./AutoPickControl";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { renderRoutes } from "../test/render";

// Same mocking rationale as ElectionChoiceControls.test: the control forks
// on the session, and renderRoutes seeds nothing — a real useMe would stay
// loading forever.
const SIGNED_IN: Me = {
  id: "user-1",
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

function renderControl(seatsToFill: number | null = null, measure = false) {
  return renderRoutes(
    [
      {
        path: "/elections/:id",
        element: <AutoPickControl electionId={ELECTION_ID} seatsToFill={seatsToFill} measure={measure} />,
      },
      { path: "/register", element: <p>Register page</p> },
      { path: "/login", element: <p>Login page</p> },
      { path: "/me/settings", element: <p>Settings page</p> },
    ],
    `/elections/${ELECTION_ID}`
  );
}

/** The button disables while the preferences load; click only once ready. */
async function clickPickForMe() {
  const button = await screen.findByRole("button", { name: "Auto-pick by my issues" });
  await waitFor(() => expect(button).toBeEnabled());
  await userEvent.click(button);
}

describe("AutoPickControl", () => {
  // Guests get the pitch instead of the button: a plain-words question
  // that opens the shared log in / sign up dialog, both links carrying
  // this page as the post-auth return path so the visitor lands back on
  // the race. The auto-pick button itself never renders for them (issue
  // preferences are account-only).
  it("shows guests a teaser that opens the sign-up dialog", async () => {
    mockMe = null;
    stubApiRoutes({});
    renderControl();
    expect(screen.queryByRole("button", { name: "Auto-pick by my issues" })).not.toBeInTheDocument();
    await userEvent.setup().click(screen.getByRole("button", { name: "Which candidate best matches your values?" }));
    expect(
      await screen.findByText(
        "Sign up to pick the issues you care about, and see which candidate best matches what you believe. Signing up is free."
      )
    ).toBeInTheDocument();
    const next = encodeURIComponent(`/elections/${ELECTION_ID}`);
    expect(screen.getByRole("link", { name: "Sign up" })).toHaveAttribute("href", `/register?next=${next}`);
    expect(screen.getByRole("link", { name: "Log in" })).toHaveAttribute("href", `/login?next=${next}`);
  });

  it("asks about the measure, not a candidate, on a ballot measure", async () => {
    mockMe = null;
    stubApiRoutes({});
    renderControl(null, true);
    expect(screen.queryByRole("button", { name: /Which candidate/ })).not.toBeInTheDocument();
    await userEvent.setup().click(screen.getByRole("button", { name: "Does this measure match your values?" }));
    expect(await screen.findByText(/see whether this measure matches what you believe/)).toBeInTheDocument();
  });

  it("renders nothing while the session is still resolving", () => {
    mockMe = undefined;
    stubApiRoutes({});
    renderControl();
    expect(screen.queryByRole("button", { name: "Auto-pick by my issues" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /your values/ })).not.toBeInTheDocument();
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
    await clickPickForMe();
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
    await clickPickForMe();
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

  // A run started on election A must not reach into election B's page:
  // the control remounts per election, and the stale onPicked would clear
  // B's party filter. The usage outcome still lands (tracked elsewhere).
  it("drops the onPicked callback when the control unmounts before the engine answers", async () => {
    let answer: (() => void) | null = null;
    const held = new Promise<{ body: unknown }>((resolve) => {
      answer = () => resolve({ body: { results: [pickedResult()] } });
    });
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": () => held,
    });
    const onPicked = vi.fn();
    renderRoutes([
      {
        path: "/",
        element: (
          <>
            <AutoPickControl electionId={ELECTION_ID} seatsToFill={null} onPicked={onPicked} />
            <Link to="/away">Away</Link>
          </>
        ),
      },
      { path: "/away", element: <p>Away page</p> },
    ]);
    await clickPickForMe();
    await userEvent.click(screen.getByRole("link", { name: "Away" }));
    await screen.findByText("Away page");
    answer!();
    await act(async () => {
      await held;
    });
    expect(onPicked).not.toHaveBeenCalled();
  });

  it("flags the open seats when a multi-seat pick fills only some of them", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": { body: { results: [pickedResult()] } },
    });
    renderControl(2);
    await clickPickForMe();
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent(
      "Picked Alice Alvarez — the best match for your issues. One seat is still open: nothing known separates the other candidates, so those picks are yours to make."
    );
  });

  it("lets the backend decide the issue floor when the preferences fetch failed", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": apiError(500, "internal_error", "boom"),
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": {
        body: {
          results: [
            pickedResult({
              outcome: "no_pick",
              reason: "too_few_issues",
              picked_candidate_ids: [],
              candidates: [],
              unresearched: [],
            }),
          ],
        },
      },
    });
    renderControl();
    await clickPickForMe();
    // No client-side "rank your issues" misdirection from the errored empty
    // list — the POST ran and the backend's authoritative reason rendered.
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent("Rank at least 3 issues first");
    expect(screen.queryByRole("link", { name: "Rank your issues" })).not.toBeInTheDocument();
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
    await clickPickForMe();
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
    await clickPickForMe();
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
    await clickPickForMe();
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent("Vote No — this measure goes against an issue you drew a line on.");
    expect(panel).toHaveTextContent("Climate · conflicts");
  });

  // The two measure no-answer flavors are distinct outcomes with distinct
  // remedies — the panel must say which one happened.
  it("tells a measure voter the measure isn't tagged with their issues", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": {
        body: {
          results: [
            pickedResult({
              race_type: "ballot_measure",
              outcome: "no_pick",
              reason: "insufficient_evidence",
              picked_candidate_ids: [],
              candidates: [],
              measure_per_issue: [],
              unresearched: [],
            }),
          ],
        },
      },
    });
    renderControl();
    await clickPickForMe();
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent("No answer — this measure isn't tagged with any of your issues yet.");
  });

  it("tells a measure voter when their issues cancel out", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": { body: THREE_PREFERENCES },
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": {
        body: {
          results: [
            pickedResult({
              race_type: "ballot_measure",
              outcome: "no_pick",
              reason: "insufficient_evidence",
              picked_candidate_ids: [],
              candidates: [],
              measure_per_issue: [
                { research_area_id: AREA_HOUSING, net: 1 },
                { research_area_id: AREA_CLIMATE, net: -1 },
              ],
              unresearched: [],
            }),
          ],
        },
      },
    });
    renderControl();
    await clickPickForMe();
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent(
      "No answer — this measure helps some of your issues and hurts others about equally, so it's your call."
    );
    expect(panel).toHaveTextContent("Housing · aligned");
    expect(panel).toHaveTextContent("Climate · conflicts");
  });

  // Race-type-independent reasons must survive the measure branch: a
  // failed preferences fetch defers the issue floor to the backend, and
  // "isn't tagged" would misdirect a voter who needs to rank issues.
  it("renders the backend issue floor on a measure, not the tagging message", async () => {
    stubApiRoutes({
      "/api/me": { body: { user: SIGNED_IN } },
      "/api/me/research-area-preferences": apiError(500, "internal_error", "boom"),
      "/api/me/election-choices": { body: { choices: [] } },
      "/api/me/auto-picks": {
        body: {
          results: [
            pickedResult({
              race_type: "ballot_measure",
              outcome: "no_pick",
              reason: "too_few_issues",
              picked_candidate_ids: [],
              candidates: [],
              measure_per_issue: [],
              unresearched: [],
            }),
          ],
        },
      },
    });
    renderControl();
    await clickPickForMe();
    const panel = await screen.findByRole("region", { name: "Why this pick" });
    expect(panel).toHaveTextContent("Rank at least 3 issues first");
    expect(panel).not.toHaveTextContent("isn't tagged");
  });
});
