import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ElectionPage, ErrorBoundary } from "./ElectionPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { electionDetail, financeSummary, ME_VERIFIED, VOTE_POWER_WITH_EXPLANATION } from "../test/fixtures";
import type { ElectionDetail } from "@voteapp/api-client";

const ANONYMOUS = { "/api/me": apiError(401, "unauthorized", "Not logged in") };

// The subject arrives via the route loader (server-fetched in production);
// tests supply it directly instead of stubbing the loader's fetch.
// `state` simulates arriving with nav context (see detailNavContext.ts).
function renderElection(
  loader: (args: { params: { electionId?: string } }) => unknown,
  id = "e-1",
  state?: unknown
) {
  return renderRoutes(
    [
      {
        path: "/elections/:electionId",
        element: <ElectionPage />,
        errorElement: <ErrorBoundary />,
        hydrateFallbackElement: <p />,
        loader,
      },
      { path: "/candidates/:candidateId", element: <p /> },
      { path: "/ballot", element: <p /> },
      { path: "/disclaimer", element: <p /> },
    ],
    state === undefined ? `/elections/${id}` : { pathname: `/elections/${id}`, state }
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ElectionPage", () => {
  it("renders not-found UI when the loader throws a 404", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => {
      throw new Response("Not Found", { status: 404 });
    }, "e-missing");
    expect(await screen.findByText("Election not found")).toBeInTheDocument();
  });

  it("renders the election header and every candidate", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail());

    expect(await screen.findByRole("heading", { name: "Governor" })).toBeInTheDocument();
    expect(screen.getByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getByText("Riley Runner")).toBeInTheDocument();
    // The candidate list carries no follow controls; following lives on the
    // candidate profile page.
    expect(screen.queryByRole("button", { name: /follow/i })).not.toBeInTheDocument();
    // The report button sits at the end of the page, after the candidates.
    const reportButton = screen.getByRole("button", { name: "Report an issue with election" });
    const candidatesHeading = screen.getByRole("heading", { name: "Candidates" });
    expect(candidatesHeading.compareDocumentPosition(reportButton) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("renders a collapsed vote power explanation when the detail payload carries one", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ vote_power: VOTE_POWER_WITH_EXPLANATION }));

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.getByText("How do we calculate vote impact?")).toBeInTheDocument();
    // Native <details> keeps content in the DOM while collapsed; the backend
    // copy must arrive verbatim.
    expect(screen.getByText("Vote impact = representation + decisiveness.")).toBeInTheDocument();
    // Each part renders formula-style: title, grade, stat, then the detail.
    expect(screen.getByText("Representation:")).toBeInTheDocument();
    expect(screen.getByText("· 50 out of 100")).toBeInTheDocument();
    expect(screen.getByText("· 3.3-point margin in 2022")).toBeInTheDocument();
    expect(
      screen.getByText("Past results here were very close — a small number of votes could decide the winner.")
    ).toBeInTheDocument();
    expect(screen.getByText("Average representation + high decisiveness → High vote impact.")).toBeInTheDocument();
    expect(screen.getByText("Some data is missing.")).toBeInTheDocument();
    // The exact formula renders when the backend provides one; the null
    // formula on the other part must not render an empty line.
    expect(screen.getByText("score = 100 × ln(9,808,667 ÷ 104,650) ÷ ln(9,808,667 ÷ 1,204) = 50")).toBeInTheDocument();
  });

  it("omits the vote power explanation when the payload has none or the label is unknown", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail());

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.queryByText("How do we calculate vote impact?")).not.toBeInTheDocument();
  });

  it("hides the vote power explanation entirely for an unknown label", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        vote_power: { ...VOTE_POWER_WITH_EXPLANATION, label: "unknown", score: null },
      })
    );

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.queryByText(/Vote impact:/)).not.toBeInTheDocument();
    expect(screen.queryByText("How do we calculate vote impact?")).not.toBeInTheDocument();
  });

  it("shows the seat count for multi-seat contests and hides it otherwise", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ seats_to_fill: 3 }));

    expect(await screen.findByRole("heading", { name: "Governor" })).toBeInTheDocument();
    expect(screen.getByText(/3 seats/)).toBeInTheDocument();
  });

  it("shows no seat count when seats_to_fill is absent or 1", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ seats_to_fill: 1 }));

    expect(await screen.findByRole("heading", { name: "Governor" })).toBeInTheDocument();
    expect(screen.queryByText(/seats/)).not.toBeInTheDocument();
  });

  it("hides the party filter when the roster spans a single bucket", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    // Default fixture: two Independent candidates — one "other" bucket.
    renderElection(() => electionDetail());

    await screen.findByRole("heading", { name: "Candidates" });
    expect(screen.queryByRole("group", { name: "Filter candidates by party" })).not.toBeInTheDocument();
  });

  it("filters candidates by party bucket and restores with All", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const user = userEvent.setup();
    renderElection(() =>
      electionDetail({
        candidates: [
          {
            candidate_id: "c-1",
            display_name: "Dana Democrat",
            party: "Democratic",
            is_incumbent: false,
            status: "active",
            summary: null,
            finance_summary: null,
            records: [],
          },
          {
            candidate_id: "c-2",
            display_name: "Riley Republican",
            party: "Republican",
            is_incumbent: false,
            status: "active",
            summary: null,
            finance_summary: null,
            records: [],
          },
          {
            candidate_id: "c-3",
            // The registration label buckets with its party for filtering,
            // even though storage keeps it distinct.
            display_name: "Alex Alaskan",
            party: "Registered Democrat",
            is_incumbent: false,
            status: "active",
            summary: null,
            finance_summary: null,
            records: [],
          },
          {
            candidate_id: "c-4",
            display_name: "Indy Other",
            party: "Independent",
            is_incumbent: false,
            status: "active",
            summary: null,
            finance_summary: null,
            records: [],
          },
        ],
      })
    );

    await screen.findByRole("group", { name: "Filter candidates by party" });
    // Chips carry counts; All is pressed by default.
    expect(screen.getByRole("button", { name: "All (4)" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "Democrats (2)" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Republicans (1)" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Other (1)" })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Democrats (2)" }));
    expect(screen.getByText("Dana Democrat")).toBeInTheDocument();
    expect(screen.getByText("Alex Alaskan")).toBeInTheDocument();
    expect(screen.queryByText("Riley Republican")).not.toBeInTheDocument();
    expect(screen.queryByText("Indy Other")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "All (4)" }));
    expect(screen.getByText("Riley Republican")).toBeInTheDocument();
    expect(screen.getByText("Indy Other")).toBeInTheDocument();
  });

  it("resets the party filter when navigating to a different election", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const user = userEvent.setup();
    const candidate = (id: string, name: string, party: string) => ({
      candidate_id: id,
      display_name: name,
      party,
      is_incumbent: false,
      status: "active",
      summary: null,
      finance_summary: null,
      records: [],
    });
    // Same route stays mounted across param changes; both rosters offer the
    // "Democrats" bucket, so a leaked pick WOULD apply — and must not.
    const { router } = renderElection(({ params }) =>
      params.electionId === "e-2"
        ? electionDetail({
            id: "e-2",
            candidates: [candidate("c-5", "Casey Second", "Democratic"), candidate("c-6", "Robin Second", "Republican")],
          })
        : electionDetail({
            id: "e-1",
            candidates: [candidate("c-1", "Dana First", "Democratic"), candidate("c-2", "Riley First", "Republican")],
          })
    );

    await user.click(await screen.findByRole("button", { name: "Democrats (1)" }));
    expect(screen.queryByText("Riley First")).not.toBeInTheDocument();

    await router.navigate("/elections/e-2");
    // Fresh election, fresh filter: both candidates visible, All pressed.
    expect(await screen.findByText("Robin Second")).toBeInTheDocument();
    expect(screen.getByText("Casey Second")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "All (2)" })).toHaveAttribute("aria-pressed", "true");
  });

  it("explains an empty candidate list instead of hiding the section", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        candidates: [],
        candidate_roster_status: { reason: "awaiting_official_roster", check_after: "2026-08-27" },
      })
    );

    expect(await screen.findByRole("heading", { name: "Candidates" })).toBeInTheDocument();
    expect(
      screen.getByText(
        "Election officials haven't published a final candidate list for this race. We'll check again after August 27, 2026."
      )
    ).toBeInTheDocument();
  });

  it("hides the candidates section entirely when empty with no roster status", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ candidates: [], candidate_roster_status: null }));

    expect(await screen.findByRole("heading", { name: "Governor" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Candidates" })).not.toBeInTheDocument();
  });

  it("falls back to generic copy for an unknown roster status reason", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        candidates: [],
        candidate_roster_status: { reason: "some_future_reason", check_after: null },
      })
    );

    expect(await screen.findByText("Candidate information for this race isn't available yet.")).toBeInTheDocument();
  });

  it("renders no campaign finance on candidate cards, even when the payload carries it", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const detail = electionDetail();
    detail.candidates[0].finance_summary = financeSummary();
    renderElection(() => detail);

    // Finance lives on the candidate profile page only — no disclosure, no
    // "Raised $X" chip here.
    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.queryByText("Campaign finance")).not.toBeInTheDocument();
    expect(screen.queryByText(/Raised \$/)).not.toBeInTheDocument();
    expect(screen.queryByText("Top disclosed occupations of direct donors")).not.toBeInTheDocument();
    expect(screen.queryByText("Growth PAC")).not.toBeInTheDocument();
    // The card link to the profile survives.
    const cardLink = screen.getByRole("link", { name: /Jordan Voter/ });
    expect(cardLink).toHaveAttribute("href", "/candidates/c-1");
  });

  it("shows no follow buttons in the candidate list even for verified users", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
    });
    renderElection(() => electionDetail());

    // Following moved to the candidate profile page; the list stays a pure
    // navigation surface.
    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Follow" })).not.toBeInTheDocument();
    const cardLink = screen.getByRole("link", { name: "Jordan Voter" });
    expect(cardLink).toHaveAttribute("href", "/candidates/c-1");
  });

  it("renders ballot measure yes/no explanations when present", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
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
          results: [],
        },
      })
    );

    expect(await screen.findByText("Yes approves the bond.")).toBeInTheDocument();
    expect(screen.getByText("No rejects the bond.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Report an issue with ballot measure" })).toBeInTheDocument();
  });

  it("renders candidate stance chips as +N/-N colored by direction", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const record = (id: string, tag: { research_area_id: string; slug: string; name: string; stance: "for" | "against" | null }) => ({
      id,
      description: "A record.",
      source_url: "https://example.gov/record",
      event_date: "2025-01-15",
      created_at: "2025-02-01T00:00:00.000Z",
      research_area_tags: [tag],
    });
    const detail = electionDetail();
    detail.candidates[0].records = [
      record("r-1", { research_area_id: "a-1", slug: "civil_rights", name: "Civil Rights", stance: "for" }),
      record("r-2", { research_area_id: "a-2", slug: "housing_affordability", name: "Housing Affordability", stance: "against" }),
      record("r-3", { research_area_id: "a-2", slug: "housing_affordability", name: "Housing Affordability", stance: "against" }),
      record("r-4", { research_area_id: "a-3", slug: "gun_control", name: "Gun Control", stance: "for" }),
      record("r-5", { research_area_id: "a-3", slug: "gun_control", name: "Gun Control", stance: "against" }),
      record("r-6", { research_area_id: "a-3", slug: "gun_control", name: "Gun Control", stance: "against" }),
    ];
    renderElection(() => detail);

    // All-for compresses to +N in green, all-against to -N in red, and a
    // mixed area shows both counts in amber.
    const forChip = (await screen.findByText("Civil Rights")).closest("span")!;
    expect(forChip.textContent).toContain("+1");
    expect(forChip.className).toContain("text-green-900");
    const againstChip = screen.getByText("Housing Affordability").closest("span")!;
    expect(againstChip.textContent).toContain("-2");
    expect(againstChip.className).toContain("text-red-900");
    const mixedChip = screen.getByText("Gun Control").closest("span")!;
    expect(mixedChip.textContent).toContain("+1 -2");
    expect(mixedChip.className).toContain("text-amber-900");
    // Screen readers hear spelled-out counts — "-2" alone can be read as "2".
    expect(mixedChip.textContent).toContain("1 for, 2 against");
    // The row is labelled: an issue name and a bare number say nothing about
    // what was counted.
    expect(forChip.closest("p")).toHaveTextContent("Records:");
  });

  it("colors measure research-area chips by stance", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
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
          research_area_tags: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", stance: "for" },
            { research_area_id: "a-2", slug: "civil_rights", name: "Civil Rights", stance: "against" },
            { research_area_id: "a-3", slug: "gun_control", name: "Gun Control", stance: null },
          ],
          results: [],
        },
      })
    );

    // The measure works FOR housing (green, under "Supports:"), AGAINST
    // civil rights (red, under "Opposes:"); a stanceless tag stays muted
    // under "Affects:". The direction is the visible group label, not just
    // color, so color-blind readers can tell the groups apart too.
    const forChip = await screen.findByText("Housing Affordability");
    expect(forChip.className).toContain("text-green-900");
    expect(screen.getByText("Supports:")).toBeInTheDocument();
    const againstChip = screen.getByText("Civil Rights");
    expect(againstChip.className).toContain("text-red-900");
    expect(screen.getByText("Opposes:")).toBeInTheDocument();
    const neutralChip = screen.getByText("Gun Control");
    expect(neutralChip.className).toContain("text-ink-soft");
    expect(screen.getByText("Affects:")).toBeInTheDocument();
  });

  it("marks saved areas with an sr-only cue on measure and candidate chips", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null, rank: 1 },
          ],
        },
      },
    });
    const detail = electionDetail({
      race_type: "ballot_measure",
      ballot_measure: {
        id: "m-1",
        official_ballot_title: "Measure 1",
        summary: "A measure.",
        what_yes_means: "Yes approves the bond.",
        what_no_means: "No rejects the bond.",
        result: null,
        source_urls: [],
        official_measure_url: null,
        research_area_tags: [
          { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", stance: "for" },
        ],
        results: [],
      },
    });
    detail.candidates[0].records = [
      {
        id: "r-1",
        description: "A record.",
        source_url: "https://example.gov/record",
        event_date: "2025-01-15",
        created_at: "2025-02-01T00:00:00.000Z",
        research_area_tags: [
          { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", stance: "for" },
        ],
      },
    ];
    renderElection(() => detail);

    // Both chip surfaces voice saved-ness for assistive tech, matching the
    // ElectionCard precedent. The cue arrives with the async preferences
    // fetch, after the loader-fed chips render — so wait on the cue itself.
    const savedCues = await screen.findAllByText("(saved)");
    expect(savedCues).toHaveLength(2);
    // Both surfaces now render the bare area name; the measure chip lives
    // under the "Supports:" group label, the candidate chip carries counts.
    const chips = screen.getAllByText("Housing Affordability");
    expect(chips).toHaveLength(2);
    const measureChip = chips.find((chip) => chip.closest("p")?.textContent?.includes("Supports:"))!;
    expect(measureChip).toHaveTextContent("(saved)");
    const candidateChip = chips
      .find((chip) => !chip.closest("p")?.textContent?.includes("Supports:"))!
      .closest("span")!;
    expect(candidateChip).toHaveTextContent("1 for");
    expect(candidateChip).toHaveTextContent("(saved)");
  });

  it("renders the office description before the affects row, in public-salience order", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        office: {
          id: "o-1",
          scope: "statewide",
          canonical_name: "Governor",
          summary: "Running the state government\nProposing the state budget",
        },
        // Alphabetical (API order) on purpose: the page must re-order by
        // public salience, which puts Environment ahead of Civil Rights.
        research_areas: [
          { id: "a-civ", slug: "civil_rights", name: "Civil Rights", description: null },
          { id: "a-env", slug: "environment_and_public_health", name: "Environment & Public Health", description: null },
        ],
      })
    );

    expect(
      await screen.findByRole("heading", { name: "Governor is responsible for:" })
    ).toBeInTheDocument();
    // Newline-separated duties render as individual bullets.
    expect(screen.getByText("Running the state government")).toBeInTheDocument();
    const description = screen.getByText("Proposing the state budget");
    const label = screen.getByText("Affects:");
    expect(description.compareDocumentPosition(label) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    const environment = screen.getByText("Environment & Public Health");
    const civilRights = screen.getByText("Civil Rights");
    expect(environment.compareDocumentPosition(civilRights) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("falls back to a neutral heading for catalog bucket office names", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        office: {
          id: "o-2",
          scope: "state_lower",
          canonical_name: "State Lower Chamber Legislator",
          summary: "Voting on state laws and the state budget",
        },
      })
    );

    expect(
      await screen.findByRole("heading", { name: "This office is responsible for:" })
    ).toBeInTheDocument();
    expect(screen.queryByText(/State Lower Chamber Legislator is responsible/)).not.toBeInTheDocument();
  });

  it("shows logged-out visitors pick buttons that prompt them to register", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail());

    // One per active candidate, same placement as the real control — and each
    // accessible name carries its candidate, so screen-reader button lists and
    // voice control can tell the page's pick buttons apart.
    const jordanPick = await screen.findByRole("button", { name: "Make my pick: Jordan Voter" });
    expect(screen.getByRole("button", { name: "Make my pick: Riley Runner" })).toBeInTheDocument();
    await userEvent.setup().click(jordanPick);

    expect(
      await screen.findByText(
        "Save Jordan Voter as your election pick and keep your whole ballot in one place. Signing up is free."
      )
    ).toBeInTheDocument();
    // Both links carry the election page as the post-auth return path.
    expect(screen.getByRole("link", { name: "Sign up" })).toHaveAttribute(
      "href",
      "/register?next=%2Felections%2Fe-1"
    );
    expect(screen.getByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Felections%2Fe-1"
    );
  });

  it("shows logged-out visitors no pick buttons on past elections", async () => {
    // The backend rejects choice writes to past elections, so the register
    // prompt would advertise an action the visitor could never complete.
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ election_date: "2020-11-03" }));

    expect(await screen.findByRole("heading", { name: "Governor" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Make my pick/ })).not.toBeInTheDocument();
  });

  it("gives logged-in viewers the real pick button, not the register prompt", async () => {
    const fetchMock = stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/election-choices": (_url, init) => {
        if (init?.method === "PUT") {
          expect(JSON.parse(String(init.body))).toEqual({
            election_id: "e-1",
            candidate_id: "c-1",
            chosen: true,
          });
          return { status: 200, body: { choice: null } };
        }
        return { status: 200, body: { choices: [] } };
      },
    });
    renderElection(() => electionDetail());

    const pickButton = await screen.findByRole("button", { name: "Make my pick: Jordan Voter" });
    await userEvent.setup().click(pickButton);

    // The click writes a choice instead of opening the register dialog.
    await waitFor(() =>
      expect(fetchMock.mock.calls.some(([, init]) => (init as RequestInit | undefined)?.method === "PUT")).toBe(true)
    );
    expect(screen.queryByRole("link", { name: "Sign up" })).not.toBeInTheDocument();
  });

  it("puts the viewer's saved areas first with an sr-only cue", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-civ", slug: "civil_rights", name: "Civil Rights", description: null, rank: 1 },
          ],
        },
      },
    });
    renderElection(() =>
      electionDetail({
        research_areas: [
          { id: "a-civ", slug: "civil_rights", name: "Civil Rights", description: null },
          { id: "a-env", slug: "environment_and_public_health", name: "Environment & Public Health", description: null },
        ],
      })
    );

    // Saved-ness arrives with the async preferences fetch; wait on the cue.
    const savedCue = await screen.findByText("(saved)");
    expect(savedCue.parentElement?.textContent).toContain("Civil Rights");
    // Saved Civil Rights outranks the globally higher-salience Environment.
    const civilRights = screen.getByText("Civil Rights");
    const environment = screen.getByText("Environment & Public Health");
    expect(civilRights.compareDocumentPosition(environment) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("shows no office section on ballot measure elections", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        race_type: "ballot_measure",
        candidates: [],
        // The measure's areas arrive on research_areas too; the measure
        // section owns rendering them (with stance), so the office row
        // must not duplicate the chips.
        research_areas: [
          { id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null },
        ],
        ballot_measure: {
          id: "m-1",
          official_ballot_title: "Measure 1",
          summary: "A measure.",
          what_yes_means: "Yes approves the bond.",
          what_no_means: "No rejects the bond.",
          result: null,
          source_urls: [],
          official_measure_url: null,
          research_area_tags: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", stance: "for" },
          ],
          results: [],
        },
      })
    );

    await screen.findByRole("heading", { name: "Ballot Measure" });
    expect(screen.queryByText("About this office")).not.toBeInTheDocument();
    expect(screen.queryByText("Affects:")).not.toBeInTheDocument();
  });

  it("defaults to my-issues order for viewers with saved areas and can switch to alphabetical", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null, rank: 1 },
          ],
        },
      },
    });
    const detail = electionDetail();
    // Riley (second in the alphabetical payload) is the only match on the
    // viewer's saved area, so the my-issues default must lift them first.
    detail.candidates[1].records = [
      {
        id: "r-1",
        description: "A record.",
        source_url: "https://example.gov/record",
        event_date: "2025-01-15",
        created_at: "2025-02-01T00:00:00.000Z",
        research_area_tags: [
          { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", stance: "for" },
        ],
      },
    ];
    renderElection(() => detail);

    // The dropdown only renders once the async preferences arrive.
    const select = await screen.findByRole("combobox");
    expect(select).toHaveValue("my_issues");
    expect(screen.getByRole("option", { name: "My issues first" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Alphabetical" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /against my issues/i })).not.toBeInTheDocument();
    const riley = screen.getByText("Riley Runner");
    const jordan = screen.getByText("Jordan Voter");
    expect(riley.compareDocumentPosition(jordan) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    const user = userEvent.setup();
    await user.selectOptions(select, "alphabetical");
    expect(
      screen.getByText("Jordan Voter").compareDocumentPosition(screen.getByText("Riley Runner")) &
        Node.DOCUMENT_POSITION_FOLLOWING
    ).toBeTruthy();
  });

  it("ranks an against-only candidate above one with no relevant records under my-issues sort", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null, rank: 1 },
          ],
        },
      },
    });
    const detail = electionDetail();
    // "My issues first" sorts by relevance, not agreement: Riley's record is
    // AGAINST the saved issue, but it is still a track record on it, so Riley
    // must outrank record-less Jordan rather than tying at zero.
    detail.candidates[1].records = [
      {
        id: "r-1",
        description: "A record.",
        source_url: "https://example.gov/record",
        event_date: "2025-01-15",
        created_at: "2025-02-01T00:00:00.000Z",
        research_area_tags: [
          { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", stance: "against" },
        ],
      },
    ];
    renderElection(() => detail);

    await screen.findByRole("combobox");
    const riley = screen.getByText("Riley Runner");
    const jordan = screen.getByText("Jordan Voter");
    expect(riley.compareDocumentPosition(jordan) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  describe("records filter", () => {
    const SAVED_HOUSING = {
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", description: null, rank: 1 },
          ],
        },
      },
    };
    const candidate = (id: string, name: string, party: string, records: ReturnType<typeof housingRecord>[] = []) => ({
      candidate_id: id,
      display_name: name,
      party,
      is_incumbent: false,
      status: "active",
      summary: null,
      finance_summary: null,
      records,
    });
    const housingRecord = (id: string) => ({
      id,
      description: "A record.",
      source_url: "https://example.gov/record",
      event_date: "2025-01-15",
      created_at: "2025-02-01T00:00:00.000Z",
      research_area_tags: [
        { research_area_id: "a-1", slug: "housing_affordability", name: "Housing Affordability", stance: "for" as const },
      ],
    });

    it("hides the control for signed-out viewers", async () => {
      stubApiRoutes({ ...ANONYMOUS });
      const detail = electionDetail();
      detail.candidates[0].records = [housingRecord("r-1")];
      renderElection(() => detail);

      await screen.findByRole("heading", { name: "Candidates" });
      expect(screen.queryByRole("button", { name: "Has a record on my issues" })).not.toBeInTheDocument();
    });

    it("hides the control when the viewer has no saved areas", async () => {
      stubApiRoutes({
        "/api/me": { body: ME_VERIFIED },
        "/api/me/candidate-follows": { body: { follows: [] } },
        "/api/me/research-area-preferences": { body: { preferences: [] } },
      });
      const detail = electionDetail();
      detail.candidates[0].records = [housingRecord("r-1")];
      renderElection(() => detail);

      await screen.findByRole("heading", { name: "Candidates" });
      expect(screen.queryByRole("button", { name: "Has a record on my issues" })).not.toBeInTheDocument();
    });

    it("hides the control when no candidate matches", async () => {
      stubApiRoutes({ ...SAVED_HOUSING });
      // Default fixture: no candidate has records → filtering would empty
      // the list, so the (off) toggle has nothing to offer.
      renderElection(() => electionDetail());

      // The sort dropdown proves the async preferences have arrived.
      await screen.findByRole("combobox");
      expect(screen.queryByRole("button", { name: "Has a record on my issues" })).not.toBeInTheDocument();
    });

    it("hides the control when every candidate matches", async () => {
      stubApiRoutes({ ...SAVED_HOUSING });
      // All-match: filtering would be a no-op, so the (off) toggle has
      // nothing to offer either.
      renderElection(() =>
        electionDetail({
          candidates: [
            candidate("c-1", "Casey Record", "Independent", [housingRecord("r-1")]),
            candidate("c-2", "Jamie Record", "Independent", [housingRecord("r-2")]),
          ],
        })
      );

      await screen.findByRole("combobox");
      expect(screen.queryByRole("button", { name: "Has a record on my issues" })).not.toBeInTheDocument();
    });

    it("filters to matching candidates, shows the hidden count, and clears with Show all", async () => {
      stubApiRoutes({ ...SAVED_HOUSING });
      const user = userEvent.setup();
      renderElection(() =>
        electionDetail({
          candidates: [
            candidate("c-1", "Casey Record", "Independent", [housingRecord("r-1")]),
            candidate("c-2", "Jamie Quiet", "Independent"),
            candidate("c-3", "Robin Quiet", "Independent"),
          ],
        })
      );

      const toggle = await screen.findByRole("button", { name: "Has a record on my issues" });
      expect(toggle).toHaveAttribute("aria-pressed", "false");

      await user.click(toggle);
      expect(screen.getByText("Casey Record")).toBeInTheDocument();
      expect(screen.queryByText("Jamie Quiet")).not.toBeInTheDocument();
      expect(screen.queryByText("Robin Quiet")).not.toBeInTheDocument();
      // The hidden count keeps the filtered list from looking like the full
      // roster — no records ≠ no stances.
      expect(screen.getByText(/2 candidates hidden/)).toBeInTheDocument();

      await user.click(screen.getByRole("button", { name: "Show all" }));
      expect(screen.getByText("Jamie Quiet")).toBeInTheDocument();
      expect(screen.getByText("Robin Quiet")).toBeInTheDocument();
      expect(screen.queryByText(/hidden/)).not.toBeInTheDocument();
    });

    it("composes with the party filter and counts hidden within the party view", async () => {
      stubApiRoutes({ ...SAVED_HOUSING });
      const user = userEvent.setup();
      renderElection(() =>
        electionDetail({
          candidates: [
            candidate("c-1", "Dana Record", "Democratic", [housingRecord("r-1")]),
            candidate("c-2", "Devon Quiet", "Democratic"),
            candidate("c-3", "Riley Record", "Republican", [housingRecord("r-2")]),
            candidate("c-4", "Rory Quiet", "Republican"),
          ],
        })
      );

      await user.click(await screen.findByRole("button", { name: "Democrats (2)" }));
      await user.click(screen.getByRole("button", { name: "Has a record on my issues" }));
      expect(screen.getByText("Dana Record")).toBeInTheDocument();
      expect(screen.queryByText("Devon Quiet")).not.toBeInTheDocument();
      // Count is relative to the party view: one Democrat hidden, not three
      // candidates — the party chips already account for their own hiding.
      expect(screen.getByText(/1 candidate hidden/)).toBeInTheDocument();

      // The toggle is a per-race choice, not a per-party one: it stays on
      // across a party switch within the same race.
      await user.click(screen.getByRole("button", { name: "Republicans (2)" }));
      expect(screen.getByText("Riley Record")).toBeInTheDocument();
      expect(screen.queryByText("Rory Quiet")).not.toBeInTheDocument();
      expect(screen.getByText(/1 candidate hidden/)).toBeInTheDocument();
    });

    it("keeps an active filter applied when a party switch leaves no matches", async () => {
      stubApiRoutes({ ...SAVED_HOUSING });
      const user = userEvent.setup();
      // Only Democrats have matching records. An active filter must not
      // silently stop applying when the view moves to Republicans — that
      // would show a full roster the viewer believes is filtered. It stays
      // visible and honestly empties the view, with the hidden count and
      // Show all explaining the empty list.
      renderElection(() =>
        electionDetail({
          candidates: [
            candidate("c-1", "Dana Record", "Democratic", [housingRecord("r-1")]),
            candidate("c-2", "Devon Quiet", "Democratic"),
            candidate("c-3", "Rory Quiet", "Republican"),
            candidate("c-4", "Robin Quiet", "Republican"),
          ],
        })
      );

      await user.click(await screen.findByRole("button", { name: "Democrats (2)" }));
      await user.click(screen.getByRole("button", { name: "Has a record on my issues" }));
      expect(screen.getByText("Dana Record")).toBeInTheDocument();

      await user.click(screen.getByRole("button", { name: "Republicans (2)" }));
      // Toggle still visible and pressed; every Republican is hidden.
      expect(screen.getByRole("button", { name: "Has a record on my issues" })).toHaveAttribute(
        "aria-pressed",
        "true"
      );
      expect(screen.queryByText("Rory Quiet")).not.toBeInTheDocument();
      expect(screen.queryByText("Robin Quiet")).not.toBeInTheDocument();
      expect(screen.getByText(/2 candidates hidden/)).toBeInTheDocument();

      await user.click(screen.getByRole("button", { name: "Show all" }));
      expect(screen.getByText("Rory Quiet")).toBeInTheDocument();
      expect(screen.getByText("Robin Quiet")).toBeInTheDocument();
    });

    it("resets the records filter when navigating to a different election", async () => {
      stubApiRoutes({ ...SAVED_HOUSING });
      const user = userEvent.setup();
      // Both rosters split into matched + unmatched, so a leaked pick WOULD
      // apply on e-2 — and must not.
      const { router } = renderElection(({ params }) =>
        params.electionId === "e-2"
          ? electionDetail({
              id: "e-2",
              candidates: [
                candidate("c-3", "Casey Second", "Independent", [housingRecord("r-2")]),
                candidate("c-4", "Robin Second", "Independent"),
              ],
            })
          : electionDetail({
              id: "e-1",
              candidates: [
                candidate("c-1", "Dana First", "Independent", [housingRecord("r-1")]),
                candidate("c-2", "Riley First", "Independent"),
              ],
            })
      );

      await user.click(await screen.findByRole("button", { name: "Has a record on my issues" }));
      expect(screen.queryByText("Riley First")).not.toBeInTheDocument();

      await router.navigate("/elections/e-2");
      // Fresh election, fresh filter: both candidates visible, toggle off.
      expect(await screen.findByText("Robin Second")).toBeInTheDocument();
      expect(screen.getByText("Casey Second")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Has a record on my issues" })).toHaveAttribute(
        "aria-pressed",
        "false"
      );
    });
  });

  it("renders measure result rows, including election-night outcomes", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        race_type: "ballot_measure",
        candidates: [],
        ballot_measure: {
          id: "m-1",
          official_ballot_title: "Measure 1",
          summary: "A measure.",
          what_yes_means: "Yes approves the bond.",
          what_no_means: "No rejects the bond.",
          // Canonical result unset: election-night outcomes never project it,
          // yet the rows below must still be visible.
          result: null,
          source_urls: [],
          official_measure_url: null,
          research_area_tags: [],
          results: [
            {
              id: "mr-1",
              pass_type: "election_night",
              result_status: "unofficial",
              outcome: "passed",
              source_url: "https://results.example.gov/measure-1",
              source_type: "official",
              retrieved_at: "2026-11-04T06:00:00.000Z",
            },
          ],
        },
      })
    );

    expect(await screen.findByText("Passed")).toBeInTheDocument();
    expect(screen.getByText("· Unofficial")).toBeInTheDocument();
    // Nothing certified yet, so the pre-certification notice applies.
    expect(screen.getByText("Unofficial until certified by the relevant election authority.")).toBeInTheDocument();
    const source = screen.getByRole("link", { name: /results\.example\.gov/ });
    expect(source).toHaveAttribute("href", "https://results.example.gov/measure-1");
  });

  it("drops the pre-certification notice once a certified result row exists", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        race_type: "ballot_measure",
        candidates: [],
        ballot_measure: {
          id: "m-1",
          official_ballot_title: "Measure 1",
          summary: "A measure.",
          what_yes_means: "Yes approves the bond.",
          what_no_means: "No rejects the bond.",
          result: "passed",
          source_urls: [],
          official_measure_url: null,
          research_area_tags: [],
          results: [
            {
              id: "mr-2",
              pass_type: "certified",
              result_status: "certified",
              outcome: "passed",
              source_url: "https://sos.example.gov/certified-results",
              source_type: "official",
              retrieved_at: "2026-12-01T06:00:00.000Z",
            },
          ],
        },
      })
    );

    expect(await screen.findByText("Passed")).toBeInTheDocument();
    expect(screen.getByText("· Certified")).toBeInTheDocument();
    // "Unofficial until certified" above a row labeled Certified is
    // contradictory for official election information.
    expect(
      screen.queryByText("Unofficial until certified by the relevant election authority.")
    ).not.toBeInTheDocument();
  });

  it("links the official measure text and lists the remaining measure sources", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        race_type: "ballot_measure",
        candidates: [],
        ballot_measure: {
          id: "m-1",
          official_ballot_title: "Measure 1",
          summary: "A measure.",
          what_yes_means: "Yes approves the bond.",
          what_no_means: "No rejects the bond.",
          result: null,
          source_urls: [
            "https://sos.example.gov/qualified-measures",
            "https://sos.example.gov/measures/measure-1.pdf",
          ],
          official_measure_url: "https://sos.example.gov/measures/measure-1.pdf",
          research_area_tags: [],
          results: [],
        },
      })
    );

    const measureLink = await screen.findByRole("link", { name: "Read the official measure text (PDF)" });
    expect(measureLink).toHaveAttribute("href", "https://sos.example.gov/measures/measure-1.pdf");
    // The official URL renders only as the prominent link, not a second source line.
    expect(screen.getByRole("link", { name: "sos.example.gov" })).toHaveAttribute(
      "href",
      "https://sos.example.gov/qualified-measures"
    );
  });

  it("uses neutral wording when the measure URL is not government-hosted", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        race_type: "ballot_measure",
        candidates: [],
        ballot_measure: {
          id: "m-1",
          official_ballot_title: "Measure 1",
          summary: "A measure.",
          what_yes_means: "Yes approves the bond.",
          what_no_means: "No rejects the bond.",
          result: null,
          source_urls: ["https://ballotpedia.org/Example_Measure_(2026)"],
          official_measure_url: "https://ballotpedia.org/Example_Measure_(2026)",
          research_area_tags: [],
          results: [],
        },
      })
    );

    // A third-party page must not be presented as the official measure text.
    const link = await screen.findByRole("link", { name: "More about this measure" });
    expect(link).toHaveAttribute("href", "https://ballotpedia.org/Example_Measure_(2026)");
    expect(screen.queryByRole("link", { name: /official measure text/ })).not.toBeInTheDocument();
  });

  it("lists every measure source when there is no official measure URL", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        race_type: "ballot_measure",
        candidates: [],
        ballot_measure: {
          id: "m-1",
          official_ballot_title: "Measure 1",
          summary: "A measure.",
          what_yes_means: "Yes approves the bond.",
          what_no_means: "No rejects the bond.",
          result: null,
          source_urls: ["https://sos.example.gov/qualified-measures", "https://news.example.org/measure-1"],
          official_measure_url: null,
          research_area_tags: [],
          results: [],
        },
      })
    );

    // Without an official URL there is no prominent link, and every source
    // gets its own provenance line (the old UI capped this at one).
    expect(await screen.findByRole("link", { name: "sos.example.gov" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "news.example.org" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /official measure text/ })).not.toBeInTheDocument();
  });
});

describe("ElectionPage candidate result badges", () => {
  function officeResult(overrides: Partial<ElectionDetail["results"][number]> = {}) {
    return {
      id: "r-1",
      pass_type: "election_night",
      result_status: "unofficial",
      outcome: "advanced",
      winners: [{ candidate_id: "c-1", candidate_name: "Jordan Voter", party: "Independent" }],
      source_url: "https://results.example.gov/governor",
      retrieved_at: "2026-11-04T06:00:00.000Z",
      ...overrides,
    };
  }

  /** The badge element sitting next to one candidate's name, if any. */
  function badgeElementFor(name: string): HTMLElement | null {
    const row = screen.getByRole("heading", { name }).parentElement;
    return row?.querySelector<HTMLElement>("span:not(:has(h3))") ?? null;
  }

  /** The badge text sitting next to one candidate's name, if any. */
  function badgeFor(name: string): string | null {
    return badgeElementFor(name)?.textContent ?? null;
  }

  it("marks who advanced and who did not", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ results: [officeResult()] }));

    await screen.findByRole("heading", { name: "Governor" });
    // The roster itself says who moved on — no trip to the results section.
    expect(badgeFor("Jordan Voter")).toBe("Advanced");
    expect(badgeFor("Riley Runner")).toBe("Did not advance");
  });

  it("colors the outcome green for winners and red for losers", async () => {
    // Same red as the measure "No" chip and the "A NO vote means" box: the
    // badge pair has to read at a glance, and gray registered as "no data"
    // rather than "this candidate lost".
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ results: [officeResult()] }));

    await screen.findByRole("heading", { name: "Governor" });
    expect(badgeElementFor("Jordan Voter")?.className).toContain("text-green-900");
    const loser = badgeElementFor("Riley Runner");
    expect(loser?.className).toContain("text-red-900");
    expect(loser?.className).not.toContain("text-ink-soft");
  });

  it("says Won and Lost on a general-election result", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ results: [officeResult({ outcome: "won" })] }));

    await screen.findByRole("heading", { name: "Governor" });
    expect(badgeFor("Jordan Voter")).toBe("Won");
    expect(badgeFor("Riley Runner")).toBe("Lost");
  });

  it("marks only who continues in a runoff", async () => {
    // A runoff row names who advances to the next round; it says nothing
    // about who is out, so no loser badge goes out.
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ results: [officeResult({ outcome: "runoff" })] }));

    await screen.findByRole("heading", { name: "Governor" });
    expect(badgeFor("Jordan Voter")).toBe("In runoff");
    expect(badgeFor("Riley Runner")).toBeNull();
  });

  it("badges nobody when no winner matched the roster", async () => {
    // A name-only (write-in) winner set is not exhaustive over the roster —
    // marking every listed candidate as a loser would be a false statement.
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        results: [
          officeResult({ outcome: "won", winners: [{ candidate_name: "Sam Writein" }] }),
        ],
      })
    );

    await screen.findByRole("heading", { name: "Governor" });
    expect(badgeFor("Jordan Voter")).toBeNull();
    expect(badgeFor("Riley Runner")).toBeNull();
  });

  it("badges nobody when the winner id points outside the displayed roster", async () => {
    // A stale or filtered-out id (e.g. a withdrawn candidate dropped from the
    // payload) must not flip everyone else to losers.
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        results: [
          officeResult({
            outcome: "won",
            winners: [{ candidate_id: "c-elsewhere", candidate_name: "Sam Elsewhere" }],
          }),
        ],
      })
    );

    await screen.findByRole("heading", { name: "Governor" });
    expect(badgeFor("Jordan Voter")).toBeNull();
    expect(badgeFor("Riley Runner")).toBeNull();
  });

  it("withholds loser badges on a partial match", async () => {
    // One winner matched, the other name-only: that second winner might be a
    // roster candidate the matcher missed, so only the confirmed winner is
    // marked and nobody is called a loser.
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        results: [
          officeResult({
            outcome: "advanced",
            winners: [
              { candidate_id: "c-1", candidate_name: "Jordan Voter" },
              { candidate_name: "Sam Writein" },
            ],
          }),
        ],
      })
    );

    await screen.findByRole("heading", { name: "Governor" });
    expect(badgeFor("Jordan Voter")).toBe("Advanced");
    expect(badgeFor("Riley Runner")).toBeNull();
  });

  it("badges nobody on an undecided result", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({ results: [officeResult({ outcome: "too_close", winners: [] })] })
    );

    await screen.findByRole("heading", { name: "Governor" });
    expect(badgeFor("Jordan Voter")).toBeNull();
    expect(badgeFor("Riley Runner")).toBeNull();
  });

  it("keeps the candidate name out of the badge's accessible name", async () => {
    // Regression guard: an in-heading badge fused into the accessible name
    // ("Jordan VoterAdvanced").
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ results: [officeResult()] }));

    expect(await screen.findByRole("heading", { name: "Jordan Voter" })).toBeInTheDocument();
  });
});

describe("ElectionPage back link and nav context", () => {
  const BALLOT_BACK = { path: "/ballot?d=d-1&sort=soonest", label: "All elections" };

  it("uses the arrival context for the back link when router state validates", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail(), "e-1", { backTo: BALLOT_BACK });

    const back = await screen.findByRole("link", { name: "Back to All elections" });
    // The full query string survives the round trip.
    expect(back).toHaveAttribute("href", "/ballot?d=d-1&sort=soonest");
  });

  it("shows no nav bar on a deep link with no state", async () => {
    // Deep links (shares, search engines) have no arrival context — no bar
    // at all, by product choice.
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail());

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.queryByRole("navigation", { name: "Ballot navigation" })).not.toBeInTheDocument();
  });

  it("shows no nav bar when the stored state is malformed", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail(), "e-1", {
      backTo: { path: "https://evil.example/phish", label: "All elections" },
    });

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.queryByRole("navigation", { name: "Ballot navigation" })).not.toBeInTheDocument();
  });

  it("restores a candidate page's own context when backing out to it", async () => {
    // The return half of My Picks → candidate → election → back: the back
    // link must deliver the candidate's original nav state, or a
    // multi-election candidate lands stateless and loses its back link.
    stubApiRoutes({ ...ANONYMOUS });
    const user = userEvent.setup();
    const candidateContext = { backTo: { path: "/me/picks", label: "My Picks" } };
    const { router } = renderElection(() => electionDetail(), "e-1", {
      backTo: { path: "/candidates/c-1", label: "Jordan Voter" },
      backState: candidateContext,
    });

    await user.click(await screen.findByRole("link", { name: "Back to Jordan Voter" }));

    expect(router.state.location.pathname).toBe("/candidates/c-1");
    expect(router.state.location.state).toEqual(candidateContext);
  });

  it("hands candidate links this election as back context plus the displayed roster order", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const user = userEvent.setup();
    const incoming = { backTo: BALLOT_BACK, contests: [{ id: "e-1", title: "Governor" }] };
    const { router } = renderElection(() => electionDetail(), "e-1", incoming);

    await user.click(await screen.findByRole("link", { name: "Jordan Voter" }));

    expect(router.state.location.pathname).toBe("/candidates/c-1");
    expect(router.state.location.state).toEqual({
      backTo: { path: "/elections/e-1", label: "Governor" },
      // The election page's own arrival context rides along so the back
      // hop can restore it.
      backState: incoming,
      electionId: "e-1",
      candidates: [
        { id: "c-1", name: "Jordan Voter" },
        { id: "c-2", name: "Riley Runner" },
      ],
    });
  });

  it("scopes the handed-off roster order to the active party filter", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const user = userEvent.setup();
    const { router } = renderElection(() =>
      electionDetail({
        candidates: [
          {
            candidate_id: "c-dem",
            display_name: "Dana Democrat",
            party: "Democratic",
            is_incumbent: false,
            status: "active",
            summary: null,
            finance_summary: null,
            records: [],
          },
          {
            candidate_id: "c-rep",
            display_name: "Rory Republican",
            party: "Republican",
            is_incumbent: false,
            status: "active",
            summary: null,
            finance_summary: null,
            records: [],
          },
        ],
      })
    );

    await user.click(await screen.findByRole("button", { name: "Democrats (1)" }));
    await user.click(screen.getByRole("link", { name: "Dana Democrat" }));

    const state = router.state.location.state as { candidates: unknown };
    expect(state.candidates).toEqual([{ id: "c-dem", name: "Dana Democrat" }]);
  });
});

describe("ElectionPage ballot pager", () => {
  const CONTESTS = [
    { id: "e-1", title: "Governor" },
    { id: "e-2", title: "Mayor" },
    { id: "e-3", title: "Sheriff" },
  ];
  const ARRIVAL = { backTo: { path: "/ballot?d=d-1", label: "All elections" }, contests: CONTESTS };
  // The loader answers for whatever id the pager navigates to, so a walk
  // across contests keeps rendering real pages.
  const perIdLoader = ({ params }: { params: { electionId?: string } }) =>
    electionDetail({ id: params.electionId });

  it("renders prev/next around the current contest, forwarding the walk state", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(perIdLoader, "e-2", ARRIVAL);

    const pager = await screen.findByRole("navigation", { name: "Ballot navigation" });
    expect(within(pager).getByRole("link", { name: "Previous: Governor" })).toHaveAttribute(
      "href",
      "/elections/e-1"
    );
    expect(within(pager).getByRole("link", { name: "Next: Sheriff" })).toHaveAttribute("href", "/elections/e-3");
    expect(within(pager).getByRole("link", { name: "Back to All elections" })).toHaveAttribute(
      "href",
      "/ballot?d=d-1"
    );
  });

  it("leaves the prev slot empty at the start of the sequence", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(perIdLoader, "e-1", ARRIVAL);

    const pager = await screen.findByRole("navigation", { name: "Ballot navigation" });
    expect(within(pager).queryByRole("link", { name: /^Previous:/ })).not.toBeInTheDocument();
    expect(within(pager).getByRole("link", { name: "Next: Mayor" })).toBeInTheDocument();
  });

  it("keeps paging across two consecutive next clicks", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const user = userEvent.setup();
    const { router } = renderElection(perIdLoader, "e-1", ARRIVAL);

    await user.click(await screen.findByRole("link", { name: "Next: Mayor" }));
    // The forwarded state re-renders the pager on the next page.
    await user.click(await screen.findByRole("link", { name: "Next: Sheriff" }));

    expect(router.state.location.pathname).toBe("/elections/e-3");
    expect(router.state.location.state).toEqual(ARRIVAL);
    const pager = await screen.findByRole("navigation", { name: "Ballot navigation" });
    expect(within(pager).getByRole("link", { name: "Previous: Mayor" })).toBeInTheDocument();
    expect(within(pager).queryByRole("link", { name: /^Next:/ })).not.toBeInTheDocument();
  });

  it("shows no prev/next on a stale snapshot or a single-contest list", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    // Current election missing from the snapshot (stale filtered list).
    renderElection(perIdLoader, "e-9", { ...ARRIVAL });
    await screen.findByRole("heading", { name: "Governor" });
    let pager = screen.getByRole("navigation", { name: "Ballot navigation" });
    expect(within(pager).getByRole("link", { name: "Back to All elections" })).toBeInTheDocument();
    expect(within(pager).queryByRole("link", { name: /^(Previous|Next):/ })).not.toBeInTheDocument();

    renderElection(perIdLoader, "e-2", {
      backTo: ARRIVAL.backTo,
      contests: [{ id: "e-2", title: "Mayor" }],
    });
    await waitFor(() =>
      expect(screen.getAllByRole("navigation", { name: "Ballot navigation" })).toHaveLength(2)
    );
    pager = screen.getAllByRole("navigation", { name: "Ballot navigation" })[1];
    expect(within(pager).queryByRole("link", { name: /^(Previous|Next):/ })).not.toBeInTheDocument();
  });
});
