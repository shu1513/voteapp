import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ElectionChoice } from "@voteapp/api-client";
import { PicksPage } from "./PicksPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, electionSummary, ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

function renderPicks() {
  return renderRoutes(
    [
      { path: "/me/picks", element: <PicksPage /> },
      { path: "/login", element: <p /> },
      { path: "/elections/:electionId", element: <p>Election page</p> },
      { path: "/candidates/:candidateId", element: <p>Candidate page</p> },
    ],
    "/me/picks"
  );
}

function electionChoice(overrides: Partial<ElectionChoice> = {}): ElectionChoice {
  return {
    election_id: "e-1",
    race_type: "office",
    official_ballot_title: "Governor",
    election_date: "2026-11-03",
    seats_to_fill: null,
    picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "declared" }],
    measure_position: null,
    updated_at: "2026-08-01T00:00:00.000Z",
    ...overrides,
  };
}

// Everything a verified render touches; individual tests override entries.
function verifiedRoutes(overrides: Record<string, unknown> = {}) {
  return {
    "/api/me": { body: ME_VERIFIED },
    "/api/me/ballot": {
      body: ballotSummary([
        electionSummary(),
        electionSummary({ id: "e-2", official_ballot_title: "Mayor" }),
      ]),
    },
    "/api/me/election-choices": { body: { choices: [electionChoice()] } },
    // AutoPickFillControl reads the viewer's ranked issues on every verified
    // render (button gating); empty keeps it inert unless a test overrides.
    "/api/me/research-area-preferences": { body: { preferences: [] } },
    ...overrides,
  } as Parameters<typeof stubApiRoutes>[0];
}

// Frozen clock: the page classifies races as upcoming/past against the real
// date (usLatestLocalDate), so the 2026-11-03 fixtures would flip into the
// past section — and these assertions would rot — once that day passes.
beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  vi.setSystemTime(new Date("2026-08-01T12:00:00Z"));
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("PicksPage", () => {
  it("shows the milestone, without a sign-up link, once every race on the nearest day has a pick", async () => {
    window.localStorage.clear();
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": {
          body: {
            choices: [electionChoice(), electionChoice({ election_id: "e-2", official_ballot_title: "Mayor" })],
          },
        },
      })
    );
    renderPicks();

    const milestone = await screen.findByRole("region", { name: "November 3, 2026 election draft milestone" });
    expect(milestone).toHaveTextContent("You have completed your November 3, 2026 election draft.");
    expect(milestone).not.toHaveTextContent(/decided/);
    expect(screen.queryByRole("link", { name: /Sign up/ })).not.toBeInTheDocument();
    expect(JSON.parse(window.localStorage.getItem("voteapp_draft_complete_seen") ?? "[]")).toEqual(["2026-11-03"]);
  });

  it("judges the nearest UPCOMING day, skipping a just-finished day still carded", async () => {
    window.localStorage.clear();
    stubApiRoutes(
      verifiedRoutes({
        // A finished July race (no pick) still rides the ballot; November is
        // the nearest upcoming day and it is fully decided.
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({ id: "e-july", election_date: "2026-07-28", official_ballot_title: "Special" }),
            electionSummary(),
          ]),
        },
      })
    );
    renderPicks();

    expect(await screen.findByRole("region", { name: "November 3, 2026 election draft milestone" })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: /July 28, 2026 election draft milestone/ })).not.toBeInTheDocument();
  });

  it("shows no milestone while a race on the nearest day is still open", async () => {
    window.localStorage.clear();
    stubApiRoutes(verifiedRoutes());
    renderPicks();
    expect(await screen.findByText("1 of 2 races decided")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: /election draft milestone/ })).not.toBeInTheDocument();
    expect(window.localStorage.getItem("voteapp_draft_complete_seen")).toBeNull();
  });

  it("asks logged-out visitors to log in", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderPicks();
    expect(await screen.findByText(/Log in to plan your votes/)).toBeInTheDocument();
  });

  it("shows unverified users the verify interstitial AND their picks", async () => {
    // Choices are not verification-gated (any registered session may pick),
    // so the verify wall must not hide a pick the user just saved.
    stubApiRoutes({
      "/api/me": { body: ME_UNVERIFIED },
      "/api/me/election-choices": { body: { choices: [electionChoice()] } },
    });
    renderPicks();
    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "Your upcoming picks" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Governor" })).toHaveAttribute("href", "/elections/e-1");
    expect(screen.getByText(/Jane Smith/)).toBeInTheDocument();
  });

  it("lists an upcoming pick on a race missing from the saved ballot", async () => {
    // The choice API accepts picks on any upcoming race (candidate search,
    // shared links, an old address) — the cards only render the saved
    // ballot, so such a pick must surface in its own section instead of
    // silently vanishing.
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice(),
              electionChoice({
                election_id: "e-offballot",
                official_ballot_title: "Attorney General",
                election_date: "2026-11-03",
              }),
            ],
          },
        },
      })
    );
    renderPicks();

    expect(await screen.findByRole("heading", { name: "Other upcoming picks" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Attorney General" })).toHaveAttribute(
      "href",
      "/elections/e-offballot"
    );
    // The carded race stays on its date card only — no duplicate row here.
    const section = screen.getByRole("heading", { name: "Other upcoming picks" }).closest("section");
    expect(section).not.toHaveTextContent("Governor");
  });

  it("offers removal on a withdrawn upcoming pick and PUTs chosen: false", async () => {
    // A withdrawn candidacy vanishes from election payloads, so no pick
    // button anywhere can toggle it off — this page's remove control is the
    // only way a multi-seat slot held by a withdrawn pick gets freed.
    const puts: unknown[] = [];
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": (_url: URL, init?: RequestInit) => {
          if (init?.method === "PUT") {
            puts.push(JSON.parse(String(init.body)));
            return { status: 200, body: { choice: electionChoice({ picks: [] }) } };
          }
          return {
            body: {
              choices: [
                electionChoice({
                  picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "withdrawn" }],
                }),
              ],
            },
          };
        },
      })
    );
    renderPicks();

    expect(await screen.findByText("(withdrew)")).toBeInTheDocument();
    await userEvent.setup().click(screen.getByRole("button", { name: "Remove pick: Jane Smith" }));
    await waitFor(() =>
      expect(puts).toEqual([{ election_id: "e-1", candidate_id: "c-1", chosen: false }])
    );
  });

  it("offers no removal on a withdrawn pick from a past election", async () => {
    // The backend rejects choice writes to past elections — a dead button
    // whose only outcome is an error must not render.
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice({
                election_id: "e-past",
                election_date: "2026-05-05",
                picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "withdrawn" }],
              }),
            ],
          },
        },
      })
    );
    renderPicks();

    expect(await screen.findByText("(withdrew)")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Remove pick/ })).not.toBeInTheDocument();
  });

  it("asks for an address when the empty ballot has no districts", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/ballot": { body: { district_ids: [], districts: [], elections: [] } },
        "/api/me/election-choices": { body: { choices: [] } },
      })
    );
    renderPicks();

    expect(await screen.findByRole("link", { name: "Set your address" })).toHaveAttribute(
      "href",
      "/me/ballot"
    );
  });

  it("omits the address ask when districts are known but no election is upcoming", async () => {
    // The lookup ran and simply found nothing — telling this viewer to set an
    // address they already set would read as a bug.
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/ballot": { body: ballotSummary([]) },
        "/api/me/election-choices": { body: { choices: [] } },
      })
    );
    renderPicks();

    expect(await screen.findByText("No upcoming elections on your ballot yet.")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Set your address" })).not.toBeInTheDocument();
  });

  it("marks a measure pick with its outcome, muted when it went against the pick", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({ race_type: "ballot_measure", candidate_count: 0 }),
            electionSummary({
              id: "e-2",
              official_ballot_title: "Proposition 9",
              race_type: "ballot_measure",
              candidate_count: 0,
            }),
          ]),
        },
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice({ picks: [], measure_position: "yes", measure_result: "passed" }),
              electionChoice({
                election_id: "e-2",
                official_ballot_title: "Proposition 9",
                race_type: "ballot_measure",
                picks: [],
                measure_position: "yes",
                measure_result: "failed",
              }),
            ],
          },
        },
      })
    );
    renderPicks();

    // Matched pick: green chip; unmatched: muted — same semantics as the
    // candidate Won/Lost chips.
    expect(await screen.findByText("Passed")).toBeInTheDocument();
    expect(screen.getByText("Passed").className).toContain("bg-green-700");
    // The exact muted style, not just "not green": a broken class would
    // otherwise pass.
    expect(screen.getByText("Failed").className).toContain("bg-surface");
  });

  it("renders a date card with picked and undecided races", async () => {
    stubApiRoutes(verifiedRoutes());
    renderPicks();

    // Date card heading + decided count.
    expect(await screen.findByRole("heading", { name: "My November 3, 2026 Election Draft" })).toBeInTheDocument();
    expect(screen.getByText("1 of 2 races decided")).toBeInTheDocument();

    // Picked race: title links to the race, pick renders beside it.
    expect(screen.getByRole("link", { name: "Governor" })).toHaveAttribute("href", "/elections/e-1");
    expect(screen.getByText("Jane Smith")).toBeInTheDocument();

    // Undecided race: the grey line itself is the link to the race.
    const undecided = screen.getByRole("link", { name: "Mayor — no pick yet" });
    expect(undecided).toHaveAttribute("href", "/elections/e-2");

    // The follows manager and issue editor live on their own pages now
    // (/me/follows and Settings) — not here.
    expect(screen.queryByRole("heading", { name: "My Candidates" })).not.toBeInTheDocument();
    expect(screen.queryByText("My most important issues")).not.toBeInTheDocument();
  });

  it("mints a share link on demand and swaps in the share menu", async () => {
    const fetchMock = stubApiRoutes(
      verifiedRoutes({
        "/api/me/pick-card-shares": {
          body: { share: { token: "tok_abcdefghijklmnopqrstuvwxyz012345", election_date: "2026-11-03" } },
        },
      })
    );
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderPicks();

    await user.click(await screen.findByRole("button", { name: "Share my November 3, 2026 picks" }));

    // The standard ShareButton takes over once the token exists (menu shape
    // in jsdom — no navigator.share), alongside the visibility warning.
    expect(await screen.findByRole("button", { name: "Share my November 3, 2026 picks" })).toBeInTheDocument();
    // The caption must disclose the name reveal — minting is the consent
    // event, so the sharer learns it here, not from a recipient.
    expect(
      screen.getByText("Anyone with the link can see this card and your first name.")
    ).toBeInTheDocument();

    // The minted URL itself is visible — canonical host in the text, the
    // relative path as the href (the token only resolves where it was
    // minted; see ShareCardControl).
    const mintedLink = screen.getByRole("link", {
      name: "electionssimplified.com/picks/tok_abcdefghijklmnopqrstuvwxyz012345",
    });
    expect(mintedLink).toHaveAttribute("href", "/picks/tok_abcdefghijklmnopqrstuvwxyz012345");
    expect(mintedLink).toHaveAttribute("target", "_blank");

    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(post).toBeDefined();
    expect(String(post![0])).toContain("/api/me/pick-card-shares");
    expect(JSON.parse(String(post![1]!.body))).toEqual({ election_date: "2026-11-03" });

    // The minted link lands in the share menu's copy target.
    await user.click(screen.getByRole("button", { name: "Share my November 3, 2026 picks" }));
    expect(await screen.findByRole("menuitem", { name: "Share on X" })).toHaveAttribute(
      "href",
      expect.stringContaining("tok_abcdefghijklmnopqrstuvwxyz012345")
    );
  });

  it("shows an error instead of all-undecided cards when the choices fetch fails", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": apiError(500, "internal_error", "boom"),
      })
    );
    renderPicks();

    // The error is the whole story: no card may render claiming races are
    // undecided when the truth is unknown.
    expect(await screen.findByText(/Could not load your picks/)).toBeInTheDocument();
    expect(screen.queryByText(/no pick yet/)).not.toBeInTheDocument();
    expect(screen.queryByText(/races decided/)).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /My November 3, 2026 Election Draft/ })).not.toBeInTheDocument();
  });

  it("hides the share control on a card with zero picks", async () => {
    stubApiRoutes(verifiedRoutes({ "/api/me/election-choices": { body: { choices: [] } } }));
    renderPicks();

    expect(await screen.findByRole("heading", { name: "My November 3, 2026 Election Draft" })).toBeInTheDocument();
    expect(screen.getByText("0 of 2 races decided")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /^Share/ })).not.toBeInTheDocument();
  });

  it("keeps a just-finished election's card, with result chips, out of Past elections", async () => {
    // The ballot payload keeps finished races for a few days; the card must
    // live exactly as long, so results land on the very card that planned
    // the votes — not in the collapsed history.
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({
              election_date: "2026-07-28",
              has_results: true,
              current_result_outcome: "advanced",
              current_result_winners: [
                { candidate_id: "c-1", candidate_name: "Jane Smith", party: "Democratic" },
                { candidate_id: "c-2", candidate_name: "John James", party: "Republican" },
              ],
            }),
            electionSummary({ id: "e-2", official_ballot_title: "Mayor", election_date: "2026-07-28" }),
          ]),
        },
        "/api/me/election-choices": {
          body: { choices: [electionChoice({ election_date: "2026-07-28" })] },
        },
      })
    );
    renderPicks();

    expect(await screen.findByText("My July 28, 2026 Election Draft")).toBeInTheDocument();
    // The pick's own line carries the call.
    expect(screen.getByText("Advanced")).toBeInTheDocument();
    // Still carded → not double-listed under Past elections.
    expect(screen.queryByText(/Past elections/)).not.toBeInTheDocument();
    // A race that can no longer be decided drops the "yet".
    expect(screen.getByText("Mayor — no pick")).toBeInTheDocument();
    expect(screen.queryByText(/no pick yet/)).not.toBeInTheDocument();
  });

  it("stays silent on a pick that missed the winners", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({
              election_date: "2026-07-28",
              has_results: true,
              current_result_outcome: "advanced",
              current_result_winners: [
                { candidate_id: "c-2", candidate_name: "John James", party: "Republican" },
              ],
            }),
          ]),
        },
        "/api/me/election-choices": {
          body: { choices: [electionChoice({ election_date: "2026-07-28" })] },
        },
      })
    );
    renderPicks();

    // The pick (c-1) is not among the winners: no chip, no loss flag.
    expect(await screen.findByText("Jane Smith")).toBeInTheDocument();
    expect(screen.queryByText("Advanced")).not.toBeInTheDocument();
    expect(screen.queryByText("Lost")).not.toBeInTheDocument();
  });

  it("never doubles a certified candidacy chip with the result-derived one", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/ballot": {
          body: ballotSummary([
            electionSummary({
              election_date: "2026-07-28",
              has_results: true,
              current_result_outcome: "won",
              current_result_winners: [
                { candidate_id: "c-1", candidate_name: "Jane Smith", party: "Democratic" },
              ],
            }),
          ]),
        },
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice({
                election_date: "2026-07-28",
                picks: [{ candidate_id: "c-1", display_name: "Jane Smith", candidacy_status: "won" }],
              }),
            ],
          },
        },
      })
    );
    renderPicks();

    expect(await screen.findByText("Jane Smith")).toBeInTheDocument();
    expect(screen.getAllByText("Won")).toHaveLength(1);
  });

  it("renders the ballot view on toggle: contest boxes in payload order, picks filled, withdrawn struck", async () => {
    const fetchMock = stubApiRoutes(
      verifiedRoutes({
        // ONE fetch serves both views: the preview payload is also the list
        // payload, so List and Ballot preview share the same order by design.
        "/api/me/ballot": {
          body: ballotSummary([
                  electionSummary({
                    id: "e-2",
                    official_ballot_title: "United States Senator",
                    preview: {
                      seats_to_fill: null,
                      candidates: [
                        {
                          candidate_election_id: "ce-2",
                          candidate_id: "c-2",
                          display_name: "Sam Senate",
                          party: "Republican",
                          is_incumbent: false,
                          status: "declared",
                        },
                      ],
                      measure: null,
                    },
                  }),
                  electionSummary({
                    preview: {
                      seats_to_fill: 2,
                      candidates: [
                        {
                          candidate_election_id: "ce-1",
                          candidate_id: "c-1",
                          display_name: "Jane Smith",
                          party: "Democratic",
                          is_incumbent: true,
                          status: "declared",
                        },
                        {
                          candidate_election_id: "ce-3",
                          candidate_id: "c-3",
                          display_name: "Walt Withdrawn",
                          party: "Independent",
                          is_incumbent: false,
                          status: "withdrawn",
                        },
                      ],
                      measure: null,
                    },
                  }),
                  electionSummary({
                    id: "e-3",
                    official_ballot_title: "Measure H",
                    race_type: "ballot_measure",
                    candidate_count: 0,
                    preview: {
                      seats_to_fill: null,
                      candidates: [],
                      measure: {
                        id: "m-1",
                        official_ballot_title: "Measure H",
                        summary: "A parcel tax.",
                        what_yes_means: "Adopts the tax.",
                        what_no_means: "Keeps current law.",
                      },
                    },
                  }),
                ]),
        },
        "/api/me/election-choices": {
          body: { choices: [electionChoice(), electionChoice({ election_id: "e-3", race_type: "ballot_measure", picks: [], measure_position: "yes" })] },
        },
      })
    );
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderPicks();

    await user.click(await screen.findByRole("button", { name: "Ballot preview" }));

    // The fetch pins its own ordering contract — the user's saved list sort
    // must never reorder this page — and toggling views must NOT refetch:
    // one payload backs both List and Ballot preview.
    const ballotCalls = fetchMock.mock.calls.filter(([input]) => String(input).includes("/api/me/ballot?"));
    expect(ballotCalls).toHaveLength(1);
    expect(String(ballotCalls[0][0])).toContain("include=preview");
    expect(String(ballotCalls[0][0])).toContain("sort=state_baseline");
    expect(String(ballotCalls[0][0])).toContain("followed_first=false");

    // Sheet header + disclaimer.
    expect(await screen.findByRole("heading", { name: /Ballot preview — November 3, 2026/ })).toBeInTheDocument();
    expect(screen.getByText("Not an official ballot")).toBeInTheDocument();

    // Contest boxes render in PAYLOAD order (Senator first), not list order.
    const headings = screen.getAllByRole("heading", { level: 4 }).map((h) => h.textContent);
    expect(headings).toEqual(["United States Senator", "Governor", "Measure H"]);

    // Multi-seat instruction from seats_to_fill; single/null renders as one.
    expect(screen.getByText("Vote for up to 2")).toBeInTheDocument();
    expect(screen.getByText("Vote for One")).toBeInTheDocument();

    // The pick: filled oval is visual; the textual chip is the contract.
    expect(screen.getAllByText("My pick").length).toBe(2); // Jane + measure Yes
    expect(screen.getByText("Jane Smith").className).toContain("font-bold");

    // Withdrawn candidacy stays visible, struck through, with the warning.
    expect(screen.getByText("Walt Withdrawn").className).toContain("line-through");
    expect(screen.getByText(/withdrew — votes may not count/)).toBeInTheDocument();

    // Measure: VoteApp summary is labeled as ours, never as ballot text.
    expect(screen.getByText(/VoteApp summary \(not the printed ballot text\): A parcel tax\./)).toBeInTheDocument();

    // Toggling back restores the list cards.
    await user.click(screen.getByRole("button", { name: "List view" }));
    expect(await screen.findByText(/races decided/)).toBeInTheDocument();
  });

  it("lists past picks in a collapsible section with won/lost flags", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice(),
              electionChoice({
                election_id: "e-old",
                official_ballot_title: "Sheriff",
                election_date: "2024-11-05",
                picks: [{ candidate_id: "c-9", display_name: "Pat Winner", candidacy_status: "won" }],
              }),
              // Certified primaries project advanced/runoff onto winners'
              // candidacy_status; past picks have no result-row fallback, so
              // the chip must come from the status alone.
              electionChoice({
                election_id: "e-old-2",
                official_ballot_title: "Judge",
                election_date: "2024-08-06",
                picks: [
                  { candidate_id: "c-10", display_name: "Ada Advancer", candidacy_status: "advanced" },
                  { candidate_id: "c-11", display_name: "Rae Runoff", candidacy_status: "runoff" },
                ],
              }),
              // Pre-certification: candidacy_status still "declared", but the
              // choices list read attaches the canonical election-night
              // result — history must not lose the call for the weeks until
              // certification.
              electionChoice({
                election_id: "e-old-3",
                official_ballot_title: "Auditor",
                election_date: "2026-07-28",
                picks: [{ candidate_id: "c-12", display_name: "Nia Night", candidacy_status: "declared" }],
                current_result_outcome: "won",
                current_result_winners: [{ candidate_id: "c-12", candidate_name: "Nia Night" }],
              }),
              electionChoice({
                election_id: "e-old-4",
                official_ballot_title: "Prop 9",
                election_date: "2026-07-28",
                race_type: "ballot_measure",
                picks: [],
                measure_position: "yes",
                measure_result: null,
                current_result_outcome: "passed",
              }),
            ],
          },
        },
      })
    );
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderPicks();

    const summary = await screen.findByText("Past elections (4)");
    await user.click(summary);

    expect(screen.getByText("Pat Winner")).toBeInTheDocument();
    expect(screen.getByText("Advanced")).toBeInTheDocument();
    expect(screen.getByText("In runoff")).toBeInTheDocument();
    // Pat Winner's certified chip and Nia Night's election-night chip.
    expect(screen.getAllByText("Won")).toHaveLength(2);
    // The measure's election-night passed fills in for the certified field.
    expect(screen.getByText("Passed")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Sheriff" })).toHaveAttribute("href", "/elections/e-old");
  });

  it("badges auto picks and offers the fill/clear batch controls", async () => {
    // e-1's pick came from the engine (origin auto) → Auto chip + a clear
    // button; e-2 has no pick → the fill button counts it.
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice({
                picks: [
                  {
                    candidate_id: "c-1",
                    display_name: "Jane Smith",
                    candidacy_status: "declared",
                    origin: "auto",
                  },
                ],
              }),
            ],
          },
        },
      })
    );
    renderPicks();

    expect(await screen.findByText("Auto")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Auto-fill empty picks by my issues" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Clear auto picks" })).toBeInTheDocument();
  });

  it("annotates undecided rows with the engine's reason after a fill run", async () => {
    // e-2 stays open (insufficient evidence): no result list — the reason
    // lands on the race row itself.
    const user = userEvent.setup();
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/research-area-preferences": {
          body: {
            preferences: [1, 2, 3].map((rank) => ({
              research_area_id: `a-${rank}`,
              slug: `issue-${rank}`,
              name: `Issue ${rank}`,
              description: null,
              rank,
              direction: "support",
              hard_veto: false,
            })),
          },
        },
        "/api/me/auto-picks": {
          body: {
            results: [
              {
                election_id: "e-2",
                race_type: "office",
                outcome: "no_pick",
                reason: "insufficient_evidence",
                picked_candidate_ids: [],
                measure_position: null,
                shortlist_candidate_ids: [],
                candidates: [],
                measure_per_issue: [],
                unresearched: [],
              },
            ],
          },
        },
      })
    );
    renderPicks();

    const button = await screen.findByRole("button", { name: "Auto-fill empty picks by my issues" });
    await waitFor(() => expect(button).toBeEnabled());
    await user.click(button);

    expect(
      await screen.findByRole("link", { name: /Mayor — no pick yet · auto pick: not enough evidence/ })
    ).toBeInTheDocument();
  });

  it("keeps fill-run reasons across a switch to ballot view", async () => {
    // The results live in PicksPage, not the card: running a fill in list
    // view and toggling views must not discard the "why was this left
    // open" feedback — the ballot sheet's contest box carries it instead.
    const user = userEvent.setup();
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/research-area-preferences": {
          body: {
            preferences: [1, 2, 3].map((rank) => ({
              research_area_id: `a-${rank}`,
              slug: `issue-${rank}`,
              name: `Issue ${rank}`,
              description: null,
              rank,
              direction: "support",
              hard_veto: false,
            })),
          },
        },
        "/api/me/auto-picks": {
          body: {
            results: [
              {
                election_id: "e-2",
                race_type: "office",
                outcome: "no_pick",
                reason: "insufficient_evidence",
                picked_candidate_ids: [],
                measure_position: null,
                shortlist_candidate_ids: [],
                candidates: [],
                measure_per_issue: [],
                unresearched: [],
              },
            ],
          },
        },
      })
    );
    renderPicks();

    const button = await screen.findByRole("button", { name: "Auto-fill empty picks by my issues" });
    await waitFor(() => expect(button).toBeEnabled());
    await user.click(button);
    await screen.findByRole("link", { name: /auto pick: not enough evidence/ });

    await user.click(screen.getByRole("button", { name: "Ballot preview" }));

    expect(await screen.findByText("Auto pick left this open: not enough evidence.")).toBeInTheDocument();
  });

  it("shows no batch controls when everything is decided by hand", async () => {
    stubApiRoutes(
      verifiedRoutes({
        "/api/me/election-choices": {
          body: {
            choices: [
              electionChoice(),
              electionChoice({ election_id: "e-2", official_ballot_title: "Mayor" }),
            ],
          },
        },
      })
    );
    renderPicks();

    // The card's own count line (the finished-draft milestone above the
    // toggle repeats the count in a longer sentence).
    await screen.findByText("2 of 2 races decided");
    expect(screen.queryByText("Auto")).toBeNull();
    expect(screen.queryByRole("button", { name: /Auto-fill empty picks/ })).toBeNull();
    expect(screen.queryByRole("button", { name: "Clear auto picks" })).toBeNull();
  });
});

describe("PicksPage nav context", () => {
  const MY_PICKS_STATE = { backTo: { path: "/me/picks", label: "My Election Draft" } };

  it("hands election links My Election Draft as their back destination", async () => {
    const user = userEvent.setup();
    stubApiRoutes(verifiedRoutes());
    const { router } = renderPicks();

    await user.click(await screen.findByRole("link", { name: "Governor" }));

    expect(router.state.location.pathname).toBe("/elections/e-1");
    expect(router.state.location.state).toEqual(MY_PICKS_STATE);
  });

  it("links pick names to the candidate profile with picks back state", async () => {
    const user = userEvent.setup();
    stubApiRoutes(verifiedRoutes());
    const { router } = renderPicks();

    await user.click(await screen.findByRole("link", { name: "Jane Smith" }));

    expect(router.state.location.pathname).toBe("/candidates/c-1");
    // electionId scopes the profile's candidacy context to the picked race.
    expect(router.state.location.state).toEqual({ ...MY_PICKS_STATE, electionId: "e-1" });
  });
});
