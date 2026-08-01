import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { CandidatePage, ErrorBoundary, loader } from "./CandidatePage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import {
  candidateDetail,
  candidateElection,
  candidateFollow,
  financeSummary,
  ME_UNVERIFIED,
  ME_VERIFIED,
} from "../test/fixtures";

const ANONYMOUS = { "/api/me": apiError(401, "unauthorized", "Not logged in") };

// The subject arrives via the route loader (server-fetched in production);
// tests supply it directly instead of stubbing the loader's fetch.
function renderCandidate(loader: () => unknown, id = "c-1") {
  return renderRoutes(
    [
      {
        path: "/candidates/:candidateId",
        element: <CandidatePage />,
        errorElement: <ErrorBoundary />,
        hydrateFallbackElement: <p />,
        loader,
      },
      { path: "/elections/:electionId", element: <p /> },
    ],
    `/candidates/${id}`
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("CandidatePage", () => {
  it("renders not-found UI when the loader throws a 404", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => {
      throw new Response("Not Found", { status: 404 });
    }, "c-missing");
    expect(await screen.findByText("Candidate not found")).toBeInTheDocument();
  });

  it("renders the profile with records grouped under their research area", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => candidateDetail());

    expect(await screen.findByRole("heading", { name: "Jordan Voter" })).toBeInTheDocument();
    expect(screen.getByText("Voted for the clean water act.")).toBeInTheDocument();
    // The record's area tag names the group heading.
    expect(screen.getByText("Environment")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Report an issue with candidate profile" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Report an issue with candidate record" })).toBeInTheDocument();
  });

  it("starts every issue group collapsed, each stating its record count", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const record = (id: string, areaId: string, areaName: string) => ({
      id,
      description: `Did a thing (${id}).`,
      source_url: "https://example.gov/record",
      event_date: "2026-05-01",
      created_at: "2026-05-02T00:00:00.000Z",
      research_area_tags: [{ research_area_id: areaId, slug: areaId, name: areaName, stance: "for" as const }],
    });
    renderCandidate(() =>
      candidateDetail({
        records: [
          record("r-1", "a-1", "Civil Rights"),
          record("r-2", "a-2", "Gun Control"),
          record("r-3", "a-3", "Housing"),
          record("r-4", "a-4", "Privacy"),
        ],
      })
    );

    const groupState = (name: string) =>
      (screen.getByText(name).closest("details") as HTMLDetailsElement).open;
    expect(await screen.findByText("Civil Rights")).toBeInTheDocument();
    // Test slugs (a-1…a-4) sit outside the salience ranking, so the groups
    // fall back to alphabetical: Civil Rights, Gun Control, Housing, Privacy.
    expect(groupState("Civil Rights")).toBe(false);
    expect(groupState("Gun Control")).toBe(false);
    expect(groupState("Housing")).toBe(false);
    expect(groupState("Privacy")).toBe(false);
    // Collapsed groups still state their size, so the closed profile reads
    // as an index of which issues carry a record.
    expect(screen.getAllByText("· 1 record")).toHaveLength(4);
  });

  it("orders issue groups by public salience with untagged records last", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const record = (id: string, tags: { areaId: string; slug: string; name: string }[]) => ({
      id,
      description: `Did a thing (${id}).`,
      source_url: "https://example.gov/record",
      event_date: "2026-05-01",
      created_at: "2026-05-02T00:00:00.000Z",
      research_area_tags: tags.map((tag) => ({
        research_area_id: tag.areaId,
        slug: tag.slug,
        name: tag.name,
        stance: "for" as const,
      })),
    });
    renderCandidate(() =>
      candidateDetail({
        records: [
          // Payload arrives alphabetical-ish; salience rank must win, with
          // the untagged pseudo-group sinking to the end.
          record("r-1", [{ areaId: "a-civ", slug: "civil_rights", name: "Civil Rights" }]),
          record("r-2", []),
          record("r-3", [{ areaId: "a-env", slug: "environment_and_public_health", name: "Environment and Public Health" }]),
          record("r-4", [{ areaId: "a-gun", slug: "gun_control", name: "Gun Control" }]),
        ],
      })
    );

    await screen.findByText("Civil Rights");
    const headings = screen
      .getAllByRole("heading", { level: 3 })
      .map((heading) => heading.textContent);
    expect(headings).toEqual([
      "Environment and Public Health",
      "Gun Control",
      "Civil Rights",
      "Other records",
    ]);
  });

  it("defaults the record view to \"My issues first\" once saved areas load", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-gun", slug: "gun_control", name: "Gun Control", description: null, rank: 1 },
          ],
        },
      },
    });
    const record = (id: string, areaId: string, slug: string, name: string) => ({
      id,
      description: `Did a thing (${id}).`,
      source_url: "https://example.gov/record",
      event_date: "2026-05-01",
      created_at: "2026-05-02T00:00:00.000Z",
      research_area_tags: [{ research_area_id: areaId, slug, name, stance: "for" as const }],
    });
    renderCandidate(() =>
      candidateDetail({
        records: [
          record("r-1", "a-env", "environment_and_public_health", "Environment and Public Health"),
          record("r-2", "a-gun", "gun_control", "Gun Control"),
        ],
      })
    );

    // The option only exists for users with saved areas, and it becomes the
    // default selection — the saved Gun Control group leads even though
    // Environment outranks it publicly.
    const select = await screen.findByRole("combobox");
    await screen.findByRole("option", { name: "My issues first" });
    expect(select).toHaveValue("my_issues");
    const headings = screen
      .getAllByRole("heading", { level: 3 })
      .map((heading) => heading.textContent);
    expect(headings).toEqual(["Gun Control", "Environment and Public Health"]);
  });

  it("keeps a group the reader opened open across a view switch that reorders groups", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [] } },
      "/api/me/research-area-preferences": {
        body: {
          preferences: [
            { research_area_id: "a-gun", slug: "gun_control", name: "Gun Control", description: null, rank: 1 },
          ],
        },
      },
    });
    const record = (id: string, areaId: string, slug: string, name: string) => ({
      id,
      description: `Did a thing (${id}).`,
      source_url: "https://example.gov/record",
      event_date: "2026-05-01",
      created_at: "2026-05-02T00:00:00.000Z",
      research_area_tags: [{ research_area_id: areaId, slug, name, stance: "for" as const }],
    });
    renderCandidate(() =>
      candidateDetail({
        records: [
          record("r-1", "a-env", "environment_and_public_health", "Environment and Public Health"),
          record("r-2", "a-gun", "gun_control", "Gun Control"),
        ],
      })
    );

    // "My issues first" is the default once saved areas load; Environment
    // sits second there but first under "By issue" (public salience), so
    // switching views reorders the groups.
    const select = await screen.findByRole("combobox");
    await screen.findByRole("option", { name: "My issues first" });
    const groupDetails = (name: string) =>
      screen.getByText(name).closest("details") as HTMLDetailsElement;
    expect(groupDetails("Environment and Public Health").open).toBe(false);

    const user = userEvent.setup();
    await user.click(screen.getByText("Environment and Public Health"));
    expect(groupDetails("Environment and Public Health").open).toBe(true);

    // No `open` prop means React re-applies no default on reorder — the
    // reader's toggle must survive the switch.
    await user.selectOptions(select, "by_issue");
    expect(groupDetails("Environment and Public Health").open).toBe(true);
    expect(groupDetails("Gun Control").open).toBe(false);
  });

  it("collapses campaign finance by default while keeping it in the DOM", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => ({
      ...candidateDetail({ elections: [candidateElection()] }),
      ongoing_finance: { "ce-1": financeSummary() },
    }));

    const heading = await screen.findByRole("heading", { name: "Campaign Finance Information — Governor" });
    // The heading sits OUTSIDE the disclosure (a heading inside <summary>
    // can drop out of screen-reader heading navigation); the details is its
    // sibling within the section.
    expect(heading.closest("details")).toBeNull();
    const details = heading.closest("section")?.querySelector("details");
    expect(details).toBeTruthy();
    expect(details!.open).toBe(false);
    // Collapsed, not absent: SSR HTML must keep finance crawler-readable.
    expect(screen.getByText("$120,000")).toBeInTheDocument();
  });

  it("scopes each record's For/Against chip to the group it renders under", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    // One record, for one area and against another: each group's copy must
    // show only its own area's direction.
    renderCandidate(() =>
      candidateDetail({
        records: [
          {
            id: "r-mixed",
            description: "Backed a housing bill that cut transit funding.",
            source_url: "https://example.gov/record",
            event_date: "2026-05-01",
            created_at: "2026-05-02T00:00:00.000Z",
            research_area_tags: [
              { research_area_id: "a-housing", slug: "housing", name: "Housing", stance: "for" },
              { research_area_id: "a-transit", slug: "transit", name: "Transit", stance: "against" },
            ],
          },
        ],
      })
    );

    await screen.findByRole("heading", { name: "Jordan Voter" });
    // The chip names its topic so a card read without its group heading
    // still says what the stance is about.
    expect(screen.getByText("Supports Housing")).toBeInTheDocument();
    expect(screen.getByText("Opposes Transit")).toBeInTheDocument();
  });

  it("uses evidence wording, not advocacy verbs, for judicial evaluative areas", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    // On legal_competence/impartiality a for/against tag grades the
    // EVIDENCE (favorable/unfavorable) — "Opposes Legal Competence" would
    // claim an intent the data never asserted.
    renderCandidate(() =>
      candidateDetail({
        records: [
          {
            id: "r-judicial",
            description: "Had two rulings reversed on appeal during 2024.",
            source_url: "https://example.gov/opinions",
            event_date: "2024-08-01",
            created_at: "2026-05-02T00:00:00.000Z",
            research_area_tags: [
              {
                research_area_id: "a-competence",
                slug: "legal_competence",
                name: "Legal Competence",
                stance: "against",
              },
              {
                research_area_id: "a-impartiality",
                slug: "impartiality",
                name: "Impartiality",
                stance: "for",
              },
            ],
          },
        ],
      })
    );

    await screen.findByRole("heading", { name: "Jordan Voter" });
    expect(screen.getByText("Unfavorable on Legal Competence")).toBeInTheDocument();
    expect(screen.getByText("Favorable on Impartiality")).toBeInTheDocument();
    expect(screen.queryByText("Opposes Legal Competence")).not.toBeInTheDocument();
    expect(screen.queryByText("Supports Impartiality")).not.toBeInTheDocument();
  });

  it("keeps mixed records chipless in the newest view and spells out per-tag stances", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() =>
      candidateDetail({
        records: [
          {
            id: "r-mixed",
            description: "Backed a housing bill that cut transit funding.",
            source_url: "https://example.gov/record",
            event_date: "2026-05-01",
            created_at: "2026-05-02T00:00:00.000Z",
            research_area_tags: [
              { research_area_id: "a-housing", slug: "housing", name: "Housing", stance: "for" },
              { research_area_id: "a-transit", slug: "transit", name: "Transit", stance: "against" },
            ],
          },
        ],
      })
    );

    const user = userEvent.setup();
    await user.selectOptions(await screen.findByRole("combobox"), "newest");

    // The flat view has no single chip — the tag list carries each area's
    // stance in the same verb phrasing as the grouped chip.
    expect(screen.getByText("Supports Housing")).toBeInTheDocument();
    expect(screen.getByText("Opposes Transit")).toBeInTheDocument();
  });

  it("renders the profile report button after the record and election sections", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => candidateDetail({ elections: [candidateElection()] }));

    await screen.findByRole("heading", { name: "Jordan Voter" });
    const button = screen.getByRole("button", { name: "Report an issue with candidate profile" });
    const electionsHeading = screen.getByRole("heading", { name: "Elections" });
    expect(electionsHeading.compareDocumentPosition(button) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("cuts the newest-first view off at 20 with a show-all button", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const records = Array.from({ length: 25 }, (_, index) => ({
      id: `r-${index + 1}`,
      description: `Record number ${index + 1}.`,
      source_url: "https://example.gov/record",
      event_date: "2026-05-01",
      created_at: "2026-05-02T00:00:00.000Z",
      research_area_tags: [],
    }));
    renderCandidate(() => candidateDetail({ records }));

    const user = userEvent.setup();
    await user.selectOptions(await screen.findByRole("combobox"), "newest");

    expect(screen.getByText("Record number 20.")).toBeInTheDocument();
    expect(screen.queryByText("Record number 21.")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Show all 25 records" }));
    expect(screen.getByText("Record number 25.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Show all/ })).not.toBeInTheDocument();
  });

  it("says no verified records when the empty list was actually researched", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => candidateDetail({ records: [], records_researched_through: "2026-07-10" }));

    expect(
      await screen.findByText(
        "No verified public records for this candidate — record history researched through July 10, 2026."
      )
    ).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Track record" })).not.toBeInTheDocument();
  });

  it("says records are not researched yet when there is no research checkpoint", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => candidateDetail({ records: [], records_researched_through: null }));

    expect(
      await screen.findByText("This candidate's record history has not been researched yet.")
    ).toBeInTheDocument();
    expect(screen.queryByText(/No verified public records/)).not.toBeInTheDocument();
  });

  it("shows logged-out visitors a Follow button that prompts them to register", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => candidateDetail());

    const followButton = await screen.findByRole("button", { name: "Follow" });
    await userEvent.click(followButton);

    expect(await screen.findByText("Get a monthly update on Jordan Voter with a free account.")).toBeInTheDocument();
    // Both links carry the candidate page as the post-auth return path.
    expect(screen.getByRole("link", { name: "Sign up" })).toHaveAttribute(
      "href",
      "/register?next=%2Fcandidates%2Fc-1"
    );
    expect(screen.getByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Fcandidates%2Fc-1"
    );
  });

  it("shows no follow controls to logged-in but unverified users", async () => {
    // The follows endpoint is verified-email-gated, and the register prompt
    // would be wrong for someone who already registered.
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderCandidate(() => candidateDetail());

    expect(await screen.findByRole("heading", { name: "Jordan Voter" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Follow" })).not.toBeInTheDocument();
  });

  it("shows the follow button as Unfollow once the follows list confirms it", async () => {
    // The anonymous loader payload always carries is_following=false; the
    // button must reflect the client-fetched follows list, not the payload.
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [candidateFollow()] } },
    });
    renderCandidate(() => candidateDetail());

    expect(await screen.findByRole("button", { name: "Unfollow" })).toBeInTheDocument();
  });

  it("shows loader-fetched finance for an ongoing election", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    // Finance rides in the loader payload (SSR-rendered for crawlers), not
    // in a client-side query.
    renderCandidate(() => ({
      ...candidateDetail({ elections: [candidateElection()] }),
      ongoing_finance: { "ce-1": financeSummary() },
    }));

    // The accessible name carries the election for screen-reader heading
    // navigation (a candidate can be in two concurrent races).
    expect(await screen.findByRole("heading", { name: "Campaign Finance Information — Governor" })).toBeInTheDocument();
    expect(screen.getByText("$120,000")).toBeInTheDocument();
    expect(screen.getByText("Top disclosed occupations of direct donors")).toBeInTheDocument();
  });

  it("renders no finance section when the ongoing election has no finance for the candidate", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => ({
      ...candidateDetail({ elections: [candidateElection()] }),
      ongoing_finance: { "ce-1": null },
    }));

    expect(await screen.findByRole("heading", { name: "Jordan Voter" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /Campaign Finance/i })).not.toBeInTheDocument();
  });

  it("loader fetches ongoing-election finance, skips past elections, and survives a finance failure", async () => {
    // Two ongoing races (one finance fetch fails) and one past race. Only
    // the ongoing races may be fetched — the past election's endpoint is
    // deliberately unmocked, so a stray fetch would throw loudly.
    stubApiRoutes({
      "/api/candidates/c-1": {
        body: candidateDetail({
          elections: [
            candidateElection(),
            candidateElection({ candidate_election_id: "ce-2", election_id: "e-2" }),
            candidateElection({
              candidate_election_id: "ce-past",
              election_id: "e-past",
              election_date: "2000-11-03",
            }),
          ],
        }),
      },
      "/api/elections/e-1/candidates/c-1/finance": { body: { finance_summary: financeSummary() } },
      "/api/elections/e-2/candidates/c-1/finance": apiError(500, "internal_error", "boom"),
    });

    const data = await loader({
      params: { candidateId: "c-1" },
      request: new Request("http://localhost/candidates/c-1"),
      context: {},
      url: new URL("http://localhost/candidates/c-1"),
      pattern: "/candidates/:candidateId",
    });

    expect(data.ongoing_finance["ce-1"]).toEqual(financeSummary());
    // The failed fetch degrades to "no finance" instead of failing the page.
    expect(data.ongoing_finance["ce-2"]).toBeNull();
    expect("ce-past" in data.ongoing_finance).toBe(false);
  });

  it("shows no finance on past-election rows and never fetches it", async () => {
    const fetchMock = stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-1/candidates/c-1/finance": { body: { finance_summary: financeSummary() } },
    });
    renderCandidate(() =>
      candidateDetail({ elections: [candidateElection({ election_date: "2000-11-03" })] })
    );

    expect(await screen.findByRole("heading", { name: "Elections" })).toBeInTheDocument();
    // Campaign finance shows only for elections the candidate is currently
    // in; past rows carry no disclosure and trigger no fetch.
    expect(screen.queryByText("Campaign Finance Information")).not.toBeInTheDocument();
    const financeCalls = fetchMock.mock.calls.filter((call) =>
      String(call[0]).includes("/api/elections/")
    ).length;
    expect(financeCalls).toBe(0);
  });

  it("submits record reports with the candidate record target", async () => {
    let submittedBody: unknown = null;
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/content-reports": (_url, init) => {
        submittedBody = JSON.parse(String(init?.body));
        return { status: 201, body: { report: { id: "report-1" } } };
      },
    });
    renderCandidate(() => candidateDetail());

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Report an issue with candidate record" }));
    await user.type(screen.getByLabelText("Details"), "This record needs another source.");
    await user.click(screen.getByRole("button", { name: "Send report" }));

    expect(await screen.findByText("Report sent. Thank you.")).toBeInTheDocument();
    expect(submittedBody).toEqual({
      entity_type: "candidate_record",
      entity_id: "r-1",
      message: "This record needs another source.",
    });
  });
});
