import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { CandidatePage, ErrorBoundary } from "./CandidatePage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import {
  candidateDetail,
  candidateElection,
  candidateFollow,
  emptyFinanceSummary,
  financeSummary,
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

  it("opens the first issue groups and collapses the rest", async () => {
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
    expect(groupState("Civil Rights")).toBe(true);
    expect(groupState("Gun Control")).toBe(true);
    expect(groupState("Housing")).toBe(true);
    expect(groupState("Privacy")).toBe(false);
    // Collapsed groups still state their size.
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
    expect(screen.queryByRole("heading", { name: "Record" })).not.toBeInTheDocument();
  });

  it("says records are not researched yet when there is no research checkpoint", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderCandidate(() => candidateDetail({ records: [], records_researched_through: null }));

    expect(
      await screen.findByText("This candidate's record history has not been researched yet.")
    ).toBeInTheDocument();
    expect(screen.queryByText(/No verified public records/)).not.toBeInTheDocument();
  });

  it("shows the follow button as Following once the follows list confirms it", async () => {
    // The anonymous loader payload always carries is_following=false; the
    // button must reflect the client-fetched follows list, not the payload.
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/candidate-follows": { body: { follows: [candidateFollow()] } },
    });
    renderCandidate(() => candidateDetail());

    expect(await screen.findByRole("button", { name: "Following" })).toBeInTheDocument();
  });

  it("shows finance for an ongoing election from the per-candidate endpoint", async () => {
    // The narrow endpoint carries the candidate id, so the server — not the
    // client — scopes the summary to this candidate.
    const fetchMock = stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-1/candidates/c-1/finance": { body: { finance_summary: financeSummary() } },
    });
    renderCandidate(() => candidateDetail({ elections: [candidateElection()] }));

    // The accessible name carries the election for screen-reader heading
    // navigation (a candidate can be in two concurrent races).
    expect(await screen.findByRole("heading", { name: "Campaign finance — Governor" })).toBeInTheDocument();
    expect(screen.getByText("$120,000")).toBeInTheDocument();
    expect(screen.getByText("Top disclosed occupations of direct donors")).toBeInTheDocument();
    // No-leak guard moved server-side: the client's only obligation is to
    // request exactly this candidate's summary.
    expect(
      fetchMock.mock.calls.some((call) => String(call[0]).includes("/api/elections/e-1/candidates/c-1/finance"))
    ).toBe(true);
  });

  it("renders no finance section when the ongoing election has no finance for the candidate", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-1/candidates/c-1/finance": { body: { finance_summary: null } },
    });
    renderCandidate(() => candidateDetail({ elections: [candidateElection()] }));

    expect(await screen.findByRole("heading", { name: "Jordan Voter" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /Campaign finance/ })).not.toBeInTheDocument();
  });

  it("keeps the profile intact when the finance fetch fails", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-1/candidates/c-1/finance": apiError(500, "internal_error", "boom"),
    });
    renderCandidate(() => candidateDetail({ elections: [candidateElection()] }));

    expect(await screen.findByRole("heading", { name: "Jordan Voter" })).toBeInTheDocument();
    expect(screen.getByText("Voted for the clean water act.")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /Campaign finance/ })).not.toBeInTheDocument();
  });

  it("fetches past-election finance only after the row's disclosure is opened", async () => {
    const fetchMock = stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-1/candidates/c-1/finance": { body: { finance_summary: financeSummary() } },
    });
    renderCandidate(() =>
      candidateDetail({ elections: [candidateElection({ election_date: "2000-11-03" })] })
    );

    expect(await screen.findByRole("heading", { name: "Elections" })).toBeInTheDocument();
    // The toggle's accessible name carries the election and date so repeated
    // rows stay distinguishable for screen-reader users.
    expect(screen.getByText("Campaign finance")).toHaveAccessibleName(
      "Campaign finance for Governor, November 3, 2000"
    );
    // Past rows must not preload their finance payloads.
    const financeCalls = () =>
      fetchMock.mock.calls.filter((call) => String(call[0]).includes("/api/elections/")).length;
    expect(financeCalls()).toBe(0);

    const user = userEvent.setup();
    await user.click(screen.getByText("Campaign finance"));

    expect(await screen.findByText("$120,000")).toBeInTheDocument();
    expect(financeCalls()).toBe(1);
    expect(String(fetchMock.mock.calls.at(-1)?.[0])).toContain("/api/elections/e-1/candidates/c-1/finance");
  });

  it("says so when an opened past election has no finance data", async () => {
    stubApiRoutes({
      ...ANONYMOUS,
      "/api/elections/e-1/candidates/c-1/finance": { body: { finance_summary: emptyFinanceSummary() } },
    });
    renderCandidate(() =>
      candidateDetail({ elections: [candidateElection({ election_date: "2000-11-03" })] })
    );

    const user = userEvent.setup();
    await user.click(await screen.findByText("Campaign finance"));

    expect(await screen.findByText("No finance data for this election.")).toBeInTheDocument();
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
