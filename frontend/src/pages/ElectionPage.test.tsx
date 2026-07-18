import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import { ElectionPage, ErrorBoundary } from "./ElectionPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { electionDetail, financeSummary, ME_VERIFIED, VOTE_POWER_WITH_EXPLANATION } from "../test/fixtures";

const ANONYMOUS = { "/api/me": apiError(401, "unauthorized", "Not logged in") };

// The subject arrives via the route loader (server-fetched in production);
// tests supply it directly instead of stubbing the loader's fetch.
function renderElection(loader: () => unknown, id = "e-1") {
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
      { path: "/disclaimer", element: <p /> },
    ],
    `/elections/${id}`
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
    expect(screen.getByRole("button", { name: "Report an issue with election" })).toBeInTheDocument();
  });

  it("renders a collapsed vote power explanation when the detail payload carries one", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail({ vote_power: VOTE_POWER_WITH_EXPLANATION }));

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.getByText("Why this vote power rating?")).toBeInTheDocument();
    // Native <details> keeps content in the DOM while collapsed; the backend
    // copy must arrive verbatim.
    expect(screen.getByText("Vote power = representation + decisiveness.")).toBeInTheDocument();
    // Each part renders formula-style: title, grade, stat, then the detail.
    expect(screen.getByText("Representation:")).toBeInTheDocument();
    expect(screen.getByText("· 50 out of 100")).toBeInTheDocument();
    expect(screen.getByText("· 3.3-point margin in 2022")).toBeInTheDocument();
    expect(
      screen.getByText("Past results here were very close — a small number of votes could decide the winner.")
    ).toBeInTheDocument();
    expect(screen.getByText("Medium representation + high decisiveness → High vote power.")).toBeInTheDocument();
    expect(screen.getByText("Some data is missing.")).toBeInTheDocument();
    // The exact formula renders when the backend provides one; the null
    // formula on the other part must not render an empty line.
    expect(screen.getByText("score = 100 × ln(9,808,667 ÷ 104,650) ÷ ln(9,808,667 ÷ 1,204) = 50")).toBeInTheDocument();
  });

  it("omits the vote power explanation when the payload has none or the label is unknown", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() => electionDetail());

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.queryByText("Why this vote power rating?")).not.toBeInTheDocument();
  });

  it("hides the explanation without crashing when an old backend sends the pre-parts shape", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const legacyExplanation = {
      how: "Vote power combines representation and decisiveness.",
      reasons: ["Representation is medium."],
      caveat: null,
    } as unknown as NonNullable<typeof VOTE_POWER_WITH_EXPLANATION.explanation>;
    renderElection(() =>
      electionDetail({ vote_power: { ...VOTE_POWER_WITH_EXPLANATION, explanation: legacyExplanation } })
    );

    expect(await screen.findByRole("heading", { name: "Governor" })).toBeInTheDocument();
    expect(screen.queryByText("Why this vote power rating?")).not.toBeInTheDocument();
  });

  it("hides the vote power explanation entirely for an unknown label", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    renderElection(() =>
      electionDetail({
        vote_power: { ...VOTE_POWER_WITH_EXPLANATION, label: "unknown", score: null },
      })
    );

    await screen.findByRole("heading", { name: "Governor" });
    expect(screen.queryByText(/Vote power:/)).not.toBeInTheDocument();
    expect(screen.queryByText("Why this vote power rating?")).not.toBeInTheDocument();
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

  it("renders a collapsed campaign finance disclosure only for candidates with finance data", async () => {
    stubApiRoutes({ ...ANONYMOUS });
    const detail = electionDetail();
    detail.candidates[0].finance_summary = financeSummary();
    renderElection(() => detail);

    // One candidate has finance, the other (finance_summary: null) must not
    // grow an empty disclosure.
    expect(await screen.findByText("Jordan Voter")).toBeInTheDocument();
    expect(screen.getAllByText("Campaign finance")).toHaveLength(1);
    // The toggle's accessible name carries the candidate so repeated
    // disclosures stay distinguishable for screen-reader users.
    expect(screen.getByText("Campaign finance")).toHaveAccessibleName("Campaign finance for Jordan Voter");
    // The panel content is in the DOM (details renders children; collapsed
    // is a display state) with occupations and industries distinct.
    expect(screen.getByText("Top disclosed occupations of direct donors")).toBeInTheDocument();
    expect(screen.getByText("Retired")).toBeInTheDocument();
    expect(screen.getByText("Oil, gas, and energy")).toBeInTheDocument();
    expect(screen.getByText("Growth PAC")).toBeInTheDocument();
    // The header chip still summarizes the total.
    expect(screen.getByText("Raised $120,000")).toBeInTheDocument();
    // The card link to the profile survives the card restructure, and the
    // disclosure is not nested inside it.
    const cardLink = screen.getByRole("link", { name: /Jordan Voter/ });
    expect(cardLink).toHaveAttribute("href", "/candidates/c-1");
    expect(cardLink.querySelector("details")).toBeNull();
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
