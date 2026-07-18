import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import { ElectionList } from "./ElectionCard";
import { renderRoutes } from "../test/render";
import { electionSummary, VOTE_POWER } from "../test/fixtures";
import { buildResearchAreaWeights } from "@voteapp/api-client";
import type { ElectionSummary } from "@voteapp/api-client";

// Test ids double as slugs by default; slugs like "a-1" are unranked in the
// priority list, so those chips fall back to alphabetical order. Pass a real
// slug to exercise the priority ranking.
function area(id: string, name: string, slug = id) {
  return { id, slug, name, description: null };
}

// Saved-preference map shaped like useMyResearchAreas().weights; null means
// the area is saved but the user never ranked it.
function savedWeights(ranks: Record<string, number | null>) {
  return buildResearchAreaWeights(
    Object.entries(ranks).map(([research_area_id, rank]) => ({
      research_area_id,
      rank,
      // The card only reads membership and rank; the display fields come
      // from the election payload, so placeholders suffice here.
      slug: research_area_id,
      name: research_area_id,
      description: null,
    }))
  );
}

// ElectionCard is private to ElectionList (it omits its own date), so the
// card's chip behavior is exercised through a single-election list.
function renderCard(election: ElectionSummary, savedAreaRanks?: Record<string, number | null>) {
  return renderRoutes(
    [
      {
        path: "/",
        element: (
          <ElectionList
            elections={[election]}
            savedAreaWeights={savedAreaRanks ? savedWeights(savedAreaRanks) : undefined}
          />
        ),
      },
    ],
    "/"
  );
}

describe("ElectionCard", () => {
  it("puts vote power and the candidate count on the title row, without district meta", () => {
    renderCard(electionSummary());

    const row = screen.getByRole("heading", { name: "Governor" }).parentElement;
    expect(row).toHaveTextContent("Vote power: High");
    expect(row).toHaveTextContent("2 candidates");
    // The district/office meta line is gone — the ballot title names the race.
    expect(screen.queryByText(/Alaska/)).not.toBeInTheDocument();
  });

  it("omits the vote-power chip when the score is unknown", () => {
    renderCard(electionSummary({ vote_power: { ...VOTE_POWER, label: "unknown" } }));

    expect(screen.queryByText(/Vote power:/)).not.toBeInTheDocument();
    expect(screen.getByText("2 candidates")).toBeInTheDocument();
  });

  it("shows district names only on cards whose ballot titles collide", () => {
    renderRoutes(
      [
        {
          path: "/",
          element: (
            <ElectionList
              elections={[
                // Overlapping school districts: same generic title, one ballot.
                electionSummary({
                  id: "e-1",
                  official_ballot_title: "Board of Education Member",
                  district: {
                    id: "d-el",
                    district_type: "school_elementary",
                    name: "Yuma Elementary District, Arizona",
                    state: "AZ",
                  },
                }),
                electionSummary({
                  id: "e-2",
                  official_ballot_title: "Board of Education Member",
                  district: {
                    id: "d-hi",
                    district_type: "school_secondary",
                    name: "Yuma Union High School District, Arizona",
                    state: "AZ",
                  },
                }),
                electionSummary({ id: "e-3", official_ballot_title: "Governor" }),
              ]}
            />
          ),
        },
      ],
      "/"
    );

    // Colliding titles each carry their district as a disambiguator…
    expect(screen.getByText("Yuma Elementary District, Arizona")).toBeInTheDocument();
    expect(screen.getByText("Yuma Union High School District, Arizona")).toBeInTheDocument();
    // …while the unique title stays district-free ("Alaska" is its fixture district).
    expect(screen.queryByText("Alaska")).not.toBeInTheDocument();
  });

  it("labels ballot measures instead of counting candidates", () => {
    renderCard(electionSummary({ race_type: "ballot_measure", candidate_count: 0 }));

    expect(screen.getByText("Ballot measure")).toBeInTheDocument();
    expect(screen.queryByText(/candidates?/)).not.toBeInTheDocument();
  });

  it("caps unsaved research-area chips and counts the rest", () => {
    renderCard(
      electionSummary({
        research_areas: [
          area("a-1", "Civil Rights"),
          area("a-2", "Gun Control"),
          area("a-3", "Housing Affordability"),
          area("a-4", "Data Privacy"),
          area("a-5", "Public Infrastructure"),
        ],
      })
    );

    // These slugs are unranked, so they fall back to alphabetical order and
    // the cap keeps the first three names.
    expect(screen.getByText("Affected Areas:")).toBeInTheDocument();
    expect(screen.getByText("Civil Rights")).toBeInTheDocument();
    expect(screen.getByText("Data Privacy")).toBeInTheDocument();
    expect(screen.getByText("Gun Control")).toBeInTheDocument();
    expect(screen.queryByText("Housing Affordability")).not.toBeInTheDocument();
    expect(screen.queryByText("Public Infrastructure")).not.toBeInTheDocument();
    // The overflow count wears the same green as the area chips — it is part
    // of the same list, not a muted footnote.
    expect(screen.getByText("+2 more areas").className).toBe(screen.getByText("Civil Rights").className);
  });

  it("orders chips by public-salience priority, not the payload order", () => {
    renderCard(
      electionSummary({
        research_areas: [
          // Payload arrives alphabetical; priority rank must win.
          area("a-1", "Civil Rights", "civil_rights"),
          area("a-2", "Environment and Public Health", "environment_and_public_health"),
          area("a-3", "Gun Control", "gun_control"),
        ],
      })
    );

    const label = screen.getByText("Affected Areas:");
    const chipTexts = Array.from(label.parentElement?.children ?? [])
      .map((chip) => chip.textContent)
      .filter((text) => text !== "Affected Areas:");
    expect(chipTexts).toEqual(["Environment and Public Health", "Gun Control", "Civil Rights"]);
  });

  it("styles saved and unsaved chips alike, with a screen-reader-only saved marker", () => {
    renderCard(
      electionSummary({
        research_areas: [area("a-1", "Civil Rights"), area("a-2", "Gun Control")],
      }),
      { "a-2": 1 }
    );

    // Same green accent on both; visually the saved match just leads…
    expect(screen.getByText("Gun Control").className).toBe(screen.getByText("Civil Rights").className);
    // …while assistive tech still hears which chip is the user's.
    expect(screen.getByText("Gun Control")).toHaveTextContent("Gun Control (saved)");
    expect(screen.getByText("Civil Rights")).not.toHaveTextContent("(saved)");
  });

  it("orders saved chips by the user's own ranking, not public salience", () => {
    renderCard(
      electionSummary({
        research_areas: [
          // Environment far outranks Civil Rights in public salience; the
          // user's explicit 1–7 ranking must win anyway.
          area("a-env", "Environment and Public Health", "environment_and_public_health"),
          area("a-civ", "Civil Rights", "civil_rights"),
          // Saved but never ranked: sinks below ranked saves, still ahead of
          // unsaved chips.
          area("a-gun", "Gun Control", "gun_control"),
          area("a-imm", "Immigration", "immigration"),
        ],
      }),
      { "a-env": 2, "a-civ": 1, "a-gun": null }
    );

    const label = screen.getByText("Affected Areas:");
    const chipTexts = Array.from(label.parentElement?.children ?? [])
      .map((chip) => chip.textContent)
      .filter((text) => text !== "Affected Areas:");
    expect(chipTexts).toEqual([
      "Civil Rights (saved)",
      "Environment and Public Health (saved)",
      "Gun Control (saved)",
      "Immigration",
    ]);
  });

  it("omits the affected-areas row when a race has no research areas", () => {
    renderCard(electionSummary({ research_areas: [] }));
    expect(screen.queryByText("Affected Areas:")).not.toBeInTheDocument();
  });

  it("orders saved-area chips ahead of unsaved ones", () => {
    renderCard(
      electionSummary({
        research_areas: [
          area("a-1", "Civil Rights"),
          area("a-2", "Gun Control"),
          area("a-3", "Housing Affordability"),
        ],
      }),
      { "a-3": 1 }
    );

    // Filter the label out by text (not position) so this assertion covers
    // chip order only and survives DOM reshuffles around the label.
    const label = screen.getByText("Affected Areas:");
    const chipTexts = Array.from(label.parentElement?.children ?? [])
      .map((chip) => chip.textContent)
      .filter((text) => text !== "Affected Areas:");
    // Saved match leads even though it is last in the payload.
    expect(chipTexts).toEqual(["Housing Affordability (saved)", "Civil Rights", "Gun Control"]);
  });

  it("always shows saved-area matches, ahead of the cap", () => {
    renderCard(
      electionSummary({
        research_areas: [
          area("a-1", "Civil Rights"),
          area("a-2", "Gun Control"),
          area("a-3", "Housing Affordability"),
          area("a-4", "Data Privacy"),
          area("a-5", "Public Infrastructure"),
        ],
      }),
      { "a-4": null, "a-5": null }
    );

    // Saved matches render regardless of position in the payload…
    expect(screen.getByText("Data Privacy")).toBeInTheDocument();
    expect(screen.getByText("Public Infrastructure")).toBeInTheDocument();
    // …and the unsaved chips still fit under the cap, so no overflow chip.
    expect(screen.getByText("Civil Rights")).toBeInTheDocument();
    expect(screen.queryByText(/more area/)).not.toBeInTheDocument();
  });

  it("groups consecutive same-date elections under one date heading", () => {
    renderRoutes(
      [
        {
          path: "/",
          element: (
            <ElectionList
              elections={[
                electionSummary({ id: "e-1", official_ballot_title: "Governor" }),
                electionSummary({ id: "e-2", official_ballot_title: "State Senate" }),
                electionSummary({
                  id: "e-3",
                  official_ballot_title: "Special Runoff",
                  election_date: "2027-03-02",
                }),
              ]}
            />
          ),
        },
      ],
      "/"
    );

    // One heading for the shared date, one for the outlier — not one per card.
    expect(screen.getAllByRole("heading", { name: "November 3, 2026" })).toHaveLength(1);
    expect(screen.getByRole("heading", { name: "March 2, 2027" })).toBeInTheDocument();
    expect(screen.getByText("Governor")).toBeInTheDocument();
    expect(screen.getByText("State Senate")).toBeInTheDocument();
    expect(screen.getByText("Special Runoff")).toBeInTheDocument();
  });

  it("shows every chip when the list is short", () => {
    renderCard(electionSummary({ research_areas: [area("a-1", "Civil Rights")] }));

    expect(screen.getByText("Civil Rights")).toBeInTheDocument();
    expect(screen.queryByText(/more area/)).not.toBeInTheDocument();
  });
});
