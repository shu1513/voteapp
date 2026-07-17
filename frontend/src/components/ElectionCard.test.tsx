import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import { ElectionList } from "./ElectionCard";
import { renderRoutes } from "../test/render";
import { electionSummary } from "../test/fixtures";
import type { ElectionSummary } from "@voteapp/api-client";

function area(id: string, name: string) {
  return { id, slug: id, name, description: null };
}

// ElectionCard is private to ElectionList (it omits its own date), so the
// card's chip behavior is exercised through a single-election list.
function renderCard(election: ElectionSummary, savedAreaIds?: Set<string>) {
  return renderRoutes(
    [{ path: "/", element: <ElectionList elections={[election]} savedAreaIds={savedAreaIds} /> }],
    "/"
  );
}

describe("ElectionCard", () => {
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

    expect(screen.getByText("Affected areas:")).toBeInTheDocument();
    expect(screen.getByText("Civil Rights")).toBeInTheDocument();
    expect(screen.getByText("Gun Control")).toBeInTheDocument();
    expect(screen.getByText("Housing Affordability")).toBeInTheDocument();
    expect(screen.queryByText("Data Privacy")).not.toBeInTheDocument();
    expect(screen.queryByText("Public Infrastructure")).not.toBeInTheDocument();
    expect(screen.getByText("+2 more issues")).toBeInTheDocument();
  });

  it("omits the affected-areas row when a race has no research areas", () => {
    renderCard(electionSummary({ research_areas: [] }));
    expect(screen.queryByText("Affected areas:")).not.toBeInTheDocument();
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
      new Set(["a-3"])
    );

    const label = screen.getByText("Affected areas:");
    const chipTexts = Array.from(label.parentElement?.children ?? []).map(
      (chip) => chip.textContent
    );
    // Saved match leads even though it is last in the payload.
    expect(chipTexts).toEqual(["Affected areas:", "Housing Affordability", "Civil Rights", "Gun Control"]);
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
      new Set(["a-4", "a-5"])
    );

    // Saved matches render regardless of position in the payload…
    expect(screen.getByText("Data Privacy")).toBeInTheDocument();
    expect(screen.getByText("Public Infrastructure")).toBeInTheDocument();
    // …and the unsaved chips still fit under the cap, so no overflow chip.
    expect(screen.getByText("Civil Rights")).toBeInTheDocument();
    expect(screen.queryByText(/more issue/)).not.toBeInTheDocument();
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
    expect(screen.queryByText(/more issue/)).not.toBeInTheDocument();
  });
});
