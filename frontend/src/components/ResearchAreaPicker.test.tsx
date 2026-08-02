import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { ResearchAreaPicker } from "./ResearchAreaPicker";

function area(id: string, slug: string, name: string) {
  return { id, slug, name, description: null };
}

describe("ResearchAreaPicker", () => {
  it("lists unselected areas in public-salience order, unranked areas last", () => {
    render(
      <ResearchAreaPicker
        // Deliberately shuffled; the catalog API returns alphabetical order.
        areas={[
          area("a-legal", "legal_competence", "Legal Competence"),
          area("a-env", "environment_and_public_health", "Environment and Public Health"),
          area("a-health", "healthcare_affordability", "Healthcare Affordability"),
          area("a-wealth", "reduce_wealth_gap", "Reduce Wealth Gap"),
        ]}
        orderedIds={[]}
        disabled={false}
        onChange={() => {}}
      />
    );

    const names = screen.getAllByRole("button").map((button) => button.textContent);
    expect(names).toEqual([
      "Healthcare Affordability",
      "Environment and Public Health",
      "Reduce Wealth Gap",
      "Legal Competence",
    ]);
  });
});
