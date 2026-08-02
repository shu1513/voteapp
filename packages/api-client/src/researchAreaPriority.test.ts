import { describe, expect, it } from "vitest";
import { sortByResearchAreaPriority } from "./researchAreaPriority";

function area(slug: string, name: string) {
  return { slug, name };
}

describe("sortByResearchAreaPriority", () => {
  it("orders ranked areas by priority regardless of input order", () => {
    const sorted = sortByResearchAreaPriority([
      area("civil_rights", "Civil Rights"),
      area("anti_corruption", "Anti-Corruption"),
      area("environment_and_public_health", "Environment and Public Health"),
      area("healthcare_affordability", "Healthcare Affordability"),
      area("reduce_wealth_gap", "Reduce Wealth Gap"),
    ]);
    expect(sorted.map((a) => a.slug)).toEqual([
      "healthcare_affordability",
      "environment_and_public_health",
      "reduce_wealth_gap",
      "anti_corruption",
      "civil_rights",
    ]);
  });

  it("sinks unranked areas below every ranked one, alphabetically", () => {
    const sorted = sortByResearchAreaPriority([
      area("legal_competence", "Legal Competence"),
      area("general", "General"),
      // Last-ranked area still beats every unranked one.
      area("peaceful_foreign_policy", "Peaceful Foreign Policy"),
      area("impartiality", "Impartiality"),
    ]);
    expect(sorted.map((a) => a.slug)).toEqual([
      "peaceful_foreign_policy",
      "general",
      "impartiality",
      "legal_competence",
    ]);
  });

  it("does not mutate the input array", () => {
    const input = [area("gun_control", "Gun Control"), area("immigration", "Immigration")];
    const copy = [...input];
    sortByResearchAreaPriority(input);
    expect(input).toEqual(copy);
  });
});
