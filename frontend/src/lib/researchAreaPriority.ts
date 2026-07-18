// Display priority for research-area chips, ranked by real-world public
// salience (2026 Gallup/Pew issue polling), not alphabetically — the chips a
// card has room for should be the ones voters care about most. Keyed by slug
// so renames in the DB don't silently break the ordering.
const RESEARCH_AREA_PRIORITY: readonly string[] = [
  "environment_and_public_health",
  "reduce_wealth_gap",
  "anti_corruption",
  "government_efficiency",
  "healthcare_affordability",
  "cost_of_living_reduction",
  "immigration",
  "government_spending_reduction",
  "public_safety_and_crime_control",
  "housing_affordability",
  "social_programs_and_welfare",
  "public_education_quality",
  "gun_control",
  "womens_reproductive_rights",
  "election_integrity",
  "national_defense",
  "personal_income_tax_reduction",
  "foreign_trade",
  "data_privacy",
  "civil_rights",
  "corporate_accountability",
  "public_infrastructure",
  "peaceful_foreign_policy",
];

const rankBySlug = new Map(RESEARCH_AREA_PRIORITY.map((slug, index) => [slug, index]));

/**
 * Compares two research areas by public-salience priority (highest first).
 * Slugs outside the ranking — the judicial criteria (impartiality, legal
 * competence, integrity and ethics), the "general" catch-all, and any area
 * added later — sink below every ranked one, alphabetically so their order
 * is still deterministic. Exported on its own so personalized sorts can use
 * it as their tiebreak.
 */
export function compareByResearchAreaPriority(
  a: { slug: string; name: string },
  b: { slug: string; name: string }
): number {
  const rankA = rankBySlug.get(a.slug) ?? Number.POSITIVE_INFINITY;
  const rankB = rankBySlug.get(b.slug) ?? Number.POSITIVE_INFINITY;
  if (rankA !== rankB) {
    return rankA - rankB;
  }
  return a.name.localeCompare(b.name);
}

/** compareByResearchAreaPriority over a copy; the input is not mutated. */
export function sortByResearchAreaPriority<T extends { slug: string; name: string }>(
  areas: readonly T[]
): T[] {
  return [...areas].sort(compareByResearchAreaPriority);
}
