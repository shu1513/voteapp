export const GENERAL_RESEARCH_AREA_SLUG = "general";
export const INTEGRITY_AND_ETHICS_RESEARCH_AREA_SLUG = "integrity_and_ethics";

export const NON_STANCE_RESEARCH_AREA_SLUGS = new Set<string>([
  GENERAL_RESEARCH_AREA_SLUG,
  INTEGRITY_AND_ETHICS_RESEARCH_AREA_SLUG,
]);

export function isNonStanceResearchAreaSlug(slug: string): boolean {
  return NON_STANCE_RESEARCH_AREA_SLUGS.has(slug.trim().toLowerCase());
}
