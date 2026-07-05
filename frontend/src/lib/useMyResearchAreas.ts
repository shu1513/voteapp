import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { ResearchAreaPreference, ResearchAreaPreferencesResult } from "../api/types";
import { useMe } from "./useMe";
import { buildResearchAreaWeights, type ResearchAreaWeight } from "./researchAreaScoring";

/**
 * The session holder's saved research areas, shaped for personalization:
 * membership checks (highlighting) and weights (client-side sorts). Shares
 * the query key with the settings editor so both views stay in sync, and is
 * only fetched for verified users — the endpoint is verified-email-gated,
 * and anonymous visitors get the un-personalized experience.
 */
export function useMyResearchAreas(): {
  preferences: ResearchAreaPreference[];
  savedAreaIds: Set<string>;
  weights: Map<string, ResearchAreaWeight>;
  hasSaved: boolean;
} {
  const { me } = useMe();
  const query = useQuery({
    queryKey: ["me", "research-area-preferences"],
    queryFn: () => apiRequest<ResearchAreaPreferencesResult>("/api/me/research-area-preferences"),
    enabled: me?.email_verified === true,
    staleTime: 60_000,
  });

  const preferences = query.data?.preferences ?? [];
  return {
    preferences,
    savedAreaIds: new Set(preferences.map((preference) => preference.research_area_id)),
    weights: buildResearchAreaWeights(preferences),
    hasSaved: preferences.length > 0,
  };
}
