import type { QueryClient } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { Me, ResearchAreaPreferencesResult } from "@voteapp/api-client";
import { hasSeenWelcome } from "./welcomeSeen";

// First-login onboarding hook-in, shared by password login, Google login,
// and Google signup: a verified user with no saved research areas who hasn't
// been through the welcome step gets routed there instead of the ballot. Any
// lookup failure falls back to the ballot — login must never strand the user
// on an error because an optional step couldn't be checked.
export async function postLoginDestination(queryClient: QueryClient): Promise<string> {
  const me = queryClient.getQueryData<Me | null>(["me"]);
  if (!me?.email_verified || hasSeenWelcome(me.email)) {
    return "/me/ballot";
  }
  try {
    // fetchQuery, not a bare request: it seeds the cache the welcome page
    // and settings editor read from.
    const prefs = await queryClient.fetchQuery({
      queryKey: ["me", "research-area-preferences"],
      queryFn: () => apiRequest<ResearchAreaPreferencesResult>("/api/me/research-area-preferences"),
      staleTime: 60_000,
    });
    return prefs.preferences.length === 0 ? "/me/welcome" : "/me/ballot";
  } catch {
    return "/me/ballot";
  }
}
