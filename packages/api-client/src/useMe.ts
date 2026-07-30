import { useMutation, useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest } from "./client";

/**
 * Drops every cached query except the exact ["me"] identity entry.
 * Account-scoped data like ["me","ballot"] must never leak across sessions
 * on a shared browser — called on logout AND on login, because a previous
 * session may have ended without a clean logout (expiry, failed request,
 * or another person walking straight to /login).
 */
export function purgeAccountScopedQueries(queryClient: QueryClient): void {
  queryClient.removeQueries({
    predicate: (query) => query.queryKey[0] !== "me" || query.queryKey.length > 1,
  });
}

export type Me = {
  email: string;
  first_name: string;
  email_verified: boolean;
  accepted_terms_version: string | null;
};

/**
 * Session state for the whole app. `me` is undefined while loading, null when
 * logged out (401), and the user otherwise. GET /api/me is the only identity
 * endpoint that works for unverified users, so email_verified drives the
 * verification interstitial.
 */
export function useMe() {
  const query = useQuery<Me | null>({
    queryKey: ["me"],
    queryFn: async () => {
      try {
        const response = await apiRequest<{ user: Me }>("/api/me");
        return response.user;
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) {
          return null;
        }
        throw error;
      }
    },
    retry: false,
    staleTime: 60_000,
  });
  return { me: query.data, isLoading: query.isPending, isError: query.isError, refetch: query.refetch };
}

/**
 * Records the signed-in user's acceptance of the current terms bundle after
 * a version bump. The backend rejects any version other than its current
 * one, mirroring registration's clickwrap rule.
 */
export function useAcceptTerms() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: {
      acceptedTermsVersion: string;
      presentationVersion: string;
      acceptanceId: string;
      subjectId: string;
    }) =>
      apiRequest<{ user: Me }>("/api/me/terms-acceptance", {
        method: "POST",
        body: {
          accepted_terms_version: input.acceptedTermsVersion,
          legal_presentation_version: input.presentationVersion,
          legal_acceptance_id: input.acceptanceId,
          legal_subject_id: input.subjectId,
        },
      }),
    onSuccess: (response) => {
      queryClient.setQueryData(["me"], response.user);
    },
  });
}

export function useLogout() {
  const queryClient = useQueryClient();
  return useMutation({
    // Logout requires a JSON content-type: the backend uses it as a CSRF
    // guard against plain cross-site form POSTs.
    mutationFn: () => apiRequest<{ status: string }>("/api/auth/logout", { method: "POST", body: {} }),
    onSuccess: () => {
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
    },
  });
}
