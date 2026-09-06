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
  /** False on Google-created accounts that never set a password: Settings
   * swaps its password-gated forms for an "add a password" hint. */
  has_password: boolean;
};

/**
 * The login/logout purges only run in THIS tab's callbacks. When another tab
 * on a shared browser signs out, or signs in as someone else, the cookie
 * changes underneath this tab and its next GET /api/me returns the new
 * identity — while ["me","ballot"], ["me","districts"], ["me",
 * "election-choices"] … still hold the previous account's private data
 * (staleTime never expires an entry, and disabled hooks keep reading
 * query.data). So the identity fetch itself purges on a transition, BEFORE
 * its result reaches the cache: no render can ever pair the new identity
 * with the old account's data (an effect-based purge would paint one such
 * frame first). `Me` exposes no id, so email is the identity; an email
 * change on the same account costs one harmless refetch.
 */
function purgeIfIdentityChanged(queryClient: QueryClient, next: Me | null): void {
  const previous = queryClient.getQueryData<Me | null>(["me"]);
  if (previous === undefined || previous === null) {
    return;
  }
  if (next === null || next.email !== previous.email) {
    purgeAccountScopedQueries(queryClient);
  }
}

/**
 * Session state for the whole app. `me` is undefined while loading, null when
 * logged out (401), and the user otherwise. GET /api/me is the only identity
 * endpoint that works for unverified users, so email_verified drives the
 * verification interstitial.
 */
export function useMe() {
  const queryClient = useQueryClient();
  const query = useQuery<Me | null>({
    queryKey: ["me"],
    queryFn: async () => {
      let next: Me | null;
      try {
        const response = await apiRequest<{ user: Me }>("/api/me");
        next = response.user;
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) {
          next = null;
        } else {
          throw error;
        }
      }
      purgeIfIdentityChanged(queryClient, next);
      return next;
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
    mutationFn: (acceptedTermsVersion: string) =>
      apiRequest<{ user: Me }>("/api/me/terms-acceptance", {
        method: "POST",
        body: { accepted_terms_version: acceptedTermsVersion },
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
