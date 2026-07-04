import { useMutation, useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest } from "../api/client";

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
