import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest } from "../api/client";

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
      // Drop every cached query except the exact ["me"] entry just set:
      // account-scoped data like ["me","ballot"] must not survive into the
      // next login on a shared browser.
      queryClient.removeQueries({
        predicate: (query) => query.queryKey[0] !== "me" || query.queryKey.length > 1,
      });
    },
  });
}
