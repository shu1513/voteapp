import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { MembershipStatus } from "@voteapp/api-client";

/**
 * GET /api/me/membership for the signed-in user. staleTime 0 (the app
 * default is 60s): the webhook that records a payment can land after the
 * return from Checkout, so every mount must ask again rather than reuse a
 * pre-webhook snapshot — no polling, just no caching. `enabled` lets a
 * surface that only sometimes needs the answer (the My Draft membership
 * ask) skip the request entirely otherwise. The query `signal` rides along
 * so the membership page can cancel an in-flight GET before installing a
 * mutation's result (a slower GET must not overwrite it).
 */
export const MEMBERSHIP_QUERY_KEY = ["me", "membership"] as const;

export function useMembershipStatus(options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: MEMBERSHIP_QUERY_KEY,
    queryFn: ({ signal }) => apiRequest<MembershipStatus>("/api/me/membership", { signal }),
    staleTime: 0,
    enabled: options.enabled ?? true,
  });
}
