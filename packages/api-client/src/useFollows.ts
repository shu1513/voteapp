import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "./client";
import type { CandidateFollowsResult, CandidateFollowUpdate } from "./types";
import { useMe } from "./useMe";

/**
 * The follows list for the session holder. Only fetched for verified users —
 * the endpoint is verified-email-gated, and anonymous/unverified visitors
 * never see follow controls.
 */
export function useFollows() {
  const { me } = useMe();
  const query = useQuery({
    queryKey: ["me", "follows"],
    queryFn: () => apiRequest<CandidateFollowsResult>("/api/me/candidate-follows"),
    enabled: me?.email_verified === true,
    staleTime: 60_000,
  });
  return {
    follows: query.data?.follows,
    isLoading: query.isPending,
    isError: query.isError,
    canFollow: me?.email_verified === true,
  };
}

/**
 * Cross-mount in-flight guard for follow writes: component-local isPending
 * resets when a page unmounts mid-save, but the mutation cache does not.
 * Controls disable on this so a remount cannot race the older PUT.
 */
export function useFollowSaving(): boolean {
  return useIsMutating({ mutationKey: ["set-follow"] }) > 0;
}

export function useSetFollow() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationKey: ["set-follow"],
    mutationFn: (update: CandidateFollowUpdate) =>
      apiRequest<{ follow: unknown }>("/api/me/candidate-follows", { method: "PUT", body: update }),
    onSuccess: () =>
      // The follows list and the saved ballot's followed-first ordering can
      // change. (Candidate/election pages derive follow state from the
      // follows list — the subject itself comes from the route loader.)
      // Returned (not fire-and-forget) so the mutation stays pending until
      // the refetched server truth is in the cache: useFollowSaving keeps
      // controls disabled through the refetch, and callers can safely clear
      // optimistic overlays in onSettled without falling back to stale props.
      Promise.all([
        queryClient.invalidateQueries({ queryKey: ["me", "follows"] }),
        queryClient.invalidateQueries({ queryKey: ["me", "ballot"] }),
      ]).then(() => undefined),
  });
}
