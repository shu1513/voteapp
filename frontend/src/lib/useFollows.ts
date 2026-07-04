import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { CandidateFollowsResult, CandidateFollowUpdate } from "../api/types";
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

export function useSetFollow() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (update: CandidateFollowUpdate) =>
      apiRequest<{ follow: unknown }>("/api/me/candidate-follows", { method: "PUT", body: update }),
    onSuccess: (_data, update) => {
      // The follows list, the candidate detail's is_following, and the saved
      // ballot's followed-first ordering can all change.
      void queryClient.invalidateQueries({ queryKey: ["me", "follows"] });
      void queryClient.invalidateQueries({ queryKey: ["candidate", update.candidate_id] });
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
    },
  });
}
