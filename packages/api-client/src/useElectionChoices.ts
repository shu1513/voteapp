import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "./client";
import type { ElectionChoice, ElectionChoicesResult, ElectionChoiceUpdate } from "./types";
import { useMe } from "./useMe";

/**
 * The session holder's planned votes ("my choice"), keyed by election.
 * Unlike follows this endpoint is not verification-gated: any logged-in
 * user can plan their ballot.
 */
export function useElectionChoices() {
  const { me } = useMe();
  const query = useQuery({
    queryKey: ["me", "election-choices"],
    queryFn: () => apiRequest<ElectionChoicesResult>("/api/me/election-choices"),
    enabled: me != null,
    staleTime: 60_000,
  });
  const byElectionId = new Map<string, ElectionChoice>();
  for (const choice of query.data?.choices ?? []) {
    byElectionId.set(choice.election_id, choice);
  }
  return {
    choices: query.data?.choices,
    choiceByElectionId: query.data ? byElectionId : undefined,
    isLoading: query.isPending,
    isError: query.isError,
    canChoose: me != null,
  };
}

/**
 * Cross-mount in-flight guard for choice writes, mirroring useFollowSaving:
 * component-local isPending resets when a page unmounts mid-save, but the
 * mutation cache does not.
 */
export function useElectionChoiceSaving(): boolean {
  return useIsMutating({ mutationKey: ["set-election-choice"] }) > 0;
}

export function useSetElectionChoice() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationKey: ["set-election-choice"],
    mutationFn: (update: ElectionChoiceUpdate) =>
      apiRequest<{ choice: ElectionChoice }>("/api/me/election-choices", { method: "PUT", body: update }),
    // Returned (not fire-and-forget) so the mutation stays pending until the
    // refetched server truth is in the cache — same contract as useSetFollow.
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["me", "election-choices"] }),
  });
}
