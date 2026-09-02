import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "./client";
import type { AutoPickRequest, AutoPicksResult, ElectionChoice, ElectionChoicesResult, ElectionChoiceUpdate } from "./types";
import { useMe } from "./useMe";

/** A race counts as decided with at least one candidate pick or a measure
 * position — the one shared definition behind the pick-progress counters,
 * the district gate's decided-race safety valve, and the ballot cards'
 * "My pick" chips, on both platforms. */
export function isDecidedChoice(choice: ElectionChoice | undefined): boolean {
  return choice !== undefined && (choice.picks.length > 0 || choice.measure_position !== null);
}

// "My pick: Jane Doe" / "My picks: Jane Doe, John Roe" (multi-seat) /
// "My pick: Yes" on a measure. First person throughout, because these labels
// echo the controls that set them — MeasureChoiceButtons is headed "My
// pick:" and the candidate button reads "My pick". A pick whose candidate
// has since withdrawn gets flagged inline instead of vanishing.
export function formatChoiceLabel(choice: ElectionChoice): string | null {
  if (choice.measure_position !== null) {
    return `My pick: ${choice.measure_position === "yes" ? "Yes" : "No"}`;
  }
  if (choice.picks.length === 0) {
    return null;
  }
  const names = choice.picks
    .map((pick) => (pick.candidacy_status === "withdrawn" ? `${pick.display_name} (withdrew)` : pick.display_name))
    .join(", ");
  return `${choice.picks.length === 1 ? "My pick" : "My picks"}: ${names}`;
}

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
    // isLoading, not isPending: the query is disabled for anonymous
    // visitors, and a disabled query stays pending forever — isPending
    // would read as an eternal spinner for users who can't choose at all.
    isLoading: query.isLoading,
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

/**
 * Runs the auto-pick engine ("Pick for me") over one or more elections.
 * Shares the set-election-choice mutation key so every pick control on the
 * page disables while the engine writes, and invalidates the same choices
 * query (a dry_run's refetch is a harmless no-op — not worth branching over).
 */
export function useAutoPick() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationKey: ["set-election-choice"],
    mutationFn: (request: AutoPickRequest) =>
      apiRequest<AutoPicksResult>("/api/me/auto-picks", { method: "POST", body: request }),
    // onSettled, not onSuccess: the batch commits election by election, so a
    // failure partway through can leave real writes behind — the choices
    // cache must refetch even when the call errors.
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["me", "election-choices"] }),
  });
}
