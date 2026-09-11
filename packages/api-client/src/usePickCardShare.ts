import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "./client";
import type { PickCardShare } from "./types";
import { useMe } from "./useMe";

const PICK_CARD_SHARES_KEY = ["me", "pick-card-shares"] as const;

/**
 * Every live share link the session holder has minted. The picks page reads
 * this to offer "Stop sharing" on a date whose link already exists — after
 * a reload, or one minted on another device — not only on links minted in
 * the current session.
 */
export function useMyPickCardShares() {
  const { me } = useMe();
  const query = useQuery({
    queryKey: PICK_CARD_SHARES_KEY,
    queryFn: () => apiRequest<{ shares: PickCardShare[] }>("/api/me/pick-card-shares"),
    enabled: me != null,
    staleTime: 60_000,
  });
  return {
    shares: query.data?.shares,
    // isLoading, not isPending: disabled for anonymous visitors, and a
    // disabled query stays pending forever.
    isLoading: query.isLoading,
    isError: query.isError,
  };
}

/**
 * Mints a public share link for one election day's pick card. Account-only
 * (the backend refuses guests and empty cards); the returned token becomes
 * the public web URL /picks/<token> on both platforms — mobile shares the
 * website link, there is no native card viewer.
 */
export function useMintPickCardShare() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (electionDate: string) =>
      apiRequest<{ share: PickCardShare }>("/api/me/pick-card-shares", {
        method: "POST",
        body: { election_date: electionDate },
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: PICK_CARD_SHARES_KEY }),
  });
}

/**
 * Revokes one date's share link: the public URL stops resolving from the
 * next request on, and a later Share mints a fresh token. Idempotent on the
 * server (a missing link reports deleted=false), so a double click cannot
 * surface an error for an outcome that already holds.
 */
export function useRevokePickCardShare() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (electionDate: string) =>
      apiRequest<{ deleted: boolean }>(
        `/api/me/pick-card-shares?election_date=${encodeURIComponent(electionDate)}`,
        { method: "DELETE" }
      ),
    onSettled: () => queryClient.invalidateQueries({ queryKey: PICK_CARD_SHARES_KEY }),
  });
}
