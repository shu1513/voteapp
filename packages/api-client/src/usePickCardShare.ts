import { useMutation } from "@tanstack/react-query";
import { apiRequest } from "./client";
import type { PickCardShare } from "./types";

/**
 * Mints a public share link for one election day's pick card. Account-only
 * (the backend refuses guests and empty cards); the returned token becomes
 * the public web URL /picks/<token> on both platforms — mobile shares the
 * website link, there is no native card viewer.
 */
export function useMintPickCardShare() {
  return useMutation({
    mutationFn: (electionDate: string) =>
      apiRequest<{ share: PickCardShare }>("/api/me/pick-card-shares", {
        method: "POST",
        body: { election_date: electionDate },
      }),
  });
}
