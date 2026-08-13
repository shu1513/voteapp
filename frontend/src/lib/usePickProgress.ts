import { useQuery } from "@tanstack/react-query";
import { apiRequest, useElectionChoices, useMe } from "@voteapp/api-client";
import type { BallotSummary } from "@voteapp/api-client";
import { draftProgress, isDecidedChoice, useBallotDraft } from "./ballotDraft";
import { usLatestLocalDate } from "./usLatestLocalDate";

export type PickProgress = { picked: number; total: number; complete: boolean };

/**
 * The signed-in header's pick counter ("My Picks 4/13" → "My Picks ✓"):
 * progress over the nearest upcoming election day on the user's saved
 * ballot — the same denominator as that day's PickDateCard. Null hides the
 * counter (logged out, unverified, ballot not loaded, no upcoming races, or
 * choices still loading) and the nav shows plain "My Picks".
 */
export function useMyPicksProgress(): PickProgress | null {
  const { me } = useMe();
  const verified = me?.email_verified === true;
  // Same key and options as PicksPage's ballot query so the two share one
  // cache entry; staleTime keeps route changes from refetching every time.
  const ballot = useQuery({
    queryKey: ["me", "ballot"],
    queryFn: () => apiRequest<BallotSummary>("/api/me/ballot"),
    enabled: verified,
    retry: false,
    staleTime: 60_000,
  });
  const { choiceByElectionId } = useElectionChoices();
  if (!verified || !ballot.data || choiceByElectionId === undefined) {
    return null;
  }
  const today = usLatestLocalDate();
  const upcoming = ballot.data.elections.filter((election) => election.election_date >= today);
  if (upcoming.length === 0) {
    return null;
  }
  const date = upcoming.reduce(
    (min, election) => (election.election_date < min ? election.election_date : min),
    upcoming[0].election_date
  );
  const group = upcoming.filter((election) => election.election_date === date);
  const picked = group.filter((election) => isDecidedChoice(choiceByElectionId.get(election.id))).length;
  return { picked, total: group.length, complete: picked === group.length };
}

/**
 * The logged-out header's draft badge: label + destination for the guest's
 * ballot draft, or null when there is nothing to show (no draft target yet).
 * Complete drafts take the earned name — "My Election Picks ✓".
 */
export function useGuestDraftNav(): { to: string; label: string; complete: boolean } | null {
  const draft = useBallotDraft();
  const progress = draftProgress(draft);
  if (!progress || draft.district_ids.length === 0) {
    return null;
  }
  return {
    to: `/ballot?d=${encodeURIComponent(draft.district_ids.join(","))}`,
    label: progress.complete
      ? "My Election Picks ✓"
      : `My Ballot Draft ${progress.picked}/${progress.total}`,
    complete: progress.complete,
  };
}
