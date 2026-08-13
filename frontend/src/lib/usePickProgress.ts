import { useQuery } from "@tanstack/react-query";
import { apiRequest, useElectionChoices, useMe } from "@voteapp/api-client";
import type { BallotSummary } from "@voteapp/api-client";
import { draftPickCount, draftProgress, isDecidedChoice, useBallotDraft } from "./ballotDraft";
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
 * The logged-out header's draft link: always present (the guest counterpart
 * of "My Picks"), pointing at /draft. The label stays a plain
 * "My Ballot Draft" until the first pick — a "0/20" on arrival reads as
 * homework, not collecting — then counts up and finally takes the earned
 * name, "My Election Picks ✓". The plain label is also what the SSR pass
 * renders (server snapshot is an empty draft), so the edge-cached anonymous
 * document stays draft-free and identical for every visitor.
 */
export function useGuestDraftNav(): { to: string; label: string; complete: boolean } {
  const draft = useBallotDraft();
  const progress = draftProgress(draft);
  if (progress && progress.picked > 0) {
    return {
      to: "/draft",
      label: progress.complete
        ? "My Election Picks ✓"
        : `My Ballot Draft ${progress.picked}/${progress.total}`,
      complete: progress.complete,
    };
  }
  // Deep-link entry: picks made on an election or candidate page without
  // ever seeing /ballot have no race denominator, so count picks instead of
  // progress. /draft handles the missing district list with its own
  // address-search fallback.
  const pickCount = draftPickCount(draft);
  if (pickCount > 0) {
    return { to: "/draft", label: `My Ballot Draft (${pickCount})`, complete: false };
  }
  return { to: "/draft", label: "My Ballot Draft", complete: false };
}
