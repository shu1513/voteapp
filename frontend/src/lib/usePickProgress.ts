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
  // Same key AND url as PicksPage's query, so a cold load of /me/picks is
  // ONE ballot request shared by header and page. Progress only counts
  // races, so the preview sort is irrelevant here — and the preview extra
  // is cheap (two batched queries server-side), which is why every page's
  // header riding it beats route-gating this hook. Deliberately NOT the
  // ["me", "ballot"] key: the saved ballot page owns that one with the
  // user's saved sort. staleTime keeps route changes from refetching.
  const ballot = useQuery({
    queryKey: ["me", "ballot", "preview"],
    queryFn: () =>
      apiRequest<BallotSummary>("/api/me/ballot?include=preview&sort=state_baseline&followed_first=false"),
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
 * The logged-out header's draft link (the guest counterpart of "My Picks"),
 * pointing at /draft. Null — no link at all — until the guest has looked at
 * a ballot or made a pick: a first-time visitor on the address search has no
 * draft to speak of, and a dead-end nav item there is noise. Null is also
 * what the SSR pass returns (server snapshot is an empty draft), so the
 * edge-cached anonymous document stays draft-free and identical for every
 * visitor. Once live, the label stays a plain "My Ballot Draft" until the
 * first pick — a "0/20" on arrival reads as homework, not collecting — then
 * counts up and finally takes the earned name, "My Election Picks ✓".
 */
export function useGuestDraftNav(): { to: string; label: string; complete: boolean } | null {
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
  // Ballot seen but nothing picked yet: plain label, no counter.
  if (draft.district_ids.length > 0) {
    return { to: "/draft", label: "My Ballot Draft", complete: false };
  }
  return null;
}
