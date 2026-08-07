import type { ElectionResultWinner } from "./types";

export type CandidateResultBadge = {
  label: string;
  kind: "winner" | "loser";
};

// Only these outcomes name a winner set worth badging. too_close/unknown rows
// may carry winners in the stored payload (a recorded leader), but displaying
// them as winners would call a race that is not decided.
const BADGED_OUTCOMES = new Set(["won", "advanced", "runoff"]);

/**
 * Per-candidate result badges for an election's roster, from the most
 * authoritative result row (results[0] — the API sorts certified before
 * election_night, then freshest).
 *
 * Winner badges (Won / Advanced / In runoff) go only to winners whose
 * candidate_id matches the displayed roster — an id the matcher failed to
 * link, or one pointing at a candidate the payload filtered out (withdrawn),
 * must not poison the derivation.
 *
 * Loser badges are the dangerous half: the contract does not validate that
 * the winner list is COMPLETE, so "everyone not listed lost" can be false —
 * a researcher can omit a winner, and an election-night row naturally
 * captures partial calls (one party's race called while the other is still
 * counting). Every winner being id-matched is therefore necessary but not
 * sufficient; each outcome adds its own completeness signal:
 * - won: the race is over only if the listed winners actually fill the
 *   seats, so winners must number at least seats_to_fill (null = 1, same
 *   reading as the rest of the display layer). A multi-seat row listing
 *   fewer winners than seats marks nobody as having lost.
 * - advanced: the expected advancer count is unknowable client-side
 *   (top-two takes 2 per race, partisan primaries take one per party), so
 *   the gate is per party label: a candidate is "Did not advance" only when
 *   a winner carries their own party label — that party's contest is the
 *   one their loss can be read from. A partial call leaves the uncalled
 *   party's candidates unmarked. (Residual: a same-label partial call, e.g.
 *   top-two advancing two candidates of one party with only one recorded,
 *   still over-marks — the label carries no finer signal to gate on.)
 * - runoff: names who continues and says nothing about who is out — never
 *   produces loser badges.
 * Withdrawn candidates never get a loser badge.
 */
export function deriveCandidateResultBadges(
  results: readonly { outcome: string; winners: ElectionResultWinner[] }[],
  candidates: readonly { candidate_id: string; status: string; party: string }[],
  seatsToFill: number | null
): Map<string, CandidateResultBadge> {
  const badges = new Map<string, CandidateResultBadge>();
  const current = results.length > 0 ? results[0] : null;
  if (!current || !BADGED_OUTCOMES.has(current.outcome)) {
    return badges;
  }

  const rosterById = new Map(candidates.map((candidate) => [candidate.candidate_id, candidate]));
  const matchedWinnerIds = new Set(
    current.winners
      .map((winner) => winner.candidate_id)
      .filter((id): id is string => typeof id === "string" && rosterById.has(id))
  );
  if (matchedWinnerIds.size === 0) {
    return badges;
  }

  const winnerLabel =
    current.outcome === "won" ? "Won" : current.outcome === "advanced" ? "Advanced" : "In runoff";
  for (const id of matchedWinnerIds) {
    badges.set(id, { label: winnerLabel, kind: "winner" });
  }

  const allWinnersMatched = current.winners.every(
    (winner) => typeof winner.candidate_id === "string" && rosterById.has(winner.candidate_id)
  );
  if (!allWinnersMatched) {
    return badges;
  }

  const partyKey = (party: string) => party.trim().toLowerCase();
  if (current.outcome === "won" && matchedWinnerIds.size >= (seatsToFill ?? 1)) {
    for (const candidate of candidates) {
      if (!matchedWinnerIds.has(candidate.candidate_id) && candidate.status !== "withdrawn") {
        badges.set(candidate.candidate_id, { label: "Lost", kind: "loser" });
      }
    }
  } else if (current.outcome === "advanced") {
    const calledParties = new Set(
      [...matchedWinnerIds].map((id) => partyKey(rosterById.get(id)?.party ?? ""))
    );
    for (const candidate of candidates) {
      if (
        !matchedWinnerIds.has(candidate.candidate_id) &&
        candidate.status !== "withdrawn" &&
        calledParties.has(partyKey(candidate.party))
      ) {
        badges.set(candidate.candidate_id, { label: "Did not advance", kind: "loser" });
      }
    }
  }
  return badges;
}
