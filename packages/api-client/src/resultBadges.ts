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
 * Guards, in order:
 * - Winner badges (Won / Advanced / In runoff) go only to winners whose
 *   candidate_id matches the displayed roster — an id the matcher failed to
 *   link, or one pointing at a candidate the payload filtered out
 *   (withdrawn), must not poison the derivation.
 * - Loser badges (Lost / Did not advance) require the winner set to be
 *   EXHAUSTIVE over the roster: every winner id-matched and present. A
 *   partial match cannot distinguish "won by a write-in" from "the matcher
 *   missed a roster candidate", and in the second case a loser badge would
 *   state something false about the actual winner.
 * - A runoff row names who continues and says nothing about who is out, so
 *   it never produces loser badges.
 * - Withdrawn candidates never get a loser badge.
 */
export function deriveCandidateResultBadges(
  results: readonly { outcome: string; winners: ElectionResultWinner[] }[],
  candidates: readonly { candidate_id: string; status: string }[]
): Map<string, CandidateResultBadge> {
  const badges = new Map<string, CandidateResultBadge>();
  const current = results.length > 0 ? results[0] : null;
  if (!current || !BADGED_OUTCOMES.has(current.outcome)) {
    return badges;
  }

  const rosterIds = new Set(candidates.map((candidate) => candidate.candidate_id));
  const matchedWinnerIds = new Set(
    current.winners
      .map((winner) => winner.candidate_id)
      .filter((id): id is string => typeof id === "string" && rosterIds.has(id))
  );
  if (matchedWinnerIds.size === 0) {
    return badges;
  }

  const winnerLabel =
    current.outcome === "won" ? "Won" : current.outcome === "advanced" ? "Advanced" : "In runoff";
  for (const id of matchedWinnerIds) {
    badges.set(id, { label: winnerLabel, kind: "winner" });
  }

  const exhaustive = current.winners.every(
    (winner) => typeof winner.candidate_id === "string" && rosterIds.has(winner.candidate_id)
  );
  const loserLabel =
    current.outcome === "won" ? "Lost" : current.outcome === "advanced" ? "Did not advance" : null;
  if (exhaustive && loserLabel) {
    for (const candidate of candidates) {
      if (!matchedWinnerIds.has(candidate.candidate_id) && candidate.status !== "withdrawn") {
        badges.set(candidate.candidate_id, { label: loserLabel, kind: "loser" });
      }
    }
  }
  return badges;
}
