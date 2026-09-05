import { useEffect } from "react";
import { Link } from "react-router";
import { formatElectionDate, isDecidedChoice } from "@voteapp/api-client";
import type { ElectionChoice } from "@voteapp/api-client";
import { allRacesDecided } from "../lib/ballotDraft";
import { markDraftCompleteSeen } from "../lib/draftCompleteSeen";

// The draft pages' finish line (docs/plans/draft-completion-moment.md,
// section 2): a persistent milestone above the List / Ballot preview toggle
// once every race on the nearest upcoming election day has a pick. The
// header notice (DraftCompleteNotice) is the one-time announcement; this is
// where it points, so it stays as long as the picks do. Same counting rule
// as the date card's "N of M races decided" line (isDecidedChoice) and the
// same claim — "picks added for every race", never "complete": one pick
// decides a multi-seat race, a withdrawn pick still counts, and a partial
// (ZIP/city) ballot reaches 100% too. Rendering it marks the day as seen
// so the header notice never fires for a day the user has already read
// about here.

export function DraftMilestone({
  date,
  elections,
  choiceByElectionId,
  signup,
}: {
  /** The nearest upcoming election day (callers pick it: /draft lists
   * upcoming days only, /me/picks keeps just-finished days too). */
  date: string;
  elections: { id: string }[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
  /** Guest page: the milestone carries the sign-up link (and the "lives
   * only on this device" hint) because "save this" is the next step for a
   * finished draft. The page hides its own bottom CTA while this renders —
   * one button per page, never two identical ones. */
  signup: boolean;
}) {
  const total = elections.length;
  const picked = elections.filter((election) => isDecidedChoice(choiceByElectionId?.get(election.id))).length;
  const complete = allRacesDecided(elections, choiceByElectionId);

  useEffect(() => {
    if (complete) {
      markDraftCompleteSeen(date);
    }
  }, [complete, date]);

  if (!complete) {
    return null;
  }
  return (
    <section
      aria-label={`${formatElectionDate(date)} draft milestone`}
      className="mt-4 rounded-xl border border-green-200 bg-green-50 px-4 py-3 text-sm text-green-900"
    >
      <p className="font-semibold">
        <span aria-hidden="true">✓ </span>
        Picks added for every race in your {formatElectionDate(date)} draft.
      </p>
      <p className="mt-0.5 text-xs">
        {picked} of {total} race{total === 1 ? "" : "s"} decided. Review your picks and make any changes.
      </p>
      {signup ? (
        <p className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1">
          <Link
            to={`/register?next=${encodeURIComponent("/draft")}`}
            className="inline-block rounded-lg bg-rausch px-3 py-1.5 font-semibold text-white transition hover:bg-rausch-dark"
          >
            Sign up free to save your picks
          </Link>
          <span className="text-xs">Your draft lives only on this device until you sign up.</span>
        </p>
      ) : null}
    </section>
  );
}
