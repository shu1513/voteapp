import { ApiError, useElectionChoiceSaving, useSetElectionChoice } from "@voteapp/api-client";
import type { ElectionChoice } from "@voteapp/api-client";

// "My choice" controls: a logged-in user's planned vote per election.
// Callers gate rendering on useElectionChoices().canChoose AND a loaded
// choices list (same no-flash rule as FollowButton), and only render for
// upcoming elections — the backend rejects writes to past ones.

// Backend rejections carry human-readable messages ("This election fills 2
// seats; remove a pick before adding another"); transport failures don't
// ("Failed to fetch"), so those get generic copy instead.
function saveErrorMessage(error: unknown): string {
  return error instanceof ApiError ? error.message : "Couldn't save — check your connection and try again.";
}

function SaveError({ error }: { error: unknown }) {
  if (!error) {
    return null;
  }
  return (
    <span role="alert" className="text-xs font-medium text-red-800">
      {saveErrorMessage(error)}
    </span>
  );
}

type CandidatePickButtonProps = {
  electionId: string;
  candidateId: string;
  /** The viewer's current choice state for this election, if any. */
  choice: ElectionChoice | undefined;
  /** elections.seats_to_fill — null renders as a single seat. */
  seatsToFill: number | null;
  size?: "sm" | "md";
};

/**
 * Toggle for "this candidate is my pick". Single-seat races behave like a
 * radio (the backend replaces the previous pick); multi-seat races act as
 * checkboxes capped at seats_to_fill, so the button disables once the cap
 * is reached and this candidate is not among the picks.
 */
export function CandidatePickButton({
  electionId,
  candidateId,
  choice,
  seatsToFill,
  size = "md",
}: CandidatePickButtonProps) {
  const setChoice = useSetElectionChoice();
  const saving = useElectionChoiceSaving();
  const picks = choice?.picks ?? [];
  const isPicked = picks.some((pick) => pick.candidate_id === candidateId);
  const seatCap = seatsToFill ?? 1;
  const atMultiSeatCap = seatCap > 1 && !isPicked && picks.length >= seatCap;
  const base =
    size === "sm"
      ? "rounded-lg px-3 py-1 text-xs font-semibold transition"
      : "rounded-lg px-4 py-2 text-sm font-semibold transition";

  return (
    <span className="inline-flex flex-col items-end gap-1">
      <button
        type="button"
        disabled={saving || atMultiSeatCap}
        aria-pressed={isPicked}
        title={atMultiSeatCap ? `This election fills ${seatCap} seats — remove a pick first` : undefined}
        onClick={() =>
          setChoice.mutate({ election_id: electionId, candidate_id: candidateId, chosen: !isPicked })
        }
        className={
          isPicked
            ? `${base} bg-green-700 text-white hover:bg-green-800`
            : `${base} border border-line bg-white text-ink hover:border-green-700 disabled:opacity-50`
        }
      >
        {setChoice.isPending ? "…" : isPicked ? "✓ My pick" : "Make my pick"}
      </button>
      {/* Only this control's own failure: a shared banner would blame every
          button on the page for one candidate's rejection. */}
      {setChoice.isError && !setChoice.isPending ? <SaveError error={setChoice.error} /> : null}
    </span>
  );
}

type MeasureChoiceButtonsProps = {
  electionId: string;
  choice: ElectionChoice | undefined;
};

/**
 * Yes/No planned-vote pair for a ballot measure. Clicking the active side
 * clears the position (sends null).
 */
export function MeasureChoiceButtons({ electionId, choice }: MeasureChoiceButtonsProps) {
  const setChoice = useSetElectionChoice();
  const saving = useElectionChoiceSaving();
  const position = choice?.measure_position ?? null;
  const base = "rounded-lg px-4 py-2 text-sm font-semibold transition disabled:opacity-50";

  return (
    <div className="flex flex-wrap items-center gap-2">
      <span className="text-sm font-medium text-ink-soft">My vote:</span>
      <button
        type="button"
        disabled={saving}
        aria-pressed={position === "yes"}
        onClick={() =>
          setChoice.mutate({ election_id: electionId, measure_position: position === "yes" ? null : "yes" })
        }
        className={
          position === "yes"
            ? `${base} bg-green-700 text-white hover:bg-green-800`
            : `${base} border border-line bg-white text-ink hover:border-green-700`
        }
      >
        {position === "yes" ? "✓ Yes" : "Yes"}
      </button>
      <button
        type="button"
        disabled={saving}
        aria-pressed={position === "no"}
        onClick={() =>
          setChoice.mutate({ election_id: electionId, measure_position: position === "no" ? null : "no" })
        }
        className={
          position === "no"
            ? `${base} bg-red-700 text-white hover:bg-red-800`
            : `${base} border border-line bg-white text-ink hover:border-red-700`
        }
      >
        {position === "no" ? "✓ No" : "No"}
      </button>
      {setChoice.isError && !setChoice.isPending ? <SaveError error={setChoice.error} /> : null}
    </div>
  );
}
