import { ApiError, useElectionChoiceSaving, useSetElectionChoice } from "@voteapp/api-client";
import type { ElectionChoice } from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { Pressable, Text, View } from "react-native";

// "My choice" controls: the viewer's planned vote per election, ported from
// the web ElectionChoiceControls. Mobile difference: no guest branch — the
// guest ballot draft is not ported, so logged-out viewers get the
// LogInToPlanLine instead of controls (the screens gate on that), and every
// write goes to /api/me/election-choices via the shared mutation. Callers
// gate rendering on a loaded choices list (no-flash rule, like FollowButton)
// and on upcoming elections — the backend rejects writes to past ones.

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
    <Text accessibilityLiveRegion="polite" className="text-xs font-medium text-red-800">
      {saveErrorMessage(error)}
    </Text>
  );
}

/**
 * The logged-out stand-in for every pick control: one quiet line, no yellow,
 * no per-candidate buttons (plan: mobile guests see no pick controls).
 * Screens render it only where a control would otherwise sit — upcoming
 * races a guest is looking at.
 */
export function LogInToPlanLine() {
  const router = useRouter();
  return (
    <Text className="text-sm text-ink-soft">
      <Text className="font-medium underline" accessibilityRole="link" onPress={() => router.push("/auth/login")}>
        Log in
      </Text>{" "}
      to plan your ballot.
    </Text>
  );
}

type CandidatePickButtonProps = {
  electionId: string;
  candidateId: string;
  /** Carried in the accessible label so the screen's N pick buttons stay
   * distinguishable to screen readers. */
  candidateName: string;
  /** The viewer's current choice state for this election, if any. */
  choice: ElectionChoice | undefined;
  /** elections.seats_to_fill — null renders as a single seat. */
  seatsToFill: number | null;
  size?: "sm" | "md";
  /** Stretch the control to its container's width (the footer pick card). */
  fullWidth?: boolean;
};

/**
 * Toggle for "this candidate is my pick". Single-seat races behave like a
 * radio (the backend replaces the previous pick); multi-seat races act as
 * checkboxes capped at seats_to_fill, so the button disables once the cap
 * is reached and this candidate is not among the picks. Same color grammar
 * as the web: yellow only ever marks an UNDONE decision — picked is green,
 * and once the race's seats are all picked the other buttons demote to a
 * quiet outline.
 */
export function CandidatePickButton({
  electionId,
  candidateId,
  candidateName,
  choice,
  seatsToFill,
  size = "md",
  fullWidth = false,
}: CandidatePickButtonProps) {
  const setChoice = useSetElectionChoice();
  const saving = useElectionChoiceSaving();
  const picks = choice?.picks ?? [];
  const isPicked = picks.some((pick) => pick.candidate_id === candidateId);
  const seatCap = seatsToFill ?? 1;
  const atMultiSeatCap = seatCap > 1 && !isPicked && picks.length >= seatCap;
  const raceDecided = picks.length >= seatCap;
  const disabled = saving || atMultiSeatCap;
  // `saving` disables EVERY choice control while any one of them writes, so
  // a picked button must dim too, not only the unpicked ones.
  const sizeClass = size === "sm" ? "rounded-full px-3 py-1.5" : "rounded-full px-4 py-2.5";
  const base = `${sizeClass}${fullWidth ? " w-full" : ""}${disabled ? " opacity-50" : ""}`;
  const textBase = `text-center font-semibold ${size === "sm" ? "text-xs" : "text-sm"}`;
  const visibleLabel = setChoice.isPending ? "…" : isPicked ? "✓ My pick" : "Make my pick";

  return (
    <View className={fullWidth ? "w-full gap-1" : "items-end gap-1"}>
      <Pressable
        disabled={disabled}
        accessibilityRole="button"
        accessibilityState={{ selected: isPicked, disabled }}
        // Leading with the visible text keeps the spoken label a prefix of
        // the name; the candidate suffix keeps the screen's N pick buttons
        // apart. Same wording as the web control.
        accessibilityLabel={`${visibleLabel}: ${candidateName}`}
        onPress={() => setChoice.mutate({ election_id: electionId, candidate_id: candidateId, chosen: !isPicked })}
        className={
          isPicked
            ? `${base} bg-green-700 active:bg-green-800`
            : raceDecided
              ? `${base} border border-line bg-white active:border-green-700`
              : // pick yellow: the app's reserved primary-action color — the
                // unpicked state is the call to act, the picked state stays
                // green ("done"), so the two never compete.
                `${base} bg-pick active:bg-pick-hover`
        }
      >
        <Text className={isPicked ? `${textBase} text-white` : `${textBase} text-ink`}>{visibleLabel}</Text>
      </Pressable>
      {/* Visible, not a tooltip: a disabled control with a hidden reason
          reaches neither touch nor screen-reader users. Web shows this only
          on the standalone full-width button; on a phone every capped button
          needs it — there is no hover tooltip fallback at all. */}
      {atMultiSeatCap ? (
        <Text className="text-xs font-medium text-ink-soft">
          This election fills {seatCap} seats — remove a pick first
        </Text>
      ) : null}
      {/* Only this control's own failure: a shared banner would blame every
          button on the screen for one candidate's rejection. */}
      {setChoice.isError && !setChoice.isPending ? <SaveError error={setChoice.error} /> : null}
    </View>
  );
}

type CandidatePickRowProps = {
  electionId: string;
  candidateId: string;
  /** Shown in the row text so the action reads as a sentence. */
  candidateName: string;
  /** The election's official ballot title, e.g. "Commissioner of Agriculture". */
  raceName: string;
  /** Pre-formatted election date, e.g. "August 18, 2026". */
  dateLabel: string;
  /** The viewer's current choice state for this election, if any. */
  choice: ElectionChoice | undefined;
  /** elections.seats_to_fill — null renders as a single seat. */
  seatsToFill: number | null;
};

/**
 * Ballot-style pick toggle for the candidate screen: the whole row is the
 * button and its text reads as a sentence ("Make Jane Doe my pick for
 * Governor"), because a bare "Make my pick" button next to a race name reads
 * as two unrelated things. Rendered when the candidate is in SEVERAL
 * pickable races — each row names its race. Same toggle semantics and color
 * grammar as CandidatePickButton.
 */
export function CandidatePickRow({
  electionId,
  candidateId,
  candidateName,
  raceName,
  dateLabel,
  choice,
  seatsToFill,
}: CandidatePickRowProps) {
  const setChoice = useSetElectionChoice();
  const saving = useElectionChoiceSaving();
  const picks = choice?.picks ?? [];
  const isPicked = picks.some((pick) => pick.candidate_id === candidateId);
  const seatCap = seatsToFill ?? 1;
  const atMultiSeatCap = seatCap > 1 && !isPicked && picks.length >= seatCap;
  const raceDecided = picks.length >= seatCap;
  const disabled = saving || atMultiSeatCap;
  const base = `w-full rounded-xl border p-3${disabled ? " opacity-50" : ""}`;

  return (
    <View className="gap-1">
      <Pressable
        disabled={disabled}
        accessibilityRole="button"
        accessibilityState={{ selected: isPicked, disabled }}
        onPress={() => setChoice.mutate({ election_id: electionId, candidate_id: candidateId, chosen: !isPicked })}
        className={
          isPicked
            ? `${base} border-green-700 bg-green-50 active:bg-green-100`
            : raceDecided
              ? `${base} border-line bg-white active:border-green-700`
              : `${base} border-pick bg-pick active:bg-pick-hover`
        }
      >
        <Text className={isPicked ? "text-sm text-green-900" : "text-sm text-ink"}>
          {isPicked ? (
            <>
              ✓ <Text className="font-semibold">{candidateName}</Text> is my pick for {raceName} · {dateLabel}
            </>
          ) : (
            <>
              Make <Text className="font-semibold">{candidateName}</Text> my pick for {raceName} · {dateLabel}
            </>
          )}
        </Text>
      </Pressable>
      {atMultiSeatCap ? (
        <Text className="text-xs font-medium text-ink-soft">
          This election fills {seatCap} seats — remove a pick first
        </Text>
      ) : null}
      {setChoice.isError && !setChoice.isPending ? <SaveError error={setChoice.error} /> : null}
    </View>
  );
}

type MeasureChoiceButtonsProps = {
  electionId: string;
  choice: ElectionChoice | undefined;
};

/**
 * Yes/No planned-vote pair for a ballot measure — the measure screen's ONE
 * pick control, in the footer card. Tapping the active side clears the
 * position (sends null). Color grammar: while undecided BOTH sides wear the
 * reserved pick yellow (the screen is asking); once decided the picked side
 * keeps its semantic color (green Yes / red No) and the other demotes to a
 * quiet outline.
 */
export function MeasureChoiceButtons({ electionId, choice }: MeasureChoiceButtonsProps) {
  const setChoice = useSetElectionChoice();
  const saving = useElectionChoiceSaving();
  const position = choice?.measure_position ?? null;
  const base = `flex-1 rounded-full px-4 py-2.5${saving ? " opacity-50" : ""}`;
  const undecided = position === null;

  function setPosition(next: "yes" | "no" | null) {
    setChoice.mutate({ election_id: electionId, measure_position: next });
  }

  return (
    <View className="w-full">
      <View className="w-full flex-row items-center gap-2">
        <Text className="text-sm font-medium text-ink">My pick:</Text>
        <Pressable
          disabled={saving}
          accessibilityRole="button"
          accessibilityState={{ selected: position === "yes", disabled: saving }}
          accessibilityLabel={position === "yes" ? "Yes, picked" : "Yes"}
          onPress={() => setPosition(position === "yes" ? null : "yes")}
          className={
            position === "yes"
              ? `${base} bg-green-700 active:bg-green-800`
              : undecided
                ? `${base} bg-pick active:bg-pick-hover`
                : `${base} border border-line bg-white active:border-green-700`
          }
        >
          <Text
            className={`text-center text-sm font-semibold ${position === "yes" ? "text-white" : "text-ink"}`}
          >
            {position === "yes" ? "✓ Yes" : "Yes"}
          </Text>
        </Pressable>
        <Pressable
          disabled={saving}
          accessibilityRole="button"
          accessibilityState={{ selected: position === "no", disabled: saving }}
          accessibilityLabel={position === "no" ? "No, picked" : "No"}
          onPress={() => setPosition(position === "no" ? null : "no")}
          className={
            position === "no"
              ? `${base} bg-red-700 active:bg-red-800`
              : undecided
                ? `${base} bg-pick active:bg-pick-hover`
                : `${base} border border-line bg-white active:border-red-700`
          }
        >
          <Text className={`text-center text-sm font-semibold ${position === "no" ? "text-white" : "text-ink"}`}>
            {position === "no" ? "✓ No" : "No"}
          </Text>
        </Pressable>
      </View>
      {setChoice.isError && !setChoice.isPending ? (
        <View className="mt-1">
          <SaveError error={setChoice.error} />
        </View>
      ) : null}
    </View>
  );
}
