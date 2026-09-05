import { formatElectionDate, isDecidedChoice, type ElectionChoice } from "@voteapp/api-client";
import { useEffect } from "react";
import { Text, View } from "react-native";
import { markDraftCompleteSeen } from "../lib/draftCompleteSeen";

// Mobile port of the web's DraftMilestone (docs/plans/
// draft-completion-moment.md, section 2): the My Draft screen's persistent
// finish line, above the date cards, once every race on the nearest
// upcoming election day has a pick. Same counting rule as the card's
// "N of M races decided" line (isDecidedChoice) and the same claim — "picks
// added for every race", never "complete". Rendering it marks the day as
// seen so the pick screens' one-time notice never fires for a day the user
// has already read about here. No sign-up link: mobile has no guest draft.

export function DraftMilestone({
  date,
  elections,
  choiceByElectionId,
}: {
  /** The nearest upcoming election day — the first carded date on or after
   * today, not the first card (just-finished days stay carded for a while). */
  date: string;
  elections: { id: string }[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
}) {
  const total = elections.length;
  const picked = elections.filter((election) => isDecidedChoice(choiceByElectionId?.get(election.id))).length;
  const complete = total > 0 && picked === total;

  useEffect(() => {
    if (complete) {
      void markDraftCompleteSeen(date);
    }
  }, [complete, date]);

  if (!complete) {
    return null;
  }
  return (
    <View
      accessibilityLabel={`${formatElectionDate(date)} draft milestone`}
      className="mt-4 rounded-xl border border-green-200 bg-green-50 px-4 py-3"
    >
      <Text className="text-sm font-semibold text-green-900">
        ✓ Picks added for every race in your {formatElectionDate(date)} draft.
      </Text>
      <Text className="mt-0.5 text-xs text-green-900">
        {picked} of {total} race{total === 1 ? "" : "s"} decided. Review your picks and make any changes.
      </Text>
    </View>
  );
}
