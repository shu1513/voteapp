import { formatElectionDate, isDecidedChoice, type ElectionChoice } from "@voteapp/api-client";
import { useEffect, useState } from "react";
import { Text, View } from "react-native";
import { hasDraftCompleteBeenSeen, markDraftCompleteSeen } from "../lib/draftCompleteSeen";

// Mobile port of the web's DraftMilestone (docs/plans/
// draft-completion-moment.md, section 2): the My Draft screen's finish
// line, above the date cards, once every race on the nearest upcoming
// election day has a pick — and shown ONCE per day per device (owner's
// rule: persistent = nag). Same counting rule as the card's "N of M races
// decided" line (isDecidedChoice) and the same one-line wording as the
// notice. The seen state is read once per day (AsyncStorage, async) and
// held, so the box stays for this visit even though it marks the day right
// away; the next mount finds the marker and shows nothing. Marking also
// covers the notice's scope, so the pick screens' one-time notice never
// fires for a day read about here. No sign-up link: mobile has no guest
// draft.

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

  // null = not read yet for this day; the box waits for a known answer.
  const [seenAtMount, setSeenAtMount] = useState<{ date: string; seen: boolean } | null>(null);
  useEffect(() => {
    let cancelled = false;
    void hasDraftCompleteBeenSeen(date, "milestone").then((seen) => {
      if (!cancelled) {
        setSeenAtMount({ date, seen });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [date]);

  const show = complete && seenAtMount !== null && seenAtMount.date === date && !seenAtMount.seen;
  useEffect(() => {
    if (show) {
      void markDraftCompleteSeen(date, "milestone");
      void markDraftCompleteSeen(date, "notice");
    }
  }, [show, date]);

  if (!show) {
    return null;
  }
  return (
    <View
      accessibilityLabel={`${formatElectionDate(date)} election draft milestone`}
      className="mt-4 rounded-xl border border-green-200 bg-green-50 px-4 py-3"
    >
      <Text className="text-sm font-semibold text-green-900">
        ✓ You have completed your {formatElectionDate(date)} election draft.
      </Text>
    </View>
  );
}
