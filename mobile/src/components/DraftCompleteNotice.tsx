import { formatElectionDate, useMe } from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { useEffect, useRef, useState } from "react";
import { AccessibilityInfo, Pressable, Text, View } from "react-native";
import { hasDraftCompleteBeenSeen, markDraftCompleteSeen } from "../lib/draftCompleteSeen";
import { useMyPicksProgress } from "../lib/useMyPicksProgress";

// Mobile port of the web's DraftCompleteNotice (docs/plans/
// draft-completion-moment.md): one notice, once per election day per
// device, when every race on the nearest upcoming day has a pick. Driven by
// the same progress value the saved-ballot header counter reads — a known
// incomplete → known complete for the SAME tracked ballot (identity + day +
// race list); null is "unknown", never "incomplete", so a returning user
// whose first resolved value is already complete gets nothing.
//
// Mounted in flow at the top of the two screens where picks are made (the
// election and candidate screens), above their ScrollView and never over
// their pick footer — the same "root View + siblings, no absolute overlay"
// rule those screens follow. Per-screen instances, not a root-layout
// overlay: RN has no shared header row to ride, and the notice belongs on
// the screen where the last pick landed. Leaving that screen retires it;
// the My Draft screen's milestone (DraftMilestone) is the persistent one.
//
// Wording claims only what the counting rule establishes ("picks added for
// every race", never "complete"). Status message, not a modal: polite live
// region on Android, an explicit VoiceOver announcement on iOS, focus and
// scroll position untouched.

export function DraftCompleteNotice() {
  const { me } = useMe();
  const router = useRouter();
  const progress = useMyPicksProgress();

  const identity = me?.email ?? null;
  const trackedDate = identity !== null && progress !== null ? progress.election_date : null;
  const trackedKey =
    progress !== null && trackedDate !== null
      ? `${identity}|${trackedDate}|${[...progress.election_ids].sort().join(",")}`
      : null;
  const complete = progress?.complete ?? null;
  const total = progress?.total ?? 0;

  const baseline = useRef<{ key: string; complete: boolean } | null>(null);
  // The fired notice remembers the ballot it was about; it renders only
  // while that ballot is still the tracked one AND still complete, so an
  // unpick, a ballot change, or progress going unknown hides it at render
  // time — no setState inside the effect for the stale cases.
  const [shown, setShown] = useState<{ key: string; date: string; total: number } | null>(null);
  const visible = shown !== null && shown.key === trackedKey && complete === true ? shown : null;

  useEffect(() => {
    if (trackedKey === null || trackedDate === null || complete === null) {
      // Nothing confirms progress any more: break the chain.
      baseline.current = null;
      return;
    }
    const previous = baseline.current;
    baseline.current = { key: trackedKey, complete };
    if (previous === null || previous.key !== trackedKey || previous.complete || !complete) {
      return;
    }
    // Known incomplete → known complete for the same ballot. The seen check
    // is async (AsyncStorage); `cancelled` guards against an unpick or an
    // unmount landing before it resolves.
    let cancelled = false;
    void hasDraftCompleteBeenSeen(trackedDate).then((seen) => {
      if (cancelled || seen) {
        return;
      }
      void markDraftCompleteSeen(trackedDate);
      setShown({ key: trackedKey, date: trackedDate, total });
    });
    return () => {
      cancelled = true;
    };
  }, [trackedKey, trackedDate, complete, total]);

  // accessibilityLiveRegion below is Android-only; VoiceOver needs an
  // explicit announcement (same pattern as TermsRenewalGate's error line).
  const message = visible
    ? `Picks added for every race in your ${formatElectionDate(visible.date)} draft. ${visible.total} of ${visible.total} race${visible.total === 1 ? "" : "s"} decided. Review your picks and make any changes.`
    : null;
  useEffect(() => {
    if (message !== null) {
      AccessibilityInfo.announceForAccessibility(message);
    }
  }, [message]);

  if (visible === null || message === null) {
    return null;
  }
  return (
    <View accessibilityLiveRegion="polite" className="border-b border-green-200 bg-green-50 px-4 py-2">
      <Text className="text-sm text-green-900">
        <Text className="font-semibold">
          Picks added for every race in your {formatElectionDate(visible.date)} draft.
        </Text>{" "}
        {visible.total} of {visible.total} race{visible.total === 1 ? "" : "s"} decided. Review your picks and make
        any changes.
      </Text>
      <View className="mt-1 flex-row items-center justify-between gap-3">
        <Text
          accessibilityRole="link"
          onPress={() => {
            setShown(null);
            router.push("/my-draft");
          }}
          className="text-sm font-semibold text-green-800 underline"
        >
          Review my picks
        </Text>
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Dismiss"
          hitSlop={8}
          onPress={() => setShown(null)}
          className="rounded px-2 active:bg-green-100"
        >
          <Text className="text-lg leading-none text-green-900">×</Text>
        </Pressable>
      </View>
    </View>
  );
}
