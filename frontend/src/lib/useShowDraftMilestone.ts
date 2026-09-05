import { useEffect, useState } from "react";
import { hasDraftCompleteBeenSeen, markDraftCompleteSeen } from "./draftCompleteSeen";

/**
 * Whether the draft pages' finish-line box (DraftMilestone, and whatever
 * rides with it) shows on THIS visit: once per election day per browser,
 * never persistently — the owner's rule, for guests and members alike. The
 * seen state is snapshotted the first time a day is tracked, so the box
 * stays for the whole visit even though it marks the day as seen right
 * away; the next mount finds the marker and shows nothing. Marking also
 * covers the header notice's scope: a day read about here must not fire
 * the notice afterwards. Call above any early return — it is a hook.
 */
export function useShowDraftMilestone(date: string | undefined, complete: boolean): boolean {
  const [snapshot, setSnapshot] = useState<{ date: string; seen: boolean } | null>(null);
  // Render-time adjustment (TermsRenewalGate's pattern), not an effect, so
  // the first paint already knows. SSR has no storage: treat as seen there
  // so the server never paints a box the client would drop on hydration.
  if (date !== undefined && snapshot?.date !== date) {
    setSnapshot({ date, seen: typeof window === "undefined" || hasDraftCompleteBeenSeen(date, "milestone") });
  }
  const show = date !== undefined && complete && snapshot !== null && snapshot.date === date && !snapshot.seen;
  useEffect(() => {
    if (show && date !== undefined) {
      markDraftCompleteSeen(date, "milestone");
      markDraftCompleteSeen(date, "notice");
    }
  }, [show, date]);
  return show;
}
