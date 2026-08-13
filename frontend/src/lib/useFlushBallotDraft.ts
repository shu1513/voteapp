import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useMe } from "@voteapp/api-client";
import { flushBallotDraftToAccount, hasDraftPicks, readBallotDraft } from "./ballotDraft";

/**
 * Guest-to-account pick handoff, mounted once in App: the moment /api/me
 * reports a session (login OR registration — the choices endpoint is not
 * verification-gated, unlike the pendingDistricts handoff), any local ballot
 * draft replays into the account and clears. The same cart-merge rule as
 * every shopping site: whoever logs into this browser inherits the
 * anonymous cart built in it. On success the choices query refetches so
 * chips and counters flip from draft to account without a reload; on a
 * transport failure the draft stays put for a retry on the next session.
 */
export function useFlushBallotDraft(): void {
  const { me } = useMe();
  const queryClient = useQueryClient();
  const inFlight = useRef(false);
  useEffect(() => {
    if (me == null || inFlight.current || !hasDraftPicks(readBallotDraft())) {
      return;
    }
    inFlight.current = true;
    flushBallotDraftToAccount()
      .then(() => queryClient.invalidateQueries({ queryKey: ["me", "election-choices"] }))
      .catch(() => {
        // Draft kept — flushBallotDraftToAccount only clears after a full pass.
      })
      .finally(() => {
        inFlight.current = false;
      });
  }, [me, queryClient]);
}
