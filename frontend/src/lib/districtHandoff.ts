import { useEffect, useSyncExternalStore } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest, useMe } from "@voteapp/api-client";
import { clearPendingDistrictIds, readPendingDistrictIds, subscribePendingDistrictIds } from "./pendingDistricts";
import { track } from "./usage";

/**
 * Anonymous-to-account district handoff, app-wide. The queued ids from the
 * last anonymous address resolve (pendingDistricts.ts) POST to
 * /api/me/districts/initialize the moment /api/me reports a verified
 * session — on whatever page the user is on. Auth prompts carry ?next= back
 * to the originating page, so a Google-signup user (verified instantly)
 * lands on an election page, not /me/ballot; without this the account has
 * no districts, pick controls hide and the address nudge shows until they
 * happen to visit My Ballot.
 *
 * One module-level store, two readers: `useDistrictHandoffRunner` (mounted
 * once in App) starts the request; `useDistrictHandoffStatus` lets
 * SavedBallotPage withhold its ballot query while ids are queued and offer
 * a retry after a recoverable failure.
 *
 * Status is derived, not stored: "pending" while ids sit in the queue,
 * "done" once the queue is empty, "failed" after a recoverable failure
 * (the ids stay queued for `retryDistrictHandoff`). The server snapshot is
 * "done" so the hydration render matches the SSR HTML; the client snapshot
 * takes over one paint later.
 */
export type DistrictHandoffStatus = "pending" | "done" | "failed";

let failed = false;
let inFlight = false;
const listeners = new Set<() => void>();

function notify(): void {
  for (const listener of listeners) {
    listener();
  }
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  const unsubscribeQueue = subscribePendingDistrictIds(listener);
  return () => {
    listeners.delete(listener);
    unsubscribeQueue();
  };
}

function getStatus(): DistrictHandoffStatus {
  if (failed) {
    return "failed";
  }
  return readPendingDistrictIds().length > 0 ? "pending" : "done";
}

const getServerStatus = (): DistrictHandoffStatus => "done";

export function useDistrictHandoffStatus(): DistrictHandoffStatus {
  return useSyncExternalStore(subscribe, getStatus, getServerStatus);
}

/** Re-arms the handoff after a failure; the App runner picks it up. */
export function retryDistrictHandoff(): void {
  failed = false;
  notify();
}

// Only a definitive rejection of the payload itself (400: malformed or
// unknown district ids) resolves to "the account's ballot is the source of
// truth". Everything else — 401/403 (session or verification state changed
// server-side), 429, network and server failures — is recoverable, so the
// queued ids stay put and the ballot page surfaces an explicit retry
// instead of dropping the user onto the empty set-address form with their
// search lost. Each attempt records exactly one handoff_result event.
async function runDistrictHandoff(queryClient: QueryClient): Promise<void> {
  inFlight = true;
  const districtIds = readPendingDistrictIds();
  try {
    await apiRequest("/api/me/districts/initialize", {
      method: "POST",
      body: { district_ids: districtIds },
    });
    track("handoff_result", { outcome: "done" });
    inFlight = false;
    clearPendingDistrictIds();
    void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
    // The pick gate's district set (useMyDistricts) was just initialized —
    // refetch it or stale ids keep gating pick buttons. Not needed in the
    // 400 branch below: a rejected payload changes nothing.
    void queryClient.invalidateQueries({ queryKey: ["me", "districts"] });
  } catch (error) {
    inFlight = false;
    if (error instanceof ApiError && error.status === 400) {
      track("handoff_result", { outcome: "rejected" });
      clearPendingDistrictIds();
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
    } else {
      track("handoff_result", { outcome: "failed" });
      failed = true;
      notify();
    }
  }
}

/**
 * Mount once (App). Fires the handoff whenever ids are queued and the
 * session is verified; a save that lands while already verified (guest
 * search → login → still on the page) triggers it too, via the queue's
 * subscription. The in-flight guard keeps re-renders from double-posting.
 */
export function useDistrictHandoffRunner(): void {
  const { me } = useMe();
  const queryClient = useQueryClient();
  const status = useDistrictHandoffStatus();
  const verified = me?.email_verified === true;
  useEffect(() => {
    if (!verified || status !== "pending" || inFlight) {
      return;
    }
    void runDistrictHandoff(queryClient);
  }, [verified, status, queryClient]);
}

/** Test seam: wipes module state between tests. */
export function resetDistrictHandoffForTests(): void {
  failed = false;
  inFlight = false;
}
