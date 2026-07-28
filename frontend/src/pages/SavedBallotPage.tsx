import { useEffect, useRef, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest } from "@voteapp/api-client";
import { BALLOT_SORTS, type BallotPreferences, type BallotSummary } from "@voteapp/api-client";
import {
  AddressSavedNotice,
  SavedAddressForm,
  type AddressSavedLocationState,
  type AddressSavedNoticeData,
} from "../components/SavedAddressForm";
import { ElectionList } from "../components/ElectionCard";
import { useMyResearchAreas } from "@voteapp/api-client";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { useMe } from "@voteapp/api-client";
import { clearPendingDistrictIds, readPendingDistrictIds } from "../lib/pendingDistricts";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { useDocumentTitle } from "../lib/useDocumentTitle";

type SavedBallot = BallotSummary & { matched_address?: string };

// Persisted ordering controls: unlike the anonymous ballot's URL params,
// these save to the account and apply to every future visit (and to the
// digest-adjacent "followed first" ordering).
function BallotPreferenceControls() {
  const queryClient = useQueryClient();
  // Optimistic overlay: consecutive changes must merge from the latest view,
  // not from a stale cache snapshot — the PUT saves the FULL object, so a
  // stale spread would revert the previous change.
  const [pending, setPending] = useState<BallotPreferences | null>(null);
  const prefs = useQuery({
    queryKey: ["me", "ballot-preferences"],
    queryFn: () => apiRequest<BallotPreferences>("/api/me/ballot-preferences"),
    staleTime: 60_000,
  });

  const update = useMutation({
    mutationKey: ["put-ballot-preferences"],
    mutationFn: (next: BallotPreferences) =>
      apiRequest<BallotPreferences>("/api/me/ballot-preferences", { method: "PUT", body: next }),
    onSuccess: (saved) => {
      queryClient.setQueryData(["me", "ballot-preferences"], saved);
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
    },
    onSettled: () => {
      setPending(null);
    },
  });
  // Cross-mount in-flight guard: component-local isPending resets on remount
  // (navigate away and back mid-save), but the mutation cache does not — a
  // remounted control must stay locked until the older full-object PUT
  // settles, or two writes could commit out of order.
  const saving = useIsMutating({ mutationKey: ["put-ballot-preferences"] }) > 0;

  if (prefs.isError) {
    return <ErrorNotice error={prefs.error} />;
  }
  if (!prefs.data) {
    return null;
  }
  const current = pending ?? prefs.data;

  function change(fields: Partial<BallotPreferences>) {
    const next = { ...current, ...fields };
    setPending(next);
    update.mutate(next);
  }

  return (
    <div className="flex flex-col items-end gap-1">
      <div className="flex flex-wrap items-center gap-4 text-sm text-ink-soft">
        <label className="flex items-center gap-2">
          Sort by
          <select
            value={current.sort}
            // Disabled while a save is in flight: the PUT replaces the FULL
            // object, so concurrent requests could commit out of order and
            // the earlier write would win.
            disabled={saving}
            onChange={(event) => change({ sort: event.target.value as BallotPreferences["sort"] })}
            className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none disabled:opacity-60"
          >
            {BALLOT_SORTS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        <label className="flex cursor-pointer items-center gap-2">
          <input
            type="checkbox"
            checked={current.followed_first}
            disabled={saving}
            onChange={(event) => change({ followed_first: event.target.checked })}
            className="h-4 w-4 accent-rausch"
          />
          Followed candidates first
        </label>
      </div>
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </div>
  );
}

export function SavedBallotPage() {
  useDocumentTitle("Your saved ballot");
  const { me, isLoading, isError: meError, refetch: refetchMe } = useMe();
  const location = useLocation();
  const navigate = useNavigate();
  // Set when SavedAddressForm just navigated here after a successful save;
  // the notice carries the matched address and the ambiguous-match warning.
  const [addressSaved, setAddressSaved] = useState<AddressSavedNoticeData | null>(null);
  // Capture-then-clear: React Router copies navigation state into
  // window.history.state, which survives refresh and back/forward — left in
  // place, the "Address saved" banner (home address included) would replay
  // from the history entry indefinitely, even beside a newer address's
  // ballot. An effect, not a mount-time read: the empty-state form on this
  // page saves without remounting it, so the state can arrive mid-life.
  useEffect(() => {
    const saved = (location.state as Partial<AddressSavedLocationState> | null)?.addressSaved;
    if (saved) {
      setAddressSaved(saved);
      void navigate(location.pathname, { replace: true, state: null });
    }
  }, [location.pathname, location.state, navigate]);
  const queryClient = useQueryClient();
  const { weights: savedAreaWeights } = useMyResearchAreas();
  const [handoffState, setHandoffState] = useState<"pending" | "done" | "failed">(() =>
    readPendingDistrictIds().length === 0 ? "done" : "pending"
  );
  const handoffFiredRef = useRef(false);

  const verified = me?.email_verified === true;

  // Anonymous-to-account handoff: initialize saved districts from the last
  // anonymous search, but only once verified (the endpoint is
  // verified-email-gated). Only a definitive rejection of the payload itself
  // (400: malformed or unknown district ids) resolves to "the account's
  // ballot is the source of truth". Everything else — 401/403 (session or
  // verification state changed server-side), 429, network and server
  // failures — is recoverable, so keep the queued ids and surface an
  // explicit retry instead of dropping the user onto the empty set-address
  // form with their search lost.
  useEffect(() => {
    if (!verified || handoffState !== "pending" || handoffFiredRef.current) {
      return;
    }
    handoffFiredRef.current = true;
    const districtIds = readPendingDistrictIds();
    void (async () => {
      try {
        await apiRequest("/api/me/districts/initialize", {
          method: "POST",
          body: { district_ids: districtIds },
        });
        clearPendingDistrictIds();
        setHandoffState("done");
        void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      } catch (error) {
        if (error instanceof ApiError && error.status === 400) {
          clearPendingDistrictIds();
          setHandoffState("done");
          void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
        } else {
          setHandoffState("failed");
        }
      }
    })();
  }, [verified, handoffState, queryClient]);

  function retryHandoff() {
    handoffFiredRef.current = false;
    setHandoffState("pending");
  }

  const ballot = useQuery<SavedBallot>({
    queryKey: ["me", "ballot"],
    queryFn: () => apiRequest<SavedBallot>("/api/me/ballot"),
    enabled: verified && handoffState === "done",
    retry: false,
  });

  if (meError) {
    // /api/me failed for a non-auth reason (network, 5xx): without this the
    // me === undefined guard below would spin forever.
    return (
      <div className="mx-auto max-w-md px-4 py-10 space-y-4 text-center">
        <p className="text-ink-soft">We couldn't check your session. Please try again.</p>
        <button
          type="button"
          onClick={() => void refetchMe()}
          className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
        >
          Retry
        </button>
      </div>
    );
  }
  if (isLoading || me === undefined) {
    return <LoadingNotice text="Loading…" />;
  }
  if (me === null) {
    return (
      <div className="mx-auto max-w-md px-4 py-10 text-center">
        <p className="text-ink-soft">Log in to see your saved ballot.</p>
        <p className="mt-4">
          <Link
            to="/login"
            className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
          >
            Log in
          </Link>
        </p>
      </div>
    );
  }
  if (!verified) {
    return <VerifyPrompt email={me.email} />;
  }

  if (handoffState === "failed") {
    return (
      <div className="mx-auto max-w-md px-4 py-10 space-y-4 text-center">
        <p className="text-ink-soft">
          We couldn't save the districts from your recent address search to your account. Your search is
          still remembered.
        </p>
        <button
          type="button"
          onClick={retryHandoff}
          className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
        >
          Try again
        </button>
      </div>
    );
  }

  if (handoffState !== "done" || ballot.isPending) {
    return <LoadingNotice text="Loading your ballot…" />;
  }

  if (ballot.isError) {
    // A 403 here means verification state changed server-side; anything else
    // is a real error.
    if (ballot.error instanceof ApiError && ballot.error.status === 403) {
      return <VerifyPrompt email={me.email} />;
    }
    return (
      <div className="mx-auto max-w-3xl px-4 py-8">
        <ErrorNotice error={ballot.error} />
      </div>
    );
  }

  const data = ballot.data;

  if (data.districts.length === 0) {
    return (
      <div className="mx-auto max-w-2xl px-4 py-10">
        {/* A save can succeed and still map to zero districts; without the
            notice the user lands back on the form with no feedback. */}
        {addressSaved ? (
          <div className="mb-4">
            <AddressSavedNotice saved={addressSaved} />
          </div>
        ) : null}
        <h1 className="text-2xl font-bold">Set your address</h1>
        <p className="mt-2 text-sm text-ink-soft">
          Save your voting address once and your ballot will be waiting every time you come back.
        </p>
        <div className="mt-3">
          <SavedAddressForm inputId="saved-address" label="Voting address" />
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      {addressSaved ? (
        <div className="mb-4">
          <AddressSavedNotice saved={addressSaved} />
        </div>
      ) : null}
      {/* No visible page heading or count subtitle: the date group headings
          ("Elections on …") carry the page's identity. The sr-only h1 keeps
          a level-1 target for screen-reader heading navigation. */}
      <h1 className="sr-only">Your saved ballot</h1>
      <div className="flex flex-wrap items-center justify-end gap-3">
        <BallotPreferenceControls />
      </div>

      {data.elections.length === 0 ? (
        <EmptyNotice text="No upcoming elections found for your districts yet. Check back — new elections are added as they are announced." />
      ) : (
        <ElectionList elections={data.elections} savedAreaWeights={savedAreaWeights} />
      )}

      <p className="mt-8 text-sm text-ink-soft">
        Moved?{" "}
        <Link to="/me/settings" className="underline hover:text-ink">
          Change your address in Settings
        </Link>
        .
      </p>
      <p className="mt-2 text-sm text-ink-soft">
        Looking somewhere else?{" "}
        <Link to="/?new=1" className="underline hover:text-ink">
          Run a one-off address search
        </Link>{" "}
        without changing your saved address.
      </p>
    </div>
  );
}

export default SavedBallotPage;
