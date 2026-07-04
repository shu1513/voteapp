import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest } from "../api/client";
import {
  BALLOT_SORT_DESCRIPTIONS,
  BALLOT_SORTS,
  type BallotPreferences,
  type BallotSummary,
} from "../api/types";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { AiBanner } from "../components/AiBanner";
import { ElectionCard } from "../components/ElectionCard";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { useMe } from "../lib/useMe";
import { clearPendingDistrictIds, readPendingDistrictIds } from "../lib/pendingDistricts";
import { PRIVACY_NOTICE } from "../legal/copy";
import { VerifyPrompt } from "../components/VerifyPrompt";

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
            disabled={update.isPending}
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
            disabled={update.isPending}
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

function AddressForm({ compact }: { compact: boolean }) {
  const [address, setAddress] = useState("");
  const queryClient = useQueryClient();

  const update = useMutation({
    mutationFn: () =>
      apiRequest<SavedBallot>("/api/me/address", { method: "PUT", body: { address: address.trim() } }),
    onSuccess: () => {
      // The PUT returns a plain district ballot, but GET /api/me/ballot
      // applies saved sort preferences and followed-candidate ordering —
      // refetch the canonical version instead of caching the PUT body.
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      setAddress("");
    },
  });

  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (address.trim() && !update.isPending) {
          update.mutate();
        }
      }}
      className={compact ? "mt-2 space-y-3" : "mt-6 space-y-3"}
    >
      <div>
        <label htmlFor="saved-address" className="block text-sm font-medium text-ink">
          {compact ? "New address" : "Home address"}
        </label>
        <AddressAutocomplete
          inputId="saved-address"
          value={address}
          onChange={setAddress}
          placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
        />
        <p className="mt-1 text-xs text-ink-soft">{PRIVACY_NOTICE}</p>
      </div>
      <button
        type="submit"
        disabled={!address.trim() || update.isPending}
        className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
      >
        {update.isPending ? "Saving…" : "Save address"}
      </button>
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </form>
  );
}

export function SavedBallotPage() {
  const { me, isLoading, isError: meError, refetch: refetchMe } = useMe();
  const queryClient = useQueryClient();
  // Same key as BallotPreferenceControls: shared cache entry, no extra fetch.
  // Drives the subtitle so the copy matches the saved sort.
  const savedPrefs = useQuery({
    queryKey: ["me", "ballot-preferences"],
    queryFn: () => apiRequest<BallotPreferences>("/api/me/ballot-preferences"),
    staleTime: 60_000,
    enabled: me?.email_verified === true,
  });
  const [handoffState, setHandoffState] = useState<"pending" | "done" | "failed">(() =>
    readPendingDistrictIds().length === 0 ? "done" : "pending"
  );
  const handoffFiredRef = useRef(false);

  const verified = me?.email_verified === true;

  // Anonymous-to-account handoff: initialize saved districts from the last
  // anonymous search, but only once verified (the endpoint is
  // verified-email-gated). Permanent rejections (4xx: stale/unknown ids)
  // resolve to "the account's ballot is the source of truth"; transient
  // failures keep the ids and surface an explicit retry instead of dropping
  // the user onto the empty set-address form with their search still queued.
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
        if (error instanceof ApiError && error.status < 500) {
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
        <h1 className="text-2xl font-bold">Set your address</h1>
        <p className="mt-2 text-sm text-ink-soft">
          Save your home address once and your ballot will be waiting every time you come back.
        </p>
        <AddressForm compact={false} />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <AiBanner />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-bold">Your saved ballot</h1>
        <BallotPreferenceControls />
      </div>
      <p className="mt-1 text-sm text-ink-soft">
        {data.elections.length} election{data.elections.length === 1 ? "" : "s"} across{" "}
        {data.districts.length} district{data.districts.length === 1 ? "" : "s"},{" "}
        {BALLOT_SORT_DESCRIPTIONS[savedPrefs.data?.sort ?? "vote_power"]}
      </p>

      {data.elections.length === 0 ? (
        <EmptyNotice text="No upcoming elections found for your districts yet. Check back — new elections are added as they are announced." />
      ) : (
        <div className="mt-4 space-y-3">
          {data.elections.map((election) => (
            <ElectionCard key={election.id} election={election} />
          ))}
        </div>
      )}

      <details className="mt-8 rounded-xl border border-line bg-surface p-4 text-sm">
        <summary className="cursor-pointer select-none font-medium text-ink">Change your address</summary>
        <AddressForm compact />
      </details>
      <p className="mt-4 text-sm text-ink-soft">
        Looking somewhere else?{" "}
        <Link to="/?new=1" className="underline hover:text-ink">
          Run a one-off address search
        </Link>{" "}
        without changing your saved address.
      </p>
    </div>
  );
}
