import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest } from "../api/client";
import type { BallotSummary } from "../api/types";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { AiBanner } from "../components/AiBanner";
import { ElectionCard } from "../components/ElectionCard";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { useMe } from "../lib/useMe";
import { clearPendingDistrictIds, readPendingDistrictIds } from "../lib/pendingDistricts";
import { PRIVACY_NOTICE } from "../legal/copy";
import { VerifyPrompt } from "../components/VerifyPrompt";

type SavedBallot = BallotSummary & { matched_address?: string };

function AddressForm({ compact }: { compact: boolean }) {
  const [address, setAddress] = useState("");
  const queryClient = useQueryClient();

  const update = useMutation({
    mutationFn: () =>
      apiRequest<SavedBallot>("/api/me/address", { method: "PUT", body: { address: address.trim() } }),
    onSuccess: (ballot) => {
      queryClient.setQueryData(["me", "ballot"], ballot);
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
  const { me, isLoading } = useMe();
  const queryClient = useQueryClient();
  const [handoffDone, setHandoffDone] = useState(() => readPendingDistrictIds().length === 0);
  const handoffFiredRef = useRef(false);

  const verified = me?.email_verified === true;

  // Anonymous-to-account handoff: initialize saved districts from the last
  // anonymous search, but only once verified (the endpoint is
  // verified-email-gated). already_initialized and stale-id errors both
  // resolve to "proceed with whatever the account has".
  useEffect(() => {
    if (!verified || handoffDone || handoffFiredRef.current) {
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
      } catch {
        // unknown_district_ids (stale local data) or transient failure —
        // either way the saved ballot below is the source of truth.
      } finally {
        clearPendingDistrictIds();
        setHandoffDone(true);
        void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      }
    })();
  }, [verified, handoffDone, queryClient]);

  const ballot = useQuery<SavedBallot>({
    queryKey: ["me", "ballot"],
    queryFn: () => apiRequest<SavedBallot>("/api/me/ballot"),
    enabled: verified && handoffDone,
    retry: false,
  });

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

  if (!handoffDone || ballot.isPending) {
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
      <h1 className="text-2xl font-bold">Your saved ballot</h1>
      <p className="mt-1 text-sm text-ink-soft">
        {data.elections.length} election{data.elections.length === 1 ? "" : "s"} across{" "}
        {data.districts.length} district{data.districts.length === 1 ? "" : "s"}, ordered by where your vote
        carries the most weight.
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
