import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest, PRIVACY_NOTICE } from "@voteapp/api-client";
import type { BallotSummary } from "@voteapp/api-client";
import { AddressAutocomplete } from "./AddressAutocomplete";
import { ErrorNotice } from "./Status";

type SavedBallot = BallotSummary & { matched_address?: string };

// Saves the account's home address and replaces the saved districts. Used by
// the settings "Home address" section and the saved-ballot empty state. The
// PUT succeeds silently server-side, so the confirmation line here is the
// user's only feedback — without it a save looks like nothing happened.

export function SavedAddressForm({ inputId, label }: { inputId: string; label: string }) {
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
      className="mt-3 space-y-3"
    >
      <div>
        <label htmlFor={inputId} className="block text-sm font-medium text-ink">
          {label}
        </label>
        <AddressAutocomplete
          inputId={inputId}
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
      {update.isSuccess ? (
        <p role="status" className="rounded-md border border-line bg-surface px-3 py-2 text-sm text-ink">
          Address saved{update.data.matched_address ? <> — matched to <strong>{update.data.matched_address}</strong></> : null}.
          Your ballot now covers {update.data.districts.length} district
          {update.data.districts.length === 1 ? "" : "s"}.
        </p>
      ) : null}
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </form>
  );
}
