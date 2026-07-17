import { useState } from "react";
import { useIsMutating, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest, PRIVACY_NOTICE } from "@voteapp/api-client";
import type { BallotSummary } from "@voteapp/api-client";
import { AddressAutocomplete } from "./AddressAutocomplete";
import { ErrorNotice } from "./Status";

type SavedBallot = BallotSummary & { matched_address?: string; address_match_count?: number };

// Saves the account's home address and replaces the saved districts. Used by
// the settings "Home address" section and the saved-ballot empty state. The
// PUT succeeds silently server-side, so the confirmation line here is the
// user's only feedback — without it a save looks like nothing happened.

export function SavedAddressForm({ inputId, label }: { inputId: string; label: string }) {
  const [address, setAddress] = useState("");
  const queryClient = useQueryClient();

  const update = useMutation({
    mutationKey: ["put-address"],
    mutationFn: (submitted: string) =>
      apiRequest<SavedBallot>("/api/me/address", { method: "PUT", body: { address: submitted } }),
    onSuccess: (_saved, submitted) => {
      // The PUT returns a plain district ballot, but GET /api/me/ballot
      // applies saved sort preferences and followed-candidate ordering —
      // refetch the canonical version instead of caching the PUT body.
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      // Clear only the text that was submitted: anything typed while the
      // save was in flight is the user's next address, not ours to erase.
      setAddress((current) => (current.trim() === submitted ? "" : current));
    },
  });
  // Cross-mount in-flight guard, same as the other full-replace preference
  // writes: the PUT replaces ALL saved districts, so a submit from a
  // remounted form (or the sibling form on another screen) must wait for the
  // older request to settle or the earlier address could win.
  const saving = useIsMutating({ mutationKey: ["put-address"] }) > 0;

  function onAddressChange(next: string) {
    // Editing starts a new attempt: drop the previous save's confirmation
    // (or error) so it cannot read as status for the address being typed.
    if (!update.isIdle && !update.isPending) {
      update.reset();
    }
    setAddress(next);
  }

  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        // `saving` is from the last render; re-check the mutation cache so a
        // submit landing before the disabling re-render cannot start a
        // second overlapping PUT.
        if (!address.trim() || queryClient.isMutating({ mutationKey: ["put-address"] }) > 0) {
          return;
        }
        update.mutate(address.trim());
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
          onChange={onAddressChange}
          placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
        />
        <p className="mt-1 text-xs text-ink-soft">{PRIVACY_NOTICE}</p>
      </div>
      <button
        type="submit"
        disabled={!address.trim() || saving}
        className="w-full rounded-md bg-rausch px-4 py-3 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
      >
        {saving ? "Saving…" : "Save address"}
      </button>
      {update.isSuccess ? (
        <p role="status" className="rounded-md border border-line bg-surface px-3 py-2 text-sm text-ink">
          Address saved{update.data.matched_address ? <> — matched to <strong>{update.data.matched_address}</strong></> : null}.
          Your ballot now covers {update.data.districts.length} district
          {update.data.districts.length === 1 ? "" : "s"}.
          {typeof update.data.address_match_count === "number" && update.data.address_match_count > 1 ? (
            // The geocoder returned multiple candidates and saved the first —
            // a silently wrong match here replaces the user's whole ballot.
            <>
              {" "}Your address matched {update.data.address_match_count} possible locations and the first one was
              used — if the matched address is not yours, save again with your full street address, city, and ZIP
              code.
            </>
          ) : null}
        </p>
      ) : null}
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </form>
  );
}
