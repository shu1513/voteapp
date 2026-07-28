import { useState } from "react";
import { useNavigate } from "react-router";
import { useIsMutating, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest, PRIVACY_NOTICE } from "@voteapp/api-client";
import type { BallotSummary } from "@voteapp/api-client";
import { AddressAutocomplete } from "./AddressAutocomplete";
import { ErrorNotice } from "./Status";

type AddressSaveResult = BallotSummary & { matched_address?: string; address_match_count?: number };

// Only what the confirmation renders. Router state is copied into
// window.history.state and can outlive the navigation (refresh, session
// restore), so the home address's exposure is kept minimal and the ballot
// payload — which GET /api/me/ballot re-derives anyway — stays out entirely.
export type AddressSavedNoticeData = {
  matched_address?: string;
  address_match_count?: number;
};

// Router state carried to /me/ballot after a successful save; the saved
// ballot page renders <AddressSavedNotice> from it, then wipes the history
// entry so the notice shows once instead of replaying on refresh/back.
export type AddressSavedLocationState = { addressSaved: AddressSavedNoticeData };

// Saves the account's home address and replaces the saved districts. Used by
// the settings "Voting address" section and the saved-ballot empty state. A
// successful save navigates to the saved ballot so the user lands on the
// election list for their new districts; the confirmation (including the
// ambiguous-match warning) travels along as router state.

export function SavedAddressForm({ inputId, label }: { inputId: string; label: string }) {
  const [address, setAddress] = useState("");
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const update = useMutation({
    mutationKey: ["put-address"],
    mutationFn: (submitted: string) =>
      apiRequest<AddressSaveResult>("/api/me/address", { method: "PUT", body: { address: submitted } }),
    onSuccess: (saved) => {
      // The PUT returns a plain district ballot, but GET /api/me/ballot
      // applies saved sort preferences and followed-candidate ordering —
      // refetch the canonical version instead of caching the PUT body.
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      void navigate("/me/ballot", {
        state: {
          addressSaved: {
            matched_address: saved.matched_address,
            address_match_count: saved.address_match_count,
          },
        } satisfies AddressSavedLocationState,
      });
    },
  });
  // Cross-mount in-flight guard, same as the other full-replace preference
  // writes: the PUT replaces ALL saved districts, so a submit from a
  // remounted form (or the sibling form on another screen) must wait for the
  // older request to settle or the earlier address could win.
  const saving = useIsMutating({ mutationKey: ["put-address"] }) > 0;

  function onAddressChange(next: string) {
    // Editing starts a new attempt: drop the previous save's error so it
    // cannot read as status for the address being typed.
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
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </form>
  );
}

// Post-save confirmation rendered on the saved ballot page from the router
// state the form navigates with. The PUT succeeds silently server-side, so
// this line is the user's only textual feedback on what was matched. The
// copy leads with election districts, not "address saved", because that is
// what the account keeps: user_districts has no address column. It states
// only what WAS saved ("in your profile") and makes no absolute claim
// about the address — the backend keeps a 14-day geocoder cache (see
// addressResolutionCache.ts) which the privacy policy discloses, so
// "we never save your address" would be false.
export function AddressSavedNotice({ saved }: { saved: AddressSavedNoticeData }) {
  return (
    <p role="status" className="rounded-md border border-line bg-surface px-3 py-2 text-sm text-ink">
      Your election districts are updated
      {saved.matched_address ? <> from <strong>{saved.matched_address}</strong></> : null}. Only the new
      election districts were saved in your profile.
      {typeof saved.address_match_count === "number" && saved.address_match_count > 1 ? (
        // The geocoder returned multiple candidates and saved the first —
        // a silently wrong match here replaces the user's whole ballot.
        <>
          {" "}Your address matched {saved.address_match_count} possible locations and the first one was
          used — if the matched address is not yours, save again with your full street address, city, and ZIP
          code.
        </>
      ) : null}
    </p>
  );
}
