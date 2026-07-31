import type { BallotSummary } from "@voteapp/api-client";
import { ADDRESS_FIELD_PRIVACY_NOTE, apiRequest } from "@voteapp/api-client";
import { useIsMutating, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { registerForPushRequestingPermission } from "../lib/pushNotifications";
import { AddressAutocomplete } from "./AddressAutocomplete";
import { ErrorNotice } from "./Status";

type SavedBallot = BallotSummary & { matched_address?: string; address_match_count?: number };

// Saves the account's home address and replaces the saved districts. Used by
// the settings "Your address" screen and the my-ballot empty state. The PUT
// succeeds silently server-side, so the confirmation line here is the user's
// only feedback — without it a save looks like nothing happened.

export function SavedAddressForm({ label }: { label: string }) {
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
      // Saved-ballot moment: one of the two places the push permission
      // prompt may appear (the other: first follow). No-op after a denial
      // or when already registered.
      void registerForPushRequestingPermission();
    },
  });
  // Cross-mount in-flight guard, same as the other full-replace preference
  // writes: the PUT replaces ALL saved districts, and this form exists on
  // two screens that tabs keep mounted at once (my-ballot empty state and
  // settings), so a submit must wait for the older request to settle or the
  // earlier address could win.
  const saving = useIsMutating({ mutationKey: ["put-address"] }) > 0;

  function onAddressChange(next: string) {
    // Editing starts a new attempt: drop the previous save's confirmation
    // (or error) so it cannot read as status for the address being typed.
    if (!update.isIdle && !update.isPending) {
      update.reset();
    }
    setAddress(next);
  }

  const canSave = address.trim().length > 0 && !saving;

  return (
    <View className="mt-2 gap-3">
      <View>
        <Text className="text-sm font-medium text-ink">{label}</Text>
        <AddressAutocomplete
          value={address}
          onChange={onAddressChange}
          placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
        />
        <Text className="mt-1 text-xs text-ink-soft">{ADDRESS_FIELD_PRIVACY_NOTE}</Text>
      </View>
      <Pressable
        disabled={!canSave}
        onPress={() => {
          // `saving` is from the last render; re-check the mutation cache so
          // a tap landing before the disabling re-render cannot start a
          // second overlapping PUT.
          if (!address.trim() || queryClient.isMutating({ mutationKey: ["put-address"] }) > 0) {
            return;
          }
          update.mutate(address.trim());
        }}
        accessibilityRole="button"
        className={
          canSave ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark" : "w-full rounded-md bg-line px-4 py-3"
        }
      >
        <Text className="text-center font-semibold text-white">
          {saving ? "Saving…" : "Save address"}
        </Text>
      </Pressable>
      {update.isSuccess ? (
        // Polite live region, matching the web's role="status": a success
        // confirmation should not interrupt the screen reader mid-speech.
        <View accessibilityLiveRegion="polite" className="rounded-md border border-line bg-surface px-3 py-2">
          {/* Same copy as the web AddressSavedNotice: the account keeps
              election districts, not the address. States only what WAS
              saved ("in your profile"), no absolute claim about the
              address — the backend keeps a 14-day geocoder cache (see
              addressResolutionCache.ts) that the privacy policy discloses. */}
          <Text className="text-sm text-ink">
            Your election districts are updated
            {update.data.matched_address ? (
              <>
                {" from "}
                <Text className="font-semibold">{update.data.matched_address}</Text>
              </>
            ) : null}
            . Only the new election districts were saved in your profile.
            {typeof update.data.address_match_count === "number" && update.data.address_match_count > 1 ? (
              // The geocoder returned multiple candidates and saved the first —
              // a silently wrong match here replaces the user's whole ballot.
              <>
                {" "}Your address matched {update.data.address_match_count} possible locations and the first one
                was used — if the matched address is not yours, save again with your full street address, city,
                and ZIP code.
              </>
            ) : null}
          </Text>
        </View>
      ) : null}
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </View>
  );
}
