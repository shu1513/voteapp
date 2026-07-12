import type { BallotSummary } from "@voteapp/api-client";
import { apiRequest, PRIVACY_NOTICE } from "@voteapp/api-client";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { registerForPushRequestingPermission } from "../lib/pushNotifications";
import { AddressAutocomplete } from "./AddressAutocomplete";
import { ErrorNotice } from "./Status";

type SavedBallot = BallotSummary & { matched_address?: string };

// Saves the account's home address and replaces the saved districts. Used by
// the settings "Home address" screen and the my-ballot empty state. The PUT
// succeeds silently server-side, so the confirmation line here is the user's
// only feedback — without it a save looks like nothing happened.

export function SavedAddressForm({ label }: { label: string }) {
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
      // Saved-ballot moment: one of the two places the push permission
      // prompt may appear (the other: first follow). No-op after a denial
      // or when already registered.
      void registerForPushRequestingPermission();
    },
  });

  const canSave = address.trim().length > 0 && !update.isPending;

  return (
    <View className="mt-2 gap-3">
      <View>
        <Text className="text-sm font-medium text-ink">{label}</Text>
        <AddressAutocomplete
          value={address}
          onChange={setAddress}
          placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
        />
        <Text className="mt-1 text-xs text-ink-soft">{PRIVACY_NOTICE}</Text>
      </View>
      <Pressable
        disabled={!canSave}
        onPress={() => update.mutate()}
        accessibilityRole="button"
        className={
          canSave ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark" : "w-full rounded-md bg-line px-4 py-3"
        }
      >
        <Text className="text-center font-semibold text-white">
          {update.isPending ? "Saving…" : "Save address"}
        </Text>
      </Pressable>
      {update.isSuccess ? (
        <View accessibilityRole="alert" className="rounded-md border border-line bg-surface px-3 py-2">
          <Text className="text-sm text-ink">
            Address saved
            {update.data.matched_address ? (
              <>
                {" — matched to "}
                <Text className="font-semibold">{update.data.matched_address}</Text>
              </>
            ) : null}
            . Your ballot now covers {update.data.districts.length} district
            {update.data.districts.length === 1 ? "" : "s"}.
          </Text>
        </View>
      ) : null}
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </View>
  );
}
