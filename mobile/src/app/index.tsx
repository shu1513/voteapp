import AsyncStorage from "@react-native-async-storage/async-storage";
import type { AddressResolution } from "@voteapp/api-client";
import {
  apiRequest,
  PRE_SEARCH_ACCEPTANCE_STORAGE_KEY,
  PRE_SEARCH_CHECKBOX_LABEL,
  PRIVACY_NOTICE,
  useMe,
} from "@voteapp/api-client";
import { useMutation } from "@tanstack/react-query";
import { Stack, useRouter } from "expo-router";
import { useEffect, useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { AddressAutocomplete } from "../components/AddressAutocomplete";
import { LegalGate } from "../components/LegalGate";
import { ErrorNotice } from "../components/Status";
import { useLogout } from "../lib/auth";
import { setMatchedAddress } from "../lib/matchedAddress";

/**
 * Signed-in state / auth entry links. The account screens (saved ballot,
 * follows, settings) land in the next chunk; until then this strip is the
 * whole account surface.
 */
function AuthStrip() {
  const router = useRouter();
  const { me, isLoading } = useMe();
  const logout = useLogout();
  if (isLoading) {
    return null;
  }
  if (!me) {
    return (
      <View className="flex-row justify-end gap-4 pb-2">
        <Text
          className="text-sm text-ink underline"
          accessibilityRole="link"
          onPress={() => router.push("/auth/login")}
        >
          Log in
        </Text>
        <Text
          className="text-sm text-ink underline"
          accessibilityRole="link"
          onPress={() => router.push("/auth/register")}
        >
          Sign up
        </Text>
      </View>
    );
  }
  return (
    <View className="flex-row items-center justify-end gap-4 pb-2">
      <Text className="shrink text-sm text-ink-soft" numberOfLines={1}>
        {me.email}
        {me.email_verified ? "" : " (unverified)"}
      </Text>
      <Text
        className="text-sm text-ink underline"
        accessibilityRole="button"
        onPress={() => {
          if (!logout.isPending) {
            logout.mutate();
          }
        }}
      >
        {logout.isPending ? "Logging out…" : "Log out"}
      </Text>
    </View>
  );
}

/**
 * Port of the web HomePage's anonymous flow. Web-only concerns dropped:
 * the verified-user redirect to the saved ballot (no account screens yet)
 * and pre-hydration input adoption (no SSR). The matched address goes
 * through an in-memory holder — Expo Router serializes params into its
 * URL-based navigation state, and the address must never reach a URL.
 */
export default function HomeScreen() {
  const router = useRouter();
  const [address, setAddress] = useState("");
  const [accepted, setAccepted] = useState(false);

  // Same acceptance persistence as the web (localStorage there), keyed by
  // terms version so a bump re-prompts. Load failures leave the box
  // unchecked — the conservative default.
  useEffect(() => {
    AsyncStorage.getItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY)
      .then((stored) => {
        if (stored === "true") {
          setAccepted(true);
        }
      })
      .catch(() => {});
  }, []);

  const resolve = useMutation({
    mutationFn: (input: string) =>
      apiRequest<AddressResolution>("/api/address/resolve", { method: "POST", body: { address: input } }),
    onSuccess: (resolution) => {
      // Straight to the elections — the districts list is a detour nobody
      // asked for. (The web's pending-districts signup handoff lands with
      // the auth screens.) The matched address goes through the in-memory
      // holder, never navigation params — see lib/matchedAddress.ts.
      setMatchedAddress(resolution.matched_address);
      router.push({
        pathname: "/ballot",
        params: { d: resolution.districts.map((district) => district.id).join(",") },
      });
    },
  });

  const canSearch = accepted && address.trim().length > 0 && !resolve.isPending;

  function onAcceptChange(checked: boolean) {
    setAccepted(checked);
    const write = checked
      ? AsyncStorage.setItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY, "true")
      : AsyncStorage.removeItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY);
    write.catch(() => {
      // Storage failures must not block searching.
    });
  }

  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title: "VoteApp" }} />
      <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="px-4 py-10">
        <AuthStrip />
        <Text className="text-3xl font-bold text-ink">Find what&apos;s on your ballot</Text>
        <Text className="mt-2 text-ink-soft">
          Enter your home address to see the elections coming up on your ballot.
        </Text>

        <View className="mt-6 gap-4">
          <View>
            <Text className="text-sm font-medium text-ink">Home address</Text>
            <AddressAutocomplete
              value={address}
              onChange={setAddress}
              placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
            />
            <Text className="mt-1 text-xs text-ink-soft">{PRIVACY_NOTICE}</Text>
          </View>

          <LegalGate label={PRE_SEARCH_CHECKBOX_LABEL} checked={accepted} onChange={onAcceptChange} />

          <Pressable
            disabled={!canSearch}
            onPress={() => resolve.mutate(address.trim())}
            accessibilityRole="button"
            className={
              canSearch ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark" : "w-full rounded-md bg-line px-4 py-3"
            }
          >
            <Text className="text-center font-semibold text-white">
              {resolve.isPending ? "Searching…" : "Search"}
            </Text>
          </Pressable>
        </View>

        {resolve.isError ? (
          <View className="mt-4">
            <ErrorNotice error={resolve.error} />
          </View>
        ) : null}
      </ScrollView>
    </KeyboardAvoidingView>
  );
}
