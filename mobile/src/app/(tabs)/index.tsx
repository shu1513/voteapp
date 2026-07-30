import type { AddressResolution } from "@voteapp/api-client";
import {
  apiRequest,
  PRE_SEARCH_AGREEMENT_PARAGRAPHS,
  PRE_SEARCH_CHECKBOX_LABEL,
  PRIVACY_NOTICE,
  TERMS_VERSION,
  useMe,
} from "@voteapp/api-client";
import { useMutation } from "@tanstack/react-query";
import { useRouter } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { AddressAutocomplete } from "../../components/AddressAutocomplete";
import { LegalGate } from "../../components/LegalGate";
import { ErrorNotice } from "../../components/Status";
import { useLogout } from "../../lib/auth";
import { setMatchedAddress } from "../../lib/matchedAddress";
import { savePendingDistrictIds } from "../../lib/pendingDistricts";

/**
 * Signed-in state / auth entry links. The account screens (saved ballot,
 * follows, settings) land in the next chunk; until then this strip is the
 * whole account surface.
 */
function AuthStrip() {
  const router = useRouter();
  const { me, isLoading, isError } = useMe();
  const logout = useLogout();
  // A failed identity fetch (backend down, network) must not masquerade as
  // "signed out" — render nothing rather than the log-in links.
  if (isLoading || isError) {
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
  const { me } = useMe();
  const [address, setAddress] = useState("");
  // Always starts false and is never restored from storage — same rule as
  // the web: agreeing has to be an affirmative act on this visit, so the box
  // is never handed to the user pre-ticked. Expo retains this tab while a
  // ballot is pushed on top, so returning from a search shows the box still
  // ticked — that is deliberate: it reflects the tick the user made moments
  // ago in this same session, not a restored one. A fresh app launch starts
  // unchecked again.
  const [accepted, setAccepted] = useState(false);

  const resolve = useMutation({
    mutationFn: (input: string) =>
      // The accepted version rides along because the endpoint enforces the
      // clickwrap too, refusing a search without one. Nothing is stored
      // server-side — same rule as the web.
      apiRequest<AddressResolution>("/api/address/resolve", {
        method: "POST",
        body: { address: input, accepted_terms_version: TERMS_VERSION },
      }),
    onSuccess: (resolution) => {
      // Stash for the anonymous-to-account handoff: if this visitor signs
      // up, these districts become their saved ballot once they verify.
      // Save only when identity is KNOWN to be logged out or unverified —
      // while /api/me is still loading (me === undefined) a verified user's
      // one-off search must not re-arm the handoff. Same rule as the web.
      if (me === null || me?.email_verified === false) {
        void savePendingDistrictIds(resolution.districts.map((district) => district.id));
      }
      // Straight to the elections — the districts list is a detour nobody
      // asked for. The matched address goes through the in-memory holder,
      // never navigation params — see lib/matchedAddress.ts.
      setMatchedAddress(resolution.matched_address, resolution.address_match_count);
      router.push({
        pathname: "/ballot",
        params: { d: resolution.districts.map((district) => district.id).join(",") },
      });
    },
  });

  const canSearch = accepted && address.trim().length > 0 && !resolve.isPending;

  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="px-4 py-10">
        <AuthStrip />
        {/* One sentence, no sub-line — same headline as the web home. Two
            steps below the old 3xl: at phone width a full sentence set that
            large ran six lines and pushed the address field off the fold. */}
        <Text className="text-xl font-bold text-ink">
          Find out exactly what elections you can vote on, and who these candidates really are by
          their records instead of their slogans.
        </Text>

        <View className="mt-6 gap-4">
          <View>
            {/* Instructional label for first-time visitors — same copy as
                the web home. Signed-in surfaces keep "Your address". */}
            <Text className="text-sm font-medium text-ink">
              Enter your address to see the elections you can vote in
            </Text>
            <AddressAutocomplete
              value={address}
              onChange={setAddress}
              placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
              accessibilityLabel="Enter your address to see the elections you can vote in"
            />
            <Text className="mt-1 text-xs text-ink-soft">{PRIVACY_NOTICE}</Text>
          </View>

          <LegalGate
            label={PRE_SEARCH_CHECKBOX_LABEL}
            checked={accepted}
            onChange={setAccepted}
            fullAgreement={{
              paragraphs: PRE_SEARCH_AGREEMENT_PARAGRAPHS,
              privacyNotice: PRIVACY_NOTICE,
            }}
          />

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
