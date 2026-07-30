import type { AddressResolution } from "@voteapp/api-client";
import { apiRequest, ADDRESS_FIELD_PRIVACY_NOTE, TERMS_VERSION, useMe } from "@voteapp/api-client";
import { useMutation } from "@tanstack/react-query";
import { useRouter } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { AddressAutocomplete } from "../../components/AddressAutocomplete";
import { PreSearchTermsSheet } from "../../components/PreSearchTermsSheet";
import { ErrorNotice } from "../../components/Status";
import { useLogout } from "../../lib/auth";
import { setMatchedAddress } from "../../lib/matchedAddress";
import { savePendingDistrictIds } from "../../lib/pendingDistricts";
import { hasCurrentTermsAcceptance, rememberTermsAcceptance } from "../../lib/termsAcceptance";

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
  const [termsVisible, setTermsVisible] = useState(false);
  // The sheet's checkbox. Reset to false every time the sheet opens, never
  // seeded from storage: remembering decides whether the sheet opens and
  // nothing more. A box that arrives pre-ticked shows assent nobody gave.
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
    onError: () => {
      // Show the failure on the screen behind rather than inside the sheet.
      // The acceptance is already recorded, so a retry goes straight through.
      setTermsVisible(false);
    },
  });

  const canSearch = address.trim().length > 0 && !resolve.isPending;

  async function onSearchPress() {
    if (!canSearch) {
      return;
    }
    if (await hasCurrentTermsAcceptance()) {
      resolve.mutate(address.trim());
      return;
    }
    setAccepted(false);
    setTermsVisible(true);
  }

  function agreeAndSearch() {
    if (!accepted || resolve.isPending) {
      return;
    }
    // Recorded before the request, so a failed search does not re-ask for an
    // agreement already given. Fire-and-forget: never block the search on
    // being able to remember it.
    void rememberTermsAcceptance();
    resolve.mutate(address.trim());
  }

  function cancelTerms() {
    if (resolve.isPending) {
      return;
    }
    setTermsVisible(false);
    setAccepted(false);
    // The typed address is deliberately left alone.
  }

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
        {/* What the service is, where a first-time visitor looks — same line
            the web hero carries. */}
        <Text className="mt-3 text-sm font-medium text-ink-soft">
          Independent, nonpartisan, AI-assisted election research with linked sources.
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
            {/* Notice belongs at the field: the autocomplete forwards what is
                typed after three characters, so collection starts while the
                visitor types and long before Search. */}
            <Text className="mt-1 text-xs text-ink-soft">
              {ADDRESS_FIELD_PRIVACY_NOTE}{" "}
              <Text
                className="underline"
                accessibilityRole="link"
                onPress={() => router.push("/legal/privacy")}
              >
                Privacy notice
              </Text>
            </Text>
          </View>

          <Pressable
            disabled={!canSearch}
            onPress={() => void onSearchPress()}
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

      <PreSearchTermsSheet
        visible={termsVisible}
        checked={accepted}
        onCheckedChange={setAccepted}
        onAgree={agreeAndSearch}
        onCancel={cancelTerms}
        pending={resolve.isPending}
      />
    </KeyboardAvoidingView>
  );
}
