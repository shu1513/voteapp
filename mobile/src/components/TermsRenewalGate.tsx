import { RENEWAL_CHECKBOX_LABEL, TERMS_VERSION, useAcceptTerms, useMe } from "@voteapp/api-client";
import { usePathname } from "expo-router";
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { LegalGate } from "./LegalGate";

/**
 * Blocking overlay for signed-in users whose recorded terms acceptance
 * predates the current version — the mobile counterpart of the web's
 * TermsRenewalGate, with the same clickwrap rules (unchecked box, action
 * disabled until checked, adjacent document links via LegalGate).
 *
 * Mounted in the root layout after the Stack so it sits above every screen
 * EXCEPT the legal documents: the LegalGate links push /legal/*, and a user
 * has to be able to read what they are agreeing to.
 */
export function TermsRenewalGate() {
  const { me } = useMe();
  const pathname = usePathname();
  const acceptTerms = useAcceptTerms();
  const [checked, setChecked] = useState(false);

  if (!me || me.accepted_terms_version === TERMS_VERSION) {
    return null;
  }
  if (pathname.startsWith("/legal/")) {
    return null;
  }

  const canAccept = checked && !acceptTerms.isPending;
  return (
    <View
      accessibilityViewIsModal
      className="absolute inset-0 items-center justify-center bg-ink/40 p-4"
    >
      <View className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl">
        <Text accessibilityRole="header" className="text-lg font-bold text-ink">
          Our terms have been updated
        </Text>
        <Text className="mt-2 text-sm text-ink-soft">
          To keep using VoteApp, please review and accept the updated Terms of Use, Privacy Policy,
          and Disclaimer.
        </Text>
        <View className="mt-4">
          <LegalGate label={RENEWAL_CHECKBOX_LABEL} checked={checked} onChange={setChecked} />
        </View>
        {acceptTerms.isError ? (
          <Text accessibilityLiveRegion="assertive" className="mt-3 text-sm text-red-900">
            Something went wrong recording your acceptance. Please try again.
          </Text>
        ) : null}
        <Pressable
          disabled={!canAccept}
          onPress={() => acceptTerms.mutate(TERMS_VERSION)}
          accessibilityRole="button"
          accessibilityState={{ disabled: !canAccept }}
          className={
            canAccept
              ? "mt-4 w-full rounded-lg bg-rausch px-4 py-3 active:bg-rausch-dark"
              : "mt-4 w-full rounded-lg bg-line px-4 py-3"
          }
        >
          <Text className="text-center font-semibold text-white">
            {acceptTerms.isPending ? "Saving…" : "Agree and continue"}
          </Text>
        </Pressable>
      </View>
    </View>
  );
}
