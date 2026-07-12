import type { Me } from "@voteapp/api-client";
import { useMe } from "@voteapp/api-client";
import { useRouter } from "expo-router";
import type { ReactNode } from "react";
import { Pressable, Text, View } from "react-native";
import { LoadingNotice } from "./Status";
import { VerifyPrompt } from "./VerifyPrompt";

/**
 * Shared gate for account screens (saved ballot, follows, settings): session
 * loading, identity-fetch failure with retry, signed-out login prompt, and
 * the unverified interstitial. Children render only for a verified user —
 * the same ladder every web account page implements inline.
 *
 * `allowUnverified` skips the verify interstitial for screens that mirror
 * the backend's own gating: profile, password, email change, sessions and
 * delete all work unverified (fixing a typo or leaving must not require a
 * verified inbox — same rule as the web settings page).
 */
export function AccountGate({
  signedOutText,
  allowUnverified = false,
  children,
}: {
  signedOutText: string;
  allowUnverified?: boolean;
  children: (me: Me) => ReactNode;
}) {
  const router = useRouter();
  const { me, isLoading, isError, refetch } = useMe();

  if (isError) {
    // /api/me failed for a non-auth reason (network, 5xx): without this the
    // undefined guard below would spin forever.
    return (
      <View className="items-center px-4 py-10">
        <Text className="text-ink-soft">We couldn&apos;t check your session. Please try again.</Text>
        <Pressable
          onPress={() => void refetch()}
          accessibilityRole="button"
          className="mt-4 rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
        >
          <Text className="font-semibold text-white">Retry</Text>
        </Pressable>
      </View>
    );
  }
  if (isLoading || me === undefined) {
    return <LoadingNotice text="Loading…" />;
  }
  if (me === null) {
    return (
      <View className="items-center px-4 py-10">
        <Text className="text-ink-soft">{signedOutText}</Text>
        <Pressable
          onPress={() => router.push("/auth/login")}
          accessibilityRole="link"
          className="mt-4 rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
        >
          <Text className="font-semibold text-white">Log in</Text>
        </Pressable>
      </View>
    );
  }
  if (!me.email_verified && !allowUnverified) {
    return <VerifyPrompt email={me.email} />;
  }
  return <>{children(me)}</>;
}
