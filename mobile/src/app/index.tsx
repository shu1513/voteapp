import { useMe } from "@voteapp/api-client";
import { Stack } from "expo-router";
import { Text, View } from "react-native";

/**
 * Phase 2 placeholder screen: proves NativeWind classes (web theme tokens)
 * and the shared api-client wiring end-to-end. GET /api/me through the
 * package resolves to "signed out" (401 -> null) when the backend is
 * reachable. Real screens land in Phase 3.
 */
export default function HomeScreen() {
  const { me, isLoading, isError } = useMe();

  const apiStatus = isLoading
    ? "checking…"
    : isError
      ? "unreachable"
      : me
        ? `signed in as ${me.email}`
        : "reachable (signed out)";

  return (
    <View className="flex-1 items-center justify-center bg-surface px-6">
      <Stack.Screen options={{ title: "VoteApp" }} />
      <Text className="text-3xl font-bold text-ink">VoteApp</Text>
      <Text className="mt-2 text-base text-ink-soft">Mobile skeleton — Phase 2</Text>
      <View className="mt-6 self-stretch rounded-xl border border-line bg-white px-4 py-3">
        <Text className="text-ink">API: {apiStatus}</Text>
      </View>
      <Text className="mt-4 font-semibold text-rausch">Theme token check</Text>
    </View>
  );
}
