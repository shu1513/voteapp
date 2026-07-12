import { AI_BANNER } from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { Text } from "react-native";

/** Rendered at the top of every ballot, election, and candidate view. */
export function AiBanner() {
  const router = useRouter();
  return (
    <Text className="mb-4 rounded-lg border border-line bg-surface px-3 py-2 text-xs text-ink-soft">
      {AI_BANNER}{" "}
      <Text className="underline" accessibilityRole="link" onPress={() => router.push("/legal/disclaimer")}>
        Learn more
      </Text>
    </Text>
  );
}
