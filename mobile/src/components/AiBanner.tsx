import { AI_BANNER } from "@voteapp/api-client";
import { Text } from "react-native";
import { openExternalUrl, WEB_ORIGIN } from "../lib/openExternalUrl";

/** Rendered at the top of every ballot, election, and candidate view. */
export function AiBanner() {
  return (
    <Text className="mb-4 rounded-lg border border-line bg-surface px-3 py-2 text-xs text-ink-soft">
      {AI_BANNER}{" "}
      <Text className="underline" onPress={() => openExternalUrl(`${WEB_ORIGIN}/disclaimer`)}>
        Learn more
      </Text>
    </Text>
  );
}
