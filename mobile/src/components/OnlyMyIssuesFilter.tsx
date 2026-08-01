import { Pressable, Text, View } from "react-native";

/**
 * "Only my issues" toggle chip plus the hidden-count line, shared by the
 * anonymous and saved ballot screens — port of the web OnlyMyIssuesFilter.
 * Render only when api-client's deriveOnlyMyIssues says showFilter (the
 * derivation lives there so web and mobile share one copy); the screens own
 * the on/off state (plain state — screens stay mounted under a stack push,
 * so the choice survives navigating into an election and back, matching the
 * web's URL param).
 */
export function OnlyMyIssuesToggle({
  on,
  hiddenCount,
  onChange,
}: {
  on: boolean;
  hiddenCount: number;
  onChange: (on: boolean) => void;
}) {
  return (
    <View className="flex-row flex-wrap items-center gap-2">
      {/* Same chip styling as SortChips; a lone toggle rather than a chips
          row because there is no option set to pick from. */}
      <Pressable
        onPress={() => onChange(!on)}
        accessibilityRole="button"
        accessibilityState={{ selected: on }}
        className={
          on
            ? "rounded-full border border-ink bg-ink px-3 py-1.5"
            : "rounded-full border border-line bg-white px-3 py-1.5"
        }
      >
        <Text className={on ? "text-xs font-medium text-white" : "text-xs text-ink"}>
          Only my issues
        </Text>
      </Pressable>
      {on && hiddenCount > 0 ? (
        // The hidden count is always visible while the filter hides any
        // race: filtered-out elections still elect real officials, so the
        // filtered ballot must never look like the full one. At 0 hidden
        // there is nothing concealed and the pressed chip alone carries the
        // state.
        <Text className="text-xs text-ink-soft">
          {hiddenCount} election{hiddenCount === 1 ? "" : "s"} hidden ·{" "}
          <Text accessibilityRole="button" className="font-medium underline" onPress={() => onChange(false)}>
            Show all
          </Text>
        </Text>
      ) : null}
    </View>
  );
}
