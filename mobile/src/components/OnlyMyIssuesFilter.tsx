import type { ElectionSummary } from "@voteapp/api-client";
import { Pressable, Text, View } from "react-native";

/**
 * "Only my issues" ballot filter, shared by the anonymous and saved ballot
 * screens — port of the web OnlyMyIssuesFilter. Keep = elections whose
 * research areas intersect the viewer's saved areas. Same visibility rule as
 * the election screen's records filter: while OFF it renders only when it
 * could change the current view (viewer has saved areas AND the list splits
 * into matched + unmatched); while ON it stays visible and keeps applying —
 * even when that empties the view ("N elections hidden · Show all" explains
 * the empty list) — because an active filter that silently stops applying
 * would show a full ballot the viewer believes is filtered. A viewer with no
 * saved areas gets the request ignored (the intersection is meaningless
 * without them).
 */
export function deriveOnlyMyIssues({
  elections,
  savedAreaIds,
  hasSaved,
  requested,
}: {
  elections: ElectionSummary[];
  savedAreaIds: Set<string>;
  hasSaved: boolean;
  requested: boolean;
}): {
  visibleElections: ElectionSummary[];
  filterOn: boolean;
  showFilter: boolean;
  hiddenCount: number;
} {
  const matched = elections.filter((election) =>
    election.research_areas.some((area) => savedAreaIds.has(area.id))
  );
  const filterOn = hasSaved && requested;
  const showFilter =
    filterOn || (hasSaved && matched.length > 0 && matched.length < elections.length);
  return {
    visibleElections: filterOn ? matched : elections,
    filterOn,
    showFilter,
    hiddenCount: elections.length - matched.length,
  };
}

/**
 * The toggle chip plus the hidden-count line. Render only when
 * deriveOnlyMyIssues says showFilter; the screens own the on/off state
 * (plain state — screens stay mounted under a stack push, so the choice
 * survives navigating into an election and back, matching the web's URL
 * param).
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
