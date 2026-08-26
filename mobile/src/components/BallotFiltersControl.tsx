import type { VoteImpactThreshold } from "@voteapp/api-client";
import { type ReactNode, useState } from "react";
import { Pressable, Text, View } from "react-native";
import { Checkbox } from "./Checkbox";

const CHIP_ON = "rounded-full border border-ink bg-ink px-3 py-1.5";
const CHIP_OFF = "rounded-full border border-line bg-white px-3 py-1.5";
const CHIP_TEXT_ON = "text-xs font-medium text-white";
const CHIP_TEXT_OFF = "text-xs text-ink";

/**
 * The unified "Filters" disclosure for the ballot screens — port of the web
 * BallotFiltersControl. One chip opens an inline expandable section (no
 * portal/popover machinery in the native tree) holding the session-scoped
 * filter checkboxes (per-filter visibility comes from api-client's
 * deriveBallotFilters) and, on the saved tab, the persisted ordering
 * preference passed as orderSection; the two halves persist differently,
 * so they stay under separate Show / Order headings. The screens own the
 * filter state (plain state — screens stay mounted under a stack push, so
 * choices survive navigating into an election and back, matching the web's
 * URL params). Renders nothing when it has nothing to offer.
 *
 * The impact checkboxes are nested thresholds ("High or above" ⊂ "Normal
 * or above"), so exactly one can be engaged: checking one swaps the other
 * off, and unchecking means any impact. Labels reuse the card vocabulary
 * from formatVotePowerLabel — "Normal", never "medium".
 */
export function BallotFiltersControl({
  showIssues,
  issuesOn,
  onIssuesChange,
  showImpactHigh,
  showImpactMedium,
  impactLevel,
  onImpactChange,
  activeFilterCount,
  hiddenCount,
  onShowAll,
  orderSection,
}: {
  showIssues: boolean;
  issuesOn: boolean;
  onIssuesChange: (on: boolean) => void;
  showImpactHigh: boolean;
  showImpactMedium: boolean;
  impactLevel: VoteImpactThreshold | null;
  onImpactChange: (level: VoteImpactThreshold | null) => void;
  activeFilterCount: number;
  hiddenCount: number;
  onShowAll: () => void;
  orderSection?: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const showImpactGroup = showImpactHigh || showImpactMedium;
  const showSection = showIssues || showImpactGroup;
  if (!showSection && !orderSection) {
    return null;
  }

  return (
    <View className="mt-2 gap-2">
      <View className="flex-row flex-wrap items-center gap-2">
        <Pressable
          onPress={() => setOpen(!open)}
          accessibilityRole="button"
          accessibilityState={{ expanded: open }}
          className={open ? CHIP_ON : CHIP_OFF}
        >
          {/* The badge counts active FILTERS only, never ordering. */}
          <Text className={open ? CHIP_TEXT_ON : CHIP_TEXT_OFF}>
            Filters{activeFilterCount > 0 ? ` · ${activeFilterCount}` : ""}
          </Text>
        </Pressable>
        {hiddenCount > 0 ? (
          // Always visible while any filter hides a race — outside the
          // panel, so closing the disclosure never conceals that the list
          // is filtered. Filtered-out elections still elect real officials.
          <Text className="text-xs text-ink-soft">
            {hiddenCount} election{hiddenCount === 1 ? "" : "s"} hidden ·{" "}
            <Text accessibilityRole="button" className="font-medium underline" onPress={onShowAll}>
              Show all
            </Text>
          </Text>
        ) : null}
      </View>
      {open ? (
        <View className="gap-3 rounded-lg border border-line bg-white p-3">
          {showSection ? (
            <View className="gap-2">
              <Text className="text-xs font-medium uppercase tracking-wide text-ink-soft">Show</Text>
              {showIssues ? (
                <Checkbox label="Affects my issues" checked={issuesOn} onChange={onIssuesChange} />
              ) : null}
              {showImpactGroup ? (
                <View className="gap-2">
                  <Text className="text-xs text-ink-soft">My vote power</Text>
                  {showImpactHigh ? (
                    <Checkbox
                      label="High or above"
                      checked={impactLevel === "high"}
                      onChange={(checked) => onImpactChange(checked ? "high" : null)}
                    />
                  ) : null}
                  {showImpactMedium ? (
                    <Checkbox
                      label="Normal or above"
                      checked={impactLevel === "medium"}
                      onChange={(checked) => onImpactChange(checked ? "medium" : null)}
                    />
                  ) : null}
                </View>
              ) : null}
            </View>
          ) : null}
          {orderSection ? (
            <View className="gap-2">
              <Text className="text-xs font-medium uppercase tracking-wide text-ink-soft">Order</Text>
              {orderSection}
            </View>
          ) : null}
        </View>
      ) : null}
    </View>
  );
}
