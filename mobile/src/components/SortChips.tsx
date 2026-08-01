import { Pressable, Text, View } from "react-native";

/**
 * Port of the web pages' sort <select>: a row of chips (2–3 options fit a
 * phone row; an action sheet would hide the choices behind a tap).
 *
 * `accessibilityLabel` names the row for screen readers (the web
 * counterpart's role="group" aria-label) — needed when two chip rows sit
 * next to each other and only their purpose tells them apart. Best-effort
 * on RN: the container stays non-accessible so the chips keep individual
 * focus; VoiceOver/TalkBack announce the group where the platform supports
 * container labels.
 */
export function SortChips<Value extends string>({
  options,
  value,
  onChange,
  accessibilityLabel,
}: {
  options: readonly { value: Value; label: string }[];
  value: Value;
  onChange: (value: Value) => void;
  accessibilityLabel?: string;
}) {
  return (
    <View
      className="flex-row flex-wrap gap-2"
      accessibilityRole={accessibilityLabel ? "radiogroup" : undefined}
      accessibilityLabel={accessibilityLabel}
    >
      {options.map((option) => {
        const selected = option.value === value;
        return (
          <Pressable
            key={option.value}
            onPress={() => onChange(option.value)}
            accessibilityRole="button"
            accessibilityState={{ selected }}
            className={
              selected
                ? "rounded-full border border-ink bg-ink px-3 py-1.5"
                : "rounded-full border border-line bg-white px-3 py-1.5"
            }
          >
            <Text className={selected ? "text-xs font-medium text-white" : "text-xs text-ink"}>
              {option.label}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}
