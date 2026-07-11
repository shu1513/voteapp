import { Pressable, Text, View } from "react-native";

/**
 * Port of the web pages' sort <select>: a row of chips (2–3 options fit a
 * phone row; an action sheet would hide the choices behind a tap).
 */
export function SortChips<Value extends string>({
  options,
  value,
  onChange,
}: {
  options: readonly { value: Value; label: string }[];
  value: Value;
  onChange: (value: Value) => void;
}) {
  return (
    <View className="flex-row flex-wrap gap-2">
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
