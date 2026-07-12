import { Pressable, Text, View } from "react-native";

/** Small labeled checkbox used by preference toggles. */
export function Checkbox({
  label,
  description,
  checked,
  disabled = false,
  onChange,
}: {
  label: string;
  description?: string;
  checked: boolean;
  disabled?: boolean;
  onChange: (next: boolean) => void;
}) {
  return (
    <Pressable
      className="flex-row items-start gap-2"
      onPress={() => onChange(!checked)}
      disabled={disabled}
      accessibilityRole="checkbox"
      accessibilityState={{ checked, disabled }}
      accessibilityLabel={label}
      accessibilityHint={description}
    >
      <View
        className={
          checked
            ? "mt-0.5 h-4 w-4 items-center justify-center rounded border border-rausch bg-rausch"
            : "mt-0.5 h-4 w-4 rounded border border-ink-soft bg-white"
        }
      >
        {checked ? <Text className="text-[10px] font-bold text-white">✓</Text> : null}
      </View>
      <View className="flex-1">
        <Text className={disabled ? "text-xs text-ink-soft opacity-60" : "text-xs text-ink-soft"}>{label}</Text>
        {description ? (
          <Text className={disabled ? "mt-0.5 text-[11px] text-ink-soft opacity-50" : "mt-0.5 text-[11px] text-ink-soft opacity-80"}>
            {description}
          </Text>
        ) : null}
      </View>
    </Pressable>
  );
}
