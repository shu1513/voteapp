import { Pressable, Text, View } from "react-native";

/** Small labeled checkbox used by preference toggles. */
export function Checkbox({
  label,
  checked,
  disabled = false,
  onChange,
}: {
  label: string;
  checked: boolean;
  disabled?: boolean;
  onChange: (next: boolean) => void;
}) {
  return (
    <Pressable
      className="flex-row items-center gap-2"
      onPress={() => onChange(!checked)}
      disabled={disabled}
      accessibilityRole="checkbox"
      accessibilityState={{ checked, disabled }}
      accessibilityLabel={label}
    >
      <View
        className={
          checked
            ? "h-4 w-4 items-center justify-center rounded border border-rausch bg-rausch"
            : "h-4 w-4 rounded border border-ink-soft bg-white"
        }
      >
        {checked ? <Text className="text-[10px] font-bold text-white">✓</Text> : null}
      </View>
      <Text className={disabled ? "text-xs text-ink-soft opacity-60" : "text-xs text-ink-soft"}>{label}</Text>
    </Pressable>
  );
}
