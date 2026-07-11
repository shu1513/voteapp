import { Text, TextInput, View, type TextInputProps } from "react-native";

/** Form field with the label above, shared by the auth screens. */
export function LabeledInput({
  label,
  hint,
  ...inputProps
}: TextInputProps & { label: string; hint?: string }) {
  return (
    <View>
      <Text className="text-sm font-medium text-ink">{label}</Text>
      <TextInput
        className="mt-1 w-full rounded-md border border-line bg-white px-3 py-3 text-ink"
        placeholderTextColor="#717171"
        accessibilityLabel={label}
        {...inputProps}
      />
      {hint ? <Text className="mt-1 text-xs text-ink-soft">{hint}</Text> : null}
    </View>
  );
}
