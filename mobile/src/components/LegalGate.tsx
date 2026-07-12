import { useRouter } from "expo-router";
import { Pressable, Text, View } from "react-native";

// Clickwrap checkbox (Meyer v. Uber / Nguyen / Berman requirements):
// unchecked by default, sits directly above the action it gates, visible
// links adjacent to the checkbox, and the caller disables its action button
// until `checked` is true. Controlled, same as the web component.

type LegalGateProps = {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
};

export function LegalGate({ label, checked, onChange }: LegalGateProps) {
  const router = useRouter();
  return (
    <View className="rounded-xl border border-line bg-surface p-4">
      <Pressable
        className="flex-row items-start gap-3"
        onPress={() => onChange(!checked)}
        accessibilityRole="checkbox"
        accessibilityState={{ checked }}
        accessibilityLabel={label}
      >
        <View
          className={
            checked
              ? "mt-1 h-5 w-5 items-center justify-center rounded border border-rausch bg-rausch"
              : "mt-1 h-5 w-5 rounded border border-ink-soft bg-white"
          }
        >
          {checked ? <Text className="text-xs font-bold text-white">✓</Text> : null}
        </View>
        <Text className="flex-1 text-sm text-ink">{label}</Text>
      </Pressable>
      <View className="mt-2 flex-row flex-wrap gap-x-4 pl-8">
        <Text className="text-sm font-medium text-ink underline" accessibilityRole="link" onPress={() => router.push("/legal/terms")}>
          Terms of Use
        </Text>
        <Text
          className="text-sm font-medium text-ink underline" accessibilityRole="link"
          onPress={() => router.push("/legal/privacy")}
        >
          Privacy Policy
        </Text>
        <Text
          className="text-sm font-medium text-ink underline" accessibilityRole="link"
          onPress={() => router.push("/legal/disclaimer")}
        >
          Disclaimer
        </Text>
      </View>
    </View>
  );
}
