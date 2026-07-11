import { useState, type ReactNode } from "react";
import { Pressable, Text, View } from "react-native";

/** Port of the web pages' <details>/<summary> disclosure sections. */
export function Collapsible({ summary, children }: { summary: string; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <View className="mt-2">
      <Pressable
        onPress={() => setOpen((current) => !current)}
        accessibilityRole="button"
        accessibilityState={{ expanded: open }}
      >
        <Text className="text-xs text-ink-soft underline">{summary}</Text>
      </Pressable>
      {open ? children : null}
    </View>
  );
}
