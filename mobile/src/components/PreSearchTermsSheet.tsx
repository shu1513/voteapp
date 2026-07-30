import {
  PRE_SEARCH_AGREEMENT_PARAGRAPHS,
  PRE_SEARCH_CHECKBOX_LABEL,
  PRIVACY_NOTICE,
} from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { Modal, Pressable, ScrollView, Text, View } from "react-native";

// Mobile port of PreSearchTermsDialog. Same rules: the box starts empty every
// time the sheet opens, the action stays disabled until it is ticked, all
// three documents are named and linked, arbitration is visible here rather
// than only inside the Terms, and the button names what it does.
//
// A native Modal covers the screen, so the sheet carries its own document
// links — the ones behind it cannot be reached while it is open.

const DOCUMENT_LINKS = [
  { label: "Terms of Use", path: "/legal/terms" },
  { label: "Privacy Policy", path: "/legal/privacy" },
  { label: "Disclaimer", path: "/legal/disclaimer" },
] as const;

type PreSearchTermsSheetProps = {
  visible: boolean;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  onAgree: () => void;
  onCancel: () => void;
  pending: boolean;
};

export function PreSearchTermsSheet({
  visible,
  checked,
  onCheckedChange,
  onAgree,
  onCancel,
  pending,
}: PreSearchTermsSheetProps) {
  const router = useRouter();
  return (
    <Modal
      visible={visible}
      animationType="slide"
      transparent
      // Android back button dismisses without agreeing, unless a search is
      // already in flight.
      onRequestClose={pending ? () => undefined : onCancel}
    >
      <View className="flex-1 justify-end bg-ink/40">
        <View className="max-h-[88%] rounded-t-2xl bg-white">
          <View className="border-b border-line px-5 py-4">
            <Text accessibilityRole="header" className="text-lg font-bold text-ink">
              Before we search
            </Text>
          </View>

          <ScrollView className="px-5 py-4">
            {PRE_SEARCH_AGREEMENT_PARAGRAPHS.map((paragraph) => (
              <Text key={paragraph} className="mb-2 text-sm text-ink-soft">
                {paragraph}
              </Text>
            ))}
            <Text className="mb-4 text-sm text-ink-soft">{PRIVACY_NOTICE}</Text>

            <View className="rounded-xl border border-line bg-surface p-4">
              <Pressable
                className="flex-row items-start gap-3"
                onPress={() => onCheckedChange(!checked)}
                accessibilityRole="checkbox"
                accessibilityState={{ checked }}
                accessibilityLabel={PRE_SEARCH_CHECKBOX_LABEL}
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
                <Text className="flex-1 text-sm text-ink">{PRE_SEARCH_CHECKBOX_LABEL}</Text>
              </Pressable>
              <View className="mt-2 flex-row flex-wrap gap-x-4 pl-8">
                {DOCUMENT_LINKS.map((document) => (
                  <Text
                    key={document.path}
                    className="text-sm font-medium text-ink underline"
                    accessibilityRole="link"
                    onPress={() => {
                      // Dismiss first: a screen pushed under a visible Modal
                      // is unreachable. The typed address is untouched, so
                      // returning and pressing Search resumes the flow.
                      onCancel();
                      router.push(document.path);
                    }}
                  >
                    {document.label}
                  </Text>
                ))}
              </View>
            </View>
          </ScrollView>

          <View className="gap-2 border-t border-line px-5 py-4">
            <Pressable
              accessibilityRole="button"
              accessibilityState={{ disabled: !checked || pending }}
              disabled={!checked || pending}
              className={
                checked && !pending
                  ? "w-full rounded-lg bg-rausch px-4 py-3 active:bg-rausch-dark"
                  : "w-full rounded-lg bg-line px-4 py-3"
              }
              onPress={onAgree}
            >
              <Text className="text-center font-semibold text-white">
                {pending ? "Searching…" : "Agree and search"}
              </Text>
            </Pressable>
            <Pressable
              accessibilityRole="button"
              accessibilityState={{ disabled: pending }}
              disabled={pending}
              className="w-full rounded-lg border border-line px-4 py-3"
              onPress={onCancel}
            >
              <Text className="text-center font-semibold text-ink">Cancel</Text>
            </Pressable>
          </View>
        </View>
      </View>
    </Modal>
  );
}
