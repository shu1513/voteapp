import { useRouter } from "expo-router";
import { useState } from "react";
import { Modal, Pressable, ScrollView, Text, View } from "react-native";

// Clickwrap checkbox (Meyer v. Uber / Nguyen / Berman requirements):
// unchecked by default, sits directly above the action it gates, visible
// links adjacent to the checkbox, and the caller disables its action button
// until `checked` is true. Controlled, same as the web component.
//
// Pass `fullAgreement` where the label is a summary: it adds the same
// full-wording sheet the web gate opens, so a shortened label is never the
// only thing the visitor was shown. Agreeing from inside the sheet ticks the
// box; dismissing it changes nothing.

type FullAgreementContent = {
  paragraphs: readonly string[];
  privacyNotice: string;
};

const DOCUMENT_LINKS = [
  { label: "Terms of Use", path: "/legal/terms" },
  { label: "Privacy Policy", path: "/legal/privacy" },
  { label: "Disclaimer", path: "/legal/disclaimer" },
] as const;

type LegalGateProps = {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
  fullAgreement?: FullAgreementContent;
};

export function LegalGate({ label, checked, onChange, fullAgreement }: LegalGateProps) {
  const router = useRouter();
  const [sheetOpen, setSheetOpen] = useState(false);
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
        {DOCUMENT_LINKS.map((document) => (
          <Text
            key={document.path}
            className="text-sm font-medium text-ink underline"
            accessibilityRole="link"
            onPress={() => router.push(document.path)}
          >
            {document.label}
          </Text>
        ))}
        {fullAgreement ? (
          <Text
            className="text-sm font-medium text-ink underline"
            accessibilityRole="button"
            onPress={() => setSheetOpen(true)}
          >
            Read the full agreement
          </Text>
        ) : null}
      </View>
      {fullAgreement ? (
        <Modal
          visible={sheetOpen}
          animationType="slide"
          transparent
          // Android back button must dismiss without agreeing.
          onRequestClose={() => setSheetOpen(false)}
        >
          <View className="flex-1 justify-end bg-ink/40">
            <View className="max-h-[85%] rounded-t-2xl bg-white">
              <View className="border-b border-line px-5 py-4">
                <Text accessibilityRole="header" className="text-lg font-bold text-ink">
                  What you are agreeing to
                </Text>
              </View>
              <ScrollView className="px-5 py-4">
                <Text className="text-sm font-medium text-ink">{label}</Text>
                {fullAgreement.paragraphs.map((paragraph) => (
                  <Text key={paragraph} className="mt-3 text-sm text-ink-soft">
                    {paragraph}
                  </Text>
                ))}
                <Text className="mt-3 text-sm text-ink-soft">{fullAgreement.privacyNotice}</Text>
                {/* The links behind the gate are covered while this sheet is
                    open, so it carries its own set. Dismiss first, then
                    navigate — a screen pushed under a visible Modal is
                    unreachable. */}
                <View className="mt-4 flex-row flex-wrap gap-x-4">
                  {DOCUMENT_LINKS.map((document) => (
                    <Text
                      key={document.path}
                      className="text-sm font-medium text-ink underline"
                      accessibilityRole="link"
                      onPress={() => {
                        setSheetOpen(false);
                        router.push(document.path);
                      }}
                    >
                      {document.label}
                    </Text>
                  ))}
                </View>
              </ScrollView>
              <View className="gap-2 border-t border-line px-5 py-4">
                <Pressable
                  accessibilityRole="button"
                  className="w-full rounded-lg bg-rausch px-4 py-3 active:bg-rausch-dark"
                  onPress={() => {
                    onChange(true);
                    setSheetOpen(false);
                  }}
                >
                  <Text className="text-center font-semibold text-white">I agree</Text>
                </Pressable>
                <Pressable
                  accessibilityRole="button"
                  className="w-full rounded-lg border border-line px-4 py-3"
                  onPress={() => setSheetOpen(false)}
                >
                  <Text className="text-center font-semibold text-ink">Close</Text>
                </Pressable>
              </View>
            </View>
          </View>
        </Modal>
      ) : null}
    </View>
  );
}
