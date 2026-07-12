import { Stack, useLocalSearchParams, useRouter } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import Markdown from "react-native-markdown-display";
// Inlined at build time (babel-plugin-inline-import): the repo's versioned
// legal texts in docs/legal/ are the single source of truth; the app renders
// them, never copies them — same rule as the web's Vite ?raw imports.
import disclaimerMarkdown from "../../../../docs/legal/disclaimer.md";
import privacyMarkdown from "../../../../docs/legal/privacy-policy.md";
import termsMarkdown from "../../../../docs/legal/terms-of-use.md";
import { openExternalUrl } from "../../lib/openExternalUrl";

// The docs carry maintainer HTML comments (draft banners, version notes).
// The web's react-markdown drops raw HTML; react-native-markdown-display
// would print it as text, so strip comments before rendering.
function stripHtmlComments(markdown: string): string {
  return markdown.replace(/<!--[\s\S]*?-->/g, "");
}

const DOCUMENTS = {
  disclaimer: { title: "Disclaimer", markdown: stripHtmlComments(disclaimerMarkdown) },
  terms: { title: "Terms of Use", markdown: stripHtmlComments(termsMarkdown) },
  privacy: { title: "Privacy Policy", markdown: stripHtmlComments(privacyMarkdown) },
} as const;

// The docs cross-link each other by web route path (terms links /privacy
// and /disclaimer). Those are SPA routes, not URLs — the in-app browser
// can't open them, so route them to the sibling native screen instead.
const INTERNAL_LINKS: Record<string, keyof typeof DOCUMENTS> = {
  "/terms": "terms",
  "/privacy": "privacy",
  "/disclaimer": "disclaimer",
};

// react-native-markdown-display styles per node type; tokens match the web
// prose styling (ink text, rausch-dark links).
const markdownStyles = {
  body: { color: "#222222", fontSize: 14, lineHeight: 21 },
  heading1: { fontSize: 24, fontWeight: "700" as const, marginBottom: 8 },
  heading2: { fontSize: 18, fontWeight: "600" as const, marginTop: 16, marginBottom: 4 },
  heading3: { fontSize: 15, fontWeight: "600" as const, marginTop: 12, marginBottom: 4 },
  link: { color: "#e31c5f", textDecorationLine: "underline" as const },
  bullet_list: { marginTop: 4 },
  list_item: { marginTop: 4 },
};

export default function LegalDocumentScreen() {
  const router = useRouter();
  const params = useLocalSearchParams<{ doc: string }>();
  const doc = typeof params.doc === "string" && params.doc in DOCUMENTS ? (params.doc as keyof typeof DOCUMENTS) : null;

  if (!doc) {
    return (
      <View className="items-center px-4 py-16">
        <Stack.Screen options={{ title: "Not found" }} />
        <Text className="text-2xl font-bold text-ink">Document not found</Text>
        <Pressable
          className="mt-6 rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
          onPress={() => router.dismissTo("/")}
        >
          <Text className="font-semibold text-white">Find your ballot</Text>
        </Pressable>
      </View>
    );
  }

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-6">
      <Stack.Screen options={{ title: DOCUMENTS[doc].title }} />
      <Markdown
        style={markdownStyles}
        onLinkPress={(url) => {
          const internal = INTERNAL_LINKS[url];
          if (internal) {
            router.push(`/legal/${internal}`);
          } else {
            openExternalUrl(url);
          }
          // Handled — stop the library's default Linking.openURL.
          return false;
        }}
      >
        {DOCUMENTS[doc].markdown}
      </Markdown>
    </ScrollView>
  );
}
