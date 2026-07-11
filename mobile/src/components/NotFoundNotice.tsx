import { useRouter } from "expo-router";
import { Pressable, Text, View } from "react-native";

/** Not-found body for detail screens (the API returned 404). */
export function NotFoundNotice({ subject }: { subject: "Election" | "Candidate" }) {
  const router = useRouter();
  return (
    <View className="items-center px-4 py-16">
      <Text className="text-2xl font-bold text-ink">{subject} not found</Text>
      <Text className="mt-2 text-ink-soft">It may have been removed, or the link may be wrong.</Text>
      <Pressable
        className="mt-6 rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
        onPress={() => router.dismissTo("/")}
      >
        <Text className="font-semibold text-white">Find your ballot</Text>
      </Pressable>
    </View>
  );
}
