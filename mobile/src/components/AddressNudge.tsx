import { useRouter } from "expo-router";
import { Text, View } from "react-native";

// The pick gate's state-3 conversion nudge (docs/plans/pick-district-gate.md):
// rendered where the pick controls would sit when the viewer's districts are
// unknown. One sentence, no dismissal state — port of the web AddressNudge.
// Mobile difference: the audience here is always a logged-in account (guests
// get LogInToPlanLine instead of any gate state), so the link goes to the
// settings address form rather than the web's guest lookup.
export function AddressNudge() {
  const router = useRouter();
  return (
    <View className="rounded-md border border-nudge-line bg-nudge px-3 py-2">
      <Text className="text-sm text-ink">
        <Text
          className="font-medium text-nudge-deep underline"
          accessibilityRole="link"
          onPress={() => router.push("/settings/address")}
        >
          Enter your address
        </Text>{" "}
        to check if you can vote in this race.
      </Text>
    </View>
  );
}
