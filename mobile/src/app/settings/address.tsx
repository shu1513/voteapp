import { Stack } from "expo-router";
import { ScrollView, Text } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { SavedAddressForm } from "../../components/SavedAddressForm";

// Settings "Your address" screen. Verified-only — PUT /api/me/address is
// verified-email-gated, and AccountGate's default ladder enforces the same
// rule if someone lands here unverified.

function AddressBody() {
  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8" keyboardShouldPersistTaps="handled">
      <Text className="text-sm text-ink-soft">
        Saving a new address replaces the districts on your saved ballot.
      </Text>
      <SavedAddressForm label="New address" />
    </ScrollView>
  );
}

export default function AddressScreen() {
  return (
    <>
      <Stack.Screen options={{ title: "Your address" }} />
      <AccountGate signedOutText="Log in to manage your account.">{() => <AddressBody />}</AccountGate>
    </>
  );
}
