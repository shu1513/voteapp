import { Stack, useRouter } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { LabeledInput } from "../../components/LabeledInput";
import { ErrorNotice } from "../../components/Status";
import { useChangePassword, useDeleteAccount, useLogoutAll } from "../../lib/auth";

// Port of the web settings Change password / Sign out / Delete account
// sections. All three work unverified — the backend gates none of them.

function PasswordSection() {
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const change = useChangePassword();
  const canChange = currentPassword.length > 0 && newPassword.length > 0 && !change.isPending;

  return (
    <View className="rounded-xl border border-line bg-white p-4">
      <Text className="text-lg font-semibold text-ink">Change password</Text>
      <Text className="mt-1 text-sm text-ink-soft">
        Changing your password signs you out everywhere else; this device stays logged in.
      </Text>
      <View className="mt-3 gap-3">
        <LabeledInput
          label="Current password"
          value={currentPassword}
          onChangeText={setCurrentPassword}
          autoComplete="current-password"
          secureTextEntry
        />
        <LabeledInput
          label="New password"
          hint="At least 12 characters."
          value={newPassword}
          onChangeText={setNewPassword}
          autoComplete="new-password"
          secureTextEntry
        />
        <Pressable
          disabled={!canChange}
          onPress={() => {
            change.mutate(
              { currentPassword, newPassword },
              {
                onSuccess: () => {
                  setCurrentPassword("");
                  setNewPassword("");
                },
              }
            );
          }}
          accessibilityRole="button"
          className={
            canChange ? "rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark" : "rounded-md bg-line px-4 py-3"
          }
        >
          <Text className="text-center font-semibold text-white">
            {change.isPending ? "Changing…" : change.isSuccess ? "Password changed" : "Change password"}
          </Text>
        </Pressable>
      </View>
      {change.isError ? (
        <View className="mt-2">
          <ErrorNotice error={change.error} />
        </View>
      ) : null}
    </View>
  );
}

function SignOutSection() {
  const router = useRouter();
  // Signs out everywhere (all devices), matching the web settings page.
  const logoutAll = useLogoutAll();

  return (
    <View>
      <Pressable
        disabled={logoutAll.isPending}
        onPress={() => {
          logoutAll.mutate(undefined, {
            onSuccess: () => {
              router.replace("/");
            },
          });
        }}
        accessibilityRole="button"
        className="self-start rounded-lg border border-line bg-white px-4 py-2 active:border-rausch"
      >
        <Text className="text-sm font-semibold text-ink">
          {logoutAll.isPending ? "Signing out…" : "Sign out"}
        </Text>
      </Pressable>
      {logoutAll.isError ? (
        <View className="mt-2">
          <ErrorNotice error={logoutAll.error} />
        </View>
      ) : null}
    </View>
  );
}

function DangerSection() {
  const router = useRouter();
  const [password, setPassword] = useState("");
  const [confirming, setConfirming] = useState(false);
  const deleteAccount = useDeleteAccount();
  const canDelete = password.length > 0 && !deleteAccount.isPending;

  return (
    <View className="rounded-xl border border-rausch/40 bg-rausch/5 p-4">
      <Text className="text-lg font-semibold text-rausch-dark">Delete account</Text>
      <Text className="mt-1 text-sm text-ink-soft">
        Permanently deletes your account, saved districts, follows, and preferences. This cannot be undone.
      </Text>
      {!confirming ? (
        <Pressable
          onPress={() => setConfirming(true)}
          accessibilityRole="button"
          className="mt-3 self-start rounded-lg border border-rausch/60 bg-white px-4 py-2 active:bg-rausch/10"
        >
          <Text className="text-sm font-semibold text-rausch-dark">Delete my account…</Text>
        </Pressable>
      ) : (
        <View className="mt-3 gap-3">
          <LabeledInput
            label="Confirm with your password"
            value={password}
            onChangeText={setPassword}
            autoComplete="current-password"
            secureTextEntry
          />
          <View className="flex-row gap-3">
            <Pressable
              disabled={!canDelete}
              onPress={() => {
                deleteAccount.mutate(
                  { password },
                  {
                    onSuccess: () => {
                      router.replace("/");
                    },
                  }
                );
              }}
              accessibilityRole="button"
              className={
                canDelete ? "rounded-lg bg-rausch-dark px-4 py-2 active:bg-rausch" : "rounded-lg bg-line px-4 py-2"
              }
            >
              <Text className="text-sm font-semibold text-white">
                {deleteAccount.isPending ? "Deleting…" : "Permanently delete"}
              </Text>
            </Pressable>
            <Pressable
              onPress={() => {
                setConfirming(false);
                setPassword("");
              }}
              accessibilityRole="button"
              className="rounded-lg border border-line bg-white px-4 py-2"
            >
              <Text className="text-sm font-semibold text-ink">Cancel</Text>
            </Pressable>
          </View>
        </View>
      )}
      {deleteAccount.isError ? (
        <View className="mt-2">
          <ErrorNotice error={deleteAccount.error} />
        </View>
      ) : null}
    </View>
  );
}

export default function SecurityScreen() {
  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title: "Security" }} />
      <AccountGate signedOutText="Log in to manage your account." allowUnverified>
        {() => (
          <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="gap-4 px-4 py-8">
            <PasswordSection />
            <SignOutSection />
            <DangerSection />
          </ScrollView>
        )}
      </AccountGate>
    </KeyboardAvoidingView>
  );
}
