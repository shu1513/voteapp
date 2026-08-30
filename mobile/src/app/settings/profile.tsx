import type { Me } from "@voteapp/api-client";
import { apiRequest } from "@voteapp/api-client";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Stack } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { LabeledInput } from "../../components/LabeledInput";
import { ErrorNotice } from "../../components/Status";

// Port of the web settings Profile + Change email sections. Both work
// unverified — the backend gates neither.

function NameSection({ me }: { me: Me }) {
  const [firstName, setFirstName] = useState(me.first_name);
  const queryClient = useQueryClient();
  const update = useMutation({
    mutationFn: () =>
      apiRequest<{ user: Me }>("/api/me", { method: "PUT", body: { first_name: firstName.trim() } }),
    onSuccess: (response) => {
      queryClient.setQueryData(["me"], response.user);
    },
  });
  const canSave = firstName.trim().length > 0 && !update.isPending;

  return (
    <View className="rounded-xl border border-line bg-white p-4">
      <Text className="text-lg font-semibold text-ink">Profile</Text>
      <Text className="mt-1 text-sm text-ink-soft">Signed in as {me.email}</Text>
      <View className="mt-3 gap-3">
        <LabeledInput label="First Name" value={firstName} maxLength={80} onChangeText={setFirstName} />
        <Pressable
          disabled={!canSave}
          onPress={() => update.mutate()}
          accessibilityRole="button"
          className={
            canSave ? "rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark" : "rounded-md bg-line px-4 py-3"
          }
        >
          <Text className="text-center font-semibold text-white">
            {/* Trimmed: the PUT sends firstName.trim(), so that's what
                me.first_name echoes back. */}
            {update.isSuccess && firstName.trim() === me.first_name ? "Saved" : "Save"}
          </Text>
        </Pressable>
      </View>
      {update.isError ? (
        <View className="mt-2">
          <ErrorNotice error={update.error} />
        </View>
      ) : null}
    </View>
  );
}

function EmailSection({ me }: { me: Me }) {
  const [newEmail, setNewEmail] = useState("");
  const [password, setPassword] = useState("");
  const request = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/me/email", {
        method: "POST",
        body: { new_email: newEmail.trim(), password },
      }),
  });

  if (request.isSuccess) {
    return (
      <View className="rounded-xl border border-line bg-white p-4">
        <Text className="text-lg font-semibold text-ink">Change email</Text>
        <Text className="mt-2 text-sm text-ink-soft">
          If <Text className="font-semibold text-ink">{newEmail.trim()}</Text> is available, we sent it a
          confirmation link. Your address stays <Text className="font-semibold text-ink">{me.email}</Text>{" "}
          until you open it.
        </Text>
      </View>
    );
  }

  const canSend = newEmail.trim().length > 0 && password.length > 0 && !request.isPending;

  return (
    <View className="rounded-xl border border-line bg-white p-4">
      <Text className="text-lg font-semibold text-ink">Change email</Text>
      <View className="mt-3 gap-3">
        <LabeledInput
          label="New email"
          value={newEmail}
          onChangeText={setNewEmail}
          autoCapitalize="none"
          autoCorrect={false}
          keyboardType="email-address"
          autoComplete="email"
        />
        <LabeledInput
          label="Confirm with your password"
          value={password}
          onChangeText={setPassword}
          autoComplete="current-password"
          secureTextEntry
        />
        <Pressable
          disabled={!canSend}
          onPress={() => request.mutate()}
          accessibilityRole="button"
          className={
            canSend ? "rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark" : "rounded-md bg-line px-4 py-3"
          }
        >
          <Text className="text-center font-semibold text-white">
            {request.isPending ? "Sending…" : "Send confirmation"}
          </Text>
        </Pressable>
      </View>
      {request.isError ? (
        <View className="mt-2">
          <ErrorNotice error={request.error} />
        </View>
      ) : null}
    </View>
  );
}

export default function ProfileScreen() {
  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title: "Profile" }} />
      <AccountGate signedOutText="Log in to manage your account." allowUnverified>
        {(me) => (
          <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="gap-4 px-4 py-8">
            <NameSection me={me} />
            {/* Change email is password-confirmed, so a Google-only account
                (no password yet) can't use it — the security screen's
                AddPasswordSection names the way in. Same gate as the web
                settings page. */}
            {me.has_password ? <EmailSection me={me} /> : null}
          </ScrollView>
        )}
      </AccountGate>
    </KeyboardAvoidingView>
  );
}
