import { apiRequest } from "@voteapp/api-client";
import { useMutation } from "@tanstack/react-query";
import { Stack, useLocalSearchParams, useRouter } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { LabeledInput } from "../../components/LabeledInput";
import { ErrorNotice } from "../../components/Status";

/**
 * Port of the web ResetPasswordPage. The token normally arrives via the
 * email link (a deep link once Phase 4 lands); until then the email opens
 * the web page, so this screen also accepts a pasted token — unlike the
 * web, which can rely on ?token= always being present.
 */
export default function ResetPasswordScreen() {
  const router = useRouter();
  const params = useLocalSearchParams<{ token?: string }>();
  const [token, setToken] = useState(typeof params.token === "string" ? params.token.trim() : "");
  const [password, setPassword] = useState("");

  const reset = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/reset-password", {
        method: "POST",
        body: { token: token.trim(), password },
      }),
  });

  const canSubmit = token.trim().length > 0 && password.length > 0 && !reset.isPending;

  if (reset.isSuccess) {
    return (
      <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-10">
        <Stack.Screen options={{ title: "Password updated" }} />
        <Text className="text-2xl font-bold text-ink">Password updated</Text>
        <Text className="mt-3 text-ink-soft">
          Your password has been changed and you have been logged out everywhere. Log in with the new
          password.
        </Text>
        <Pressable
          onPress={() => router.replace("/auth/login")}
          accessibilityRole="link"
          className="mt-6 self-start rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
        >
          <Text className="font-semibold text-white">Log in</Text>
        </Pressable>
      </ScrollView>
    );
  }

  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title: "Choose a new password" }} />
      <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="px-4 py-10">
        <Text className="text-2xl font-bold text-ink">Choose a new password</Text>

        <View className="mt-6 gap-4">
          {typeof params.token === "string" && params.token.trim().length > 0 ? null : (
            <LabeledInput
              label="Reset code"
              hint="From your password-reset email."
              value={token}
              onChangeText={setToken}
              autoCapitalize="none"
              autoCorrect={false}
            />
          )}
          <LabeledInput
            label="New password"
            hint="At least 12 characters."
            value={password}
            onChangeText={setPassword}
            autoComplete="new-password"
            secureTextEntry
          />
          <Pressable
            disabled={!canSubmit}
            onPress={() => reset.mutate()}
            accessibilityRole="button"
            className={
              canSubmit
                ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark"
                : "w-full rounded-md bg-line px-4 py-3"
            }
          >
            <Text className="text-center font-semibold text-white">
              {reset.isPending ? "Saving…" : "Set new password"}
            </Text>
          </Pressable>
        </View>

        {reset.isError ? (
          <View className="mt-4">
            <ErrorNotice error={reset.error} />
          </View>
        ) : null}
      </ScrollView>
    </KeyboardAvoidingView>
  );
}
