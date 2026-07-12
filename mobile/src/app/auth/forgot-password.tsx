import { apiRequest } from "@voteapp/api-client";
import { useMutation } from "@tanstack/react-query";
import { Stack, useRouter } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { LabeledInput } from "../../components/LabeledInput";
import { ErrorNotice } from "../../components/Status";

/** Port of the web ForgotPasswordPage. */
export default function ForgotPasswordScreen() {
  const router = useRouter();
  const [email, setEmail] = useState("");

  const forgot = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/forgot-password", {
        method: "POST",
        body: { email: email.trim() },
      }),
  });

  if (forgot.isSuccess) {
    return (
      <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-10">
        <Stack.Screen options={{ title: "Check your email" }} />
        <Text className="text-2xl font-bold text-ink">Check your email</Text>
        <Text className="mt-3 text-ink-soft">
          If an account exists for <Text className="font-bold text-ink">{email.trim()}</Text>, we sent a
          password reset link. It expires in 24 hours.
        </Text>
        <Text
          className="mt-6 text-sm text-ink underline"
          accessibilityRole="link"
          onPress={() => router.replace("/auth/login")}
        >
          Back to login
        </Text>
      </ScrollView>
    );
  }

  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title: "Reset your password" }} />
      <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="px-4 py-10">
        <Text className="text-2xl font-bold text-ink">Reset your password</Text>
        <Text className="mt-2 text-sm text-ink-soft">
          Enter your account email and we&apos;ll send a reset link.
        </Text>

        <View className="mt-6 gap-4">
          <LabeledInput
            label="Email"
            value={email}
            onChangeText={setEmail}
            autoComplete="email"
            keyboardType="email-address"
            autoCapitalize="none"
          />
          <Pressable
            disabled={!email.trim() || forgot.isPending}
            onPress={() => forgot.mutate()}
            accessibilityRole="button"
            className={
              email.trim() && !forgot.isPending
                ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark"
                : "w-full rounded-md bg-line px-4 py-3"
            }
          >
            <Text className="text-center font-semibold text-white">
              {forgot.isPending ? "Sending…" : "Send reset link"}
            </Text>
          </Pressable>
        </View>

        {forgot.isError ? (
          <View className="mt-4">
            <ErrorNotice error={forgot.error} />
          </View>
        ) : null}
      </ScrollView>
    </KeyboardAvoidingView>
  );
}
