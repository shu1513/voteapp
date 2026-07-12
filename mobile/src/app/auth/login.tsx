import { Stack, useRouter } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { LabeledInput } from "../../components/LabeledInput";
import { ErrorNotice } from "../../components/Status";
import { useLogin } from "../../lib/auth";

/** Port of the web LoginPage; session storage handled by useLogin. */
export default function LoginScreen() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const login = useLogin();

  const canSubmit = email.trim().length > 0 && password.length > 0 && !login.isPending;

  function onSubmit() {
    if (!canSubmit) {
      return;
    }
    login.mutate(
      { email: email.trim(), password },
      {
        onSuccess: () => {
          // The saved-ballot screen lands with the account chunk; until then
          // return to where the user came from (home shows signed-in state).
          router.dismissTo("/");
        },
      }
    );
  }

  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title: "Log in" }} />
      <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="px-4 py-10">
        <Text className="text-2xl font-bold text-ink">Log in</Text>

        <View className="mt-6 gap-4">
          <LabeledInput
            label="Email"
            value={email}
            onChangeText={setEmail}
            autoComplete="email"
            keyboardType="email-address"
            autoCapitalize="none"
          />
          <LabeledInput
            label="Password"
            value={password}
            onChangeText={setPassword}
            autoComplete="current-password"
            secureTextEntry
            onSubmitEditing={onSubmit}
          />
          <Pressable
            disabled={!canSubmit}
            onPress={onSubmit}
            accessibilityRole="button"
            className={
              canSubmit
                ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark"
                : "w-full rounded-md bg-line px-4 py-3"
            }
          >
            <Text className="text-center font-semibold text-white">
              {login.isPending ? "Logging in…" : "Log in"}
            </Text>
          </Pressable>
        </View>

        {login.isError ? (
          <View className="mt-4">
            <ErrorNotice error={login.error} />
          </View>
        ) : null}

        <View className="mt-6 gap-1">
          <Text
            className="text-sm text-ink-soft underline"
            accessibilityRole="link"
            onPress={() => router.push("/auth/forgot-password")}
          >
            Forgot your password?
          </Text>
          <Text className="text-sm text-ink-soft">
            New here?{" "}
            <Text
              className="underline"
              accessibilityRole="link"
              onPress={() => router.push("/auth/register")}
            >
              Create an account
            </Text>
          </Text>
        </View>
      </ScrollView>
    </KeyboardAvoidingView>
  );
}
