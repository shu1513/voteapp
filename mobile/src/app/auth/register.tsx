import { apiRequest, SIGNUP_CHECKBOX_LABEL, TERMS_VERSION } from "@voteapp/api-client";
import { useMutation } from "@tanstack/react-query";
import { Stack, useRouter } from "expo-router";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { LabeledInput } from "../../components/LabeledInput";
import { LegalGate } from "../../components/LegalGate";
import { ErrorNotice } from "../../components/Status";

/** Port of the web RegisterPage. */
export default function RegisterScreen() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [firstName, setFirstName] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  // One toggle reveals both fields: the point of "Show" is checking what you
  // typed, and revealing only one of a pair defeats the comparison.
  const [showPassword, setShowPassword] = useState(false);
  const [accepted, setAccepted] = useState(false);

  const register = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/register", {
        method: "POST",
        body: {
          email: email.trim(),
          password,
          // Clickwrap record: the backend rejects registration without the
          // current terms version and stores it with a timestamp.
          accepted_terms_version: TERMS_VERSION,
          ...(firstName.trim() ? { first_name: firstName.trim() } : {}),
        },
      }),
  });

  const resend = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/resend-verification", {
        method: "POST",
        body: { email: email.trim() },
      }),
  });

  // The mismatch message waits until both fields have input — flagging a
  // half-typed confirmation as wrong would nag on every keystroke.
  const passwordsMismatch =
    password.length > 0 && confirmPassword.length > 0 && password !== confirmPassword;
  // Length gate lives here because mobile has no browser constraint layer —
  // without it a short password round-trips to the backend (min 12) just to
  // be rejected. The hint under the field explains the disabled button.
  const canSubmit =
    accepted &&
    email.trim().length > 0 &&
    password.length >= 12 &&
    password === confirmPassword &&
    !register.isPending;

  if (register.isSuccess) {
    return (
      <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-10">
        <Stack.Screen options={{ title: "Check your email" }} />
        <Text className="text-2xl font-bold text-ink">Check your email</Text>
        <Text className="mt-3 text-ink-soft">
          We sent a verification link to <Text className="font-bold text-ink">{email.trim()}</Text>. Open it
          to verify your account, then log in.
        </Text>
        <View className="mt-6 flex-row items-center gap-4">
          <Pressable
            onPress={() => router.replace("/auth/login")}
            accessibilityRole="link"
            className="rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
          >
            <Text className="font-semibold text-white">Go to login</Text>
          </Pressable>
          <Pressable onPress={() => resend.mutate()} disabled={resend.isPending} accessibilityRole="button">
            <Text className="text-sm text-ink-soft underline">
              {resend.isSuccess ? "Sent again" : "Resend email"}
            </Text>
          </Pressable>
        </View>
        {resend.isError ? (
          <View className="mt-4">
            <ErrorNotice error={resend.error} />
          </View>
        ) : null}
      </ScrollView>
    );
  }

  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title: "Create your account" }} />
      <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="px-4 py-10">
        <Text className="text-2xl font-bold text-ink">Create your account</Text>
        <Text className="mt-2 text-sm text-ink-soft">
          Save your districts, follow candidates, and get alerts when new elections appear where you live.
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
          <LabeledInput
            label="First Name (optional)"
            value={firstName}
            onChangeText={setFirstName}
            autoComplete="given-name"
          />
          <View className="gap-4">
            <LabeledInput
              label="Password"
              hint="At least 12 characters."
              value={password}
              onChangeText={setPassword}
              autoComplete="new-password"
              secureTextEntry={!showPassword}
            />
            <LabeledInput
              label="Confirm password"
              value={confirmPassword}
              onChangeText={setConfirmPassword}
              autoComplete="new-password"
              secureTextEntry={!showPassword}
            />
            {passwordsMismatch ? (
              <Text className="text-xs text-red-700">Passwords don&apos;t match.</Text>
            ) : null}
            <Pressable
              onPress={() => setShowPassword((value) => !value)}
              accessibilityRole="button"
              accessibilityState={{ expanded: showPassword }}
            >
              <Text className="text-xs text-ink-soft underline">
                {showPassword ? "Hide password" : "Show password"}
              </Text>
            </Pressable>
          </View>

          <LegalGate label={SIGNUP_CHECKBOX_LABEL} checked={accepted} onChange={setAccepted} />

          <Pressable
            disabled={!canSubmit}
            onPress={() => register.mutate()}
            accessibilityRole="button"
            className={
              canSubmit
                ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark"
                : "w-full rounded-md bg-line px-4 py-3"
            }
          >
            <Text className="text-center font-semibold text-white">
              {register.isPending ? "Creating account…" : "Create account"}
            </Text>
          </Pressable>
        </View>

        {register.isError ? (
          <View className="mt-4">
            <ErrorNotice error={register.error} />
          </View>
        ) : null}

        <Text className="mt-6 text-sm text-ink-soft">
          Already have an account?{" "}
          <Text className="underline" accessibilityRole="link" onPress={() => router.replace("/auth/login")}>
            Log in
          </Text>
        </Text>
      </ScrollView>
    </KeyboardAvoidingView>
  );
}
