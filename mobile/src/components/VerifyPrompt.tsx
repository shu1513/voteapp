import { apiRequest } from "@voteapp/api-client";
import { useMutation } from "@tanstack/react-query";
import { useRouter } from "expo-router";
import { Pressable, Text, View } from "react-native";
import { ErrorNotice } from "./Status";

// Interstitial for the unverified state: personalized features 403 until the
// email is verified, and GET /api/me (which never 403s) tells us the address
// to offer a resend for. Port of the web component.

export function VerifyPrompt({ email }: { email: string }) {
  const router = useRouter();
  const resend = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/auth/resend-verification", {
        method: "POST",
        body: { email },
      }),
  });

  const disabled = resend.isPending || resend.isSuccess;

  return (
    <View className="px-4 py-10">
      <Text className="text-2xl font-bold text-ink">Verify your email</Text>
      <Text className="mt-3 text-ink-soft">
        We sent a verification link to <Text className="font-bold text-ink">{email}</Text>. Personalized
        features unlock once it&apos;s confirmed.
      </Text>
      <Pressable
        onPress={() => resend.mutate()}
        disabled={disabled}
        accessibilityRole="button"
        className={
          disabled
            ? "mt-6 self-start rounded-lg bg-line px-4 py-2"
            : "mt-6 self-start rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
        }
      >
        <Text className="font-semibold text-white">
          {resend.isSuccess ? "Email sent" : resend.isPending ? "Sending…" : "Resend verification email"}
        </Text>
      </Pressable>
      {resend.isError ? (
        <View className="mt-4">
          <ErrorNotice error={resend.error} />
        </View>
      ) : null}
      <Text
        className="mt-4 text-sm text-ink-soft underline"
        accessibilityRole="link"
        onPress={() => router.push("/verify-email")}
      >
        Have a confirmation code? Enter it here.
      </Text>
    </View>
  );
}
