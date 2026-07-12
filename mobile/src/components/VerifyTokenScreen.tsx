import { apiRequest } from "@voteapp/api-client";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Stack, useLocalSearchParams, useRouter } from "expo-router";
import { useEffect, useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { clearSessionId } from "../lib/sessionStore";
import { LabeledInput } from "./LabeledInput";
import { ErrorNotice, LoadingNotice } from "./Status";

// Shared body for the two email-link token flows, port of the web
// VerifyTokenPage. The token normally arrives via the email link (deep
// link when the app is installed); the pasted-token input stays as a
// fallback for mail clients that mangle links — same rule as
// reset-password.
//
// Tokens are single-use server-side, so the POST is modeled as a cached
// query keyed by token (web parity): the QueryClient deduplicates it, and
// remounts can never re-POST a token that already succeeded or failed —
// refetchOn* are all off because staleTime only shields the success case.

type VerifyTokenScreenProps = {
  endpoint: "/api/auth/verify-email" | "/api/auth/verify-email-change";
  title: string;
  successMessage: string;
  /**
   * Registration verification revokes every pre-verification session (epoch
   * bump server-side) — any stored Bearer id is dead the moment the token
   * is accepted, so success clears the local session and offers Log in.
   * Email-change verification keeps the session; success just refetches
   * identity. Mirrors why the web's verify page ends on a Log in link.
   */
  revokesSessions: boolean;
};

export function VerifyTokenScreen({ endpoint, title, successMessage, revokesSessions }: VerifyTokenScreenProps) {
  const router = useRouter();
  const params = useLocalSearchParams<{ token?: string }>();
  const paramToken = typeof params.token === "string" ? params.token.trim() : "";
  const [input, setInput] = useState("");
  // The token being confirmed. Param-provided tokens fire immediately;
  // pasted ones on the Confirm tap.
  const [submittedToken, setSubmittedToken] = useState(paramToken);
  const [lastParamToken, setLastParamToken] = useState(paramToken);

  // A deep link can update the param while this screen stays mounted;
  // sync it into state then — but never clobber a pasted code
  // with an absent param. Render-time adjustment, per React's
  // "adjusting state when a prop changes" pattern.
  if (paramToken && paramToken !== lastParamToken) {
    setLastParamToken(paramToken);
    setSubmittedToken(paramToken);
  }

  const queryClient = useQueryClient();
  const verify = useQuery({
    queryKey: ["verify-token", endpoint, submittedToken],
    queryFn: async () => {
      const result = await apiRequest<{ status: string }>(endpoint, {
        method: "POST",
        body: { token: submittedToken },
      });
      if (revokesSessions) {
        // Awaited here, not in an effect, so (a) the success UI and its
        // Log in button render only after the dead session is gone, and
        // (b) the cached success can never run this again — an effect
        // keyed on isSuccess re-fires on remount (deep link tapped twice)
        // and would wipe the session of whoever logged in since. No
        // account-cache purge: it would remove this query's own entry and
        // re-POST the consumed token on remount; useLogin purges anyway.
        await clearSessionId().catch(() => {});
        queryClient.setQueryData(["me"], null);
      }
      return result;
    },
    enabled: submittedToken.length > 0,
    retry: false,
    staleTime: Infinity,
    gcTime: Infinity,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });

  useEffect(() => {
    if (verify.isSuccess && !revokesSessions) {
      // Identity fields (email, email_verified) changed server-side.
      // Re-running on remount is harmless — it is just a refetch.
      void queryClient.invalidateQueries({ queryKey: ["me"] });
    }
  }, [verify.isSuccess, revokesSessions, queryClient]);

  return (
    <KeyboardAvoidingView className="flex-1 bg-white" behavior={Platform.OS === "ios" ? "padding" : undefined}>
      <Stack.Screen options={{ title }} />
      <ScrollView keyboardShouldPersistTaps="handled" contentContainerClassName="px-4 py-10">
        <Text className="text-2xl font-bold text-ink">{title}</Text>

        {submittedToken.length === 0 ? (
          <View className="mt-6 gap-4">
            <LabeledInput
              label="Confirmation code"
              hint="From the email we sent you."
              value={input}
              onChangeText={setInput}
              autoCapitalize="none"
              autoCorrect={false}
            />
            <Pressable
              disabled={input.trim().length === 0}
              onPress={() => setSubmittedToken(input.trim())}
              accessibilityRole="button"
              className={
                input.trim().length > 0
                  ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark"
                  : "w-full rounded-md bg-line px-4 py-3"
              }
            >
              <Text className="text-center font-semibold text-white">Confirm</Text>
            </Pressable>
          </View>
        ) : null}

        {verify.isFetching ? <LoadingNotice text="Confirming…" /> : null}

        {verify.isSuccess ? (
          <View className="mt-3">
            <Text className="text-ink-soft">{successMessage}</Text>
            {revokesSessions ? (
              <Pressable
                onPress={() => router.replace("/auth/login")}
                accessibilityRole="link"
                className="mt-6 self-start rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
              >
                <Text className="font-semibold text-white">Log in</Text>
              </Pressable>
            ) : (
              <Pressable
                onPress={() => router.dismissTo("/")}
                accessibilityRole="link"
                className="mt-6 self-start rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
              >
                <Text className="font-semibold text-white">Done</Text>
              </Pressable>
            )}
          </View>
        ) : null}

        {verify.isError ? (
          <View className="mt-4 gap-3">
            <ErrorNotice error={verify.error} />
            <Text className="text-sm text-ink-soft">
              The code may have expired or been replaced by a newer one. Check for the most recent email we
              sent you.
            </Text>
            <Pressable
              onPress={() => {
                // A fresh code needs a fresh entry — clear the consumed one.
                setSubmittedToken("");
                setInput("");
              }}
              accessibilityRole="button"
              className="self-start rounded-lg border border-line bg-white px-4 py-2 active:border-rausch"
            >
              <Text className="text-sm font-semibold text-ink">Enter a different code</Text>
            </Pressable>
          </View>
        ) : null}
      </ScrollView>
    </KeyboardAvoidingView>
  );
}
