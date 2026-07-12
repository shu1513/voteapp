import { Redirect, useLocalSearchParams } from "expo-router";

// The reset email links the web path /reset-password?token=… (backend
// buildEmailLink), but the native screen lives at /auth/reset-password.
// Alias the web path so universal links and voteapp:// URLs land on the
// real screen. The other email/link targets (/verify-email,
// /verify-email-change, /elections/*, /candidates/*, /ballot) already
// share their web paths — this is the only mismatch.
export default function ResetPasswordAlias() {
  const params = useLocalSearchParams<{ token?: string }>();
  const token = typeof params.token === "string" ? params.token.trim() : "";
  return (
    <Redirect
      href={token ? { pathname: "/auth/reset-password", params: { token } } : "/auth/reset-password"}
    />
  );
}
