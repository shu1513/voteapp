import { configureApi } from "@voteapp/api-client";
import { getSessionId } from "./sessionStore";

// Render's free tier cold-starts in ~1 minute; the shared client's 15s web
// default would time out every first request after idle. Override per env
// when the API infrastructure changes.
const DEFAULT_TIMEOUT_MS = 75_000;

/**
 * Points the shared api-client at the VoteApp backend and attaches the
 * Bearer session transport. Called once from the root layout.
 *
 * EXPO_PUBLIC_API_URL examples:
 *   dev simulator: http://127.0.0.1:3001
 *   production:    https://impactperdollar.com
 */
export function initApi(): void {
  const baseUrl = process.env.EXPO_PUBLIC_API_URL ?? "";
  if (!baseUrl) {
    // An empty base URL means host-less paths like "/api/me", which native
    // networking rejects — every hook then fails with no hint at the cause.
    const message =
      "EXPO_PUBLIC_API_URL is not set; API requests will fail on native. " +
      "Set it in .env (see .env.example) or the EAS build profile.";
    if (!__DEV__) {
      // A release build without an API origin is unusable; fail at boot
      // with the cause instead of shipping an app where every screen errors.
      throw new Error(message);
    }
    console.warn(message);
  }

  const timeoutRaw = Number.parseInt(process.env.EXPO_PUBLIC_API_TIMEOUT_MS ?? "", 10);
  const timeoutMs = Number.isFinite(timeoutRaw) && timeoutRaw > 0 ? timeoutRaw : DEFAULT_TIMEOUT_MS;

  configureApi({
    baseUrl,
    // Marks requests as coming from the native app: login/password-change
    // then return the session id in the body and skip the Set-Cookie, so the
    // platform cookie jar never competes with the SecureStore transport.
    defaultHeaders: { "x-voteapp-client": "mobile" },
    timeoutMs,
    getAuthHeader: async () => {
      // A keystore failure must degrade to "signed out", not kill the
      // request (expo-secure-store throws on web, where Expo serves the
      // app for development).
      try {
        const sessionId = await getSessionId();
        return sessionId ? `Bearer ${sessionId}` : null;
      } catch {
        return null;
      }
    },
  });
}
