import { configureApi } from "@voteapp/api-client";
import { getSessionId } from "./sessionStore";

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
  if (__DEV__ && !baseUrl) {
    // An empty base URL means host-less paths like "/api/me", which native
    // networking rejects — every hook then fails with no hint at the cause.
    console.warn(
      "EXPO_PUBLIC_API_URL is not set; API requests will fail on native. " +
        "Start with EXPO_PUBLIC_API_URL=http://127.0.0.1:3001 (or your API origin)."
    );
  }
  configureApi({
    baseUrl,
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
